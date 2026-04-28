// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package integration provides integration test helpers.
package integration

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/internal/parser"
	"github.com/bureau14/qdb-nats-connector/internal/sink"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

// TestComputeFieldSegfault reproduces the segmentation fault that occurs
// when using compute_field with concat operation and processing ~1000+ messages.
// The issue is a GC race condition where string concatenation creates temporary
// allocations that can be moved/collected before QuasarDB pins the memory.
func TestComputeFieldSegfault(t *testing.T) {
	// Skip test if not in CI or explicitly requested
	if os.Getenv("RUN_SEGFAULT_TEST") != "1" {
		t.Skip("Skipping segfault test - set RUN_SEGFAULT_TEST=1 to run")
	}
	qdbEndpoint := "qdb://127.0.0.1:2836"

	// Connect to QuasarDB
	handle, err := qdb.NewHandle()
	require.NoError(t, err, "Failed to create QuasarDB handle")
	defer func() { _ = handle.Close() }()

	err = handle.Connect(qdbEndpoint)
	require.NoError(t, err, "Failed to connect to QuasarDB at %s", qdbEndpoint)

	// Create test table matching network_metrics structure
	tableName := "test_compute_field_segfault_" + time.Now().Format("20060102150405")
	table := handle.Table(tableName)

	columns := []qdb.TsColumnInfo{
		qdb.NewTsColumnInfo("device_id", qdb.TsColumnString),
		qdb.NewTsColumnInfo("bytes_in", qdb.TsColumnInt64),
		qdb.NewTsColumnInfo("bytes_out", qdb.TsColumnInt64),
		qdb.NewTsColumnInfo("packets_dropped", qdb.TsColumnInt64),
		qdb.NewTsColumnInfo("latency_ms", qdb.TsColumnDouble),
		qdb.NewTsColumnInfo("device_path", qdb.TsColumnString),
	}

	err = table.Create(24*time.Hour, columns...)
	require.NoError(t, err, "Failed to create test table")
	defer func() { _ = table.Remove() }()

	// Create YAML parser with compute_field configuration similar to network-metrics
	yamlConfig := `
output:
  columns:
    - name: "device_id"
      type: "string"
    - name: "bytes_in"
      type: "int64"
    - name: "bytes_out"
      type: "int64"
    - name: "packets_dropped"
      type: "int64"
    - name: "latency_ms"
      type: "double"
    - name: "device_path"
      type: "string"

transformations:
  - step: "parse_json"
    config: {}
  
  - step: "extract_table"
    config:
      value: "` + tableName + `"
  
  - step: "extract_index"
    config:
      source: "timestamp"
      format: "rfc3339nano"
  
  - step: "extract_field"
    config:
      source: "device_id"
      target: "device_id"
      type: "string"
  
  - step: "extract_field"
    config:
      source: "bytes_in"
      target: "bytes_in"
      type: "int64"
  
  - step: "extract_field"
    config:
      source: "bytes_out"
      target: "bytes_out"
      type: "int64"
  
  - step: "extract_field"
    config:
      source: "packets_dropped"
      target: "packets_dropped"
      type: "int64"
  
  - step: "extract_field"
    config:
      source: "latency_ms"
      target: "latency_ms"
      type: "float64"
  
  - step: "extract_field"
    config:
      source: "datacenter"
      target: "datacenter"
      type: "string"
  
  - step: "extract_field"
    config:
      source: "rack"
      target: "rack"
      type: "string"
  
  # This is the problematic compute_field that causes the segfault
  - step: "compute_field"
    config:
      operation: "concat"
      target: "device_path"
      fields: ["datacenter", "/", "rack", "/", "device_id"]
`

	// Write config to temp file
	configFile := fmt.Sprintf("/tmp/test_compute_field_segfault_%d.yaml", time.Now().Unix())
	err = os.WriteFile(configFile, []byte(yamlConfig), 0o644)
	require.NoError(t, err, "Failed to write YAML config")
	defer os.Remove(configFile)

	p, err := parser.NewYAMLParser(configFile)
	require.NoError(t, err, "Failed to create YAML parser")

	// Create sink
	sinkOpts := sink.Options{
		ClusterUri:    qdbEndpoint,
		RetryAttempts: 3,
		PushMode:      qdb.WriterPushModeAsync,
		Compression:   qdb.CompNone,
	}

	s, err := sink.NewSink(sinkOpts)
	require.NoError(t, err, "Failed to create sink")
	defer s.Close()

	// Process messages in batches to trigger GC
	// We need to process ~10000 messages to reliably trigger the segfault
	// CRITICAL: Use batch size of 100 to match actual connector behavior
	numMessages := 10000
	batchSize := 100

	ctx := context.Background()
	startTime := time.Now()

	for i := 0; i < numMessages; i += batchSize {
		// Accumulate WriterTables for batch processing
		var batchTables []qdb.WriterTable

		for j := 0; j < batchSize && i+j < numMessages; j++ {
			msgNum := i + j

			// Create test message similar to network-metrics data
			// Use more realistic and varied data to increase memory pressure
			message := fmt.Sprintf(`{
				"timestamp": "%s",
				"device": {
					"info": {
						"id": "device-sensor-monitor-production-%d"
					}
				},
				"device_id": "device-sensor-monitor-production-%d",
				"datacenter": "datacenter-us-east-%d-production",
				"rack": "rack-row-%d-cabinet-%d-slot-%d",
				"metrics": {
					"network": {
						"inbound": {
							"bytes": %d
						},
						"outbound": {
							"bytes": %d
						}
					},
					"performance": {
						"latency_percentiles": {
							"p99": %.2f
						}
					}
				},
				"errors": {
					"packets": {
						"dropped": %d
					}
				},
				"bytes_in": %d,
				"bytes_out": %d,
				"packets_dropped": %d,
				"latency_ms": %.2f
			}`,
				startTime.Add(time.Duration(msgNum)*time.Second).Format(time.RFC3339Nano),
				msgNum,
				msgNum,
				msgNum%5,
				msgNum%10,
				msgNum%20,
				msgNum%100,
				msgNum*1000,
				msgNum*2000,
				float64(msgNum%50)+0.5,
				msgNum%100,
				msgNum*1000,
				msgNum*2000,
				msgNum%100,
				float64(msgNum%50)+0.5,
			)

			// Parse message - create a NATS message
			natsMsg := &nats.Msg{
				Data: []byte(message),
			}

			results, err := p.Parse(natsMsg)
			require.NoError(t, err, "Failed to parse message %d", msgNum)
			require.Len(t, results, 1, "Expected 1 writer table from parser")

			// Accumulate tables for batch
			batchTables = append(batchTables, results...)
		}

		// Merge WriterTables to handle duplicates (all messages go to same table)
		mergedTables, err := qdb.MergeWriterTables(batchTables)
		require.NoError(t, err, "Failed to merge writer tables for batch starting at %d", i)

		// Write batch to sink
		err = s.Write(ctx, mergedTables)
		require.NoError(t, err, "Failed to write batch starting at message %d", i)

		// Force GC to increase likelihood of triggering the race condition
		// GC after each batch to simulate production memory pressure
		runtime.GC()
		runtime.Gosched()
	}

	t.Logf("✓ Processed %d messages without segfault", numMessages)
}
