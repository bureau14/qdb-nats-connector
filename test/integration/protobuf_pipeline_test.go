//go:build integration

// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.

// End-to-end protobuf pipeline test: synthetic test.v1 envelopes published
// into a hermetic JetStream, consumed by the full connector, decoded by the
// composed SKF-shaped YAML pipeline (parse_protobuf x2, extract_map_entry
// allowed_keys capability filter, extract_subject, compute_field split),
// written to a real QuasarDB table. Pins the three outcome classes
// end-to-end under --parse-error-action drop:
//   - valid messages   -> exact rows, nanosecond-exact index
//   - malformed wire   -> structural drop (counted, no row)
//   - disallowed key   -> structural drop at extract_map_entry (counted)
//   - wrong inner type -> OutcomePartial -> sentinel-filled row WRITTEN
package integration

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/connector"
	"github.com/bureau14/qdb-nats-connector/internal/parser/prototest"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	protoQDBEndpoint  = "qdb://127.0.0.1:2836"
	protoStreamName   = "PROTO_IT"
	protoConsumerName = "proto-it-consumer"
	protoSubject      = "it.proto.1.0.ion.streams.ABCDEFG=.value"
	// updated_at for every message: renders as a fixed 9-digit RFC3339
	// string through formatProtoTimestamp.
	protoIngestedAt = "2023-11-14T22:13:21.000000042Z"
)

// protoRowExpect describes one expected row keyed by its index timestamp.
type protoRowExpect struct {
	token      string
	value      float64 // NaN = sentinel expected (partial row)
	capability int64
	revision   int64
	streamID   string
	ingestedAt string
}

// protoPipelineYAML renders the composed SKF-shaped pipeline config with a
// RELATIVE descriptor_file, exercising config-dir resolution end-to-end.
func protoPipelineYAML(tableName string) string {
	return `
output:
  columns:
    - name: "stream_token"
      type: "string"
    - name: "value"
      type: "double"
    - name: "capability_key"
      type: "int64"
    - name: "revision"
      type: "int64"
    - name: "stream_id"
      type: "string"
    - name: "ingested_at"
      type: "string"

transformations:
  - step: "parse_protobuf"
    config:
      descriptor_file: "envelope.desc"
      message_type: "` + prototest.EnvelopeType + `"
  - step: "extract_map_entry"
    config:
      source: "blobs"
      key_target: "capability_key"
      value_target: "attribute_raw"
      on_multiple: "first"
      allowed_keys: ["3"]
  - step: "parse_protobuf"
    config:
      descriptor_file: "envelope.desc"
      message_type: "` + prototest.InnerType + `"
      source: "attribute_raw"
      target: "attr"
  - step: "extract_subject"
    config:
      target: "stream_token"
      segment: -2
      trim: "="
  - step: "compute_field"
    config:
      operation: "split"
      source: "name"
      separator: ":"
      index: -1
      target: "revision_raw"
  - step: "extract_table"
    config:
      value: "` + tableName + `"
  - step: "extract_index"
    config:
      source: "created_at"
      format: "rfc3339nano"
  - step: "extract_field"
    config: { source: "stream_token", target: "stream_token", type: "string" }
  - step: "extract_field"
    config: { source: "attr.reading", target: "value", type: "float64" }
  - step: "extract_field"
    config: { source: "capability_key", target: "capability_key", type: "int64" }
  - step: "extract_field"
    config: { source: "revision_raw", target: "revision", type: "int64" }
  - step: "extract_field"
    config: { source: "name", target: "stream_id", type: "string" }
  - step: "extract_field"
    config: { source: "updated_at", target: "ingested_at", type: "string" }
`
}

// writeProtoPipelineConfig writes the YAML config and the descriptor SIDE BY
// SIDE into dir (the relocatable deployment bundle shape); returns the
// config path.
func writeProtoPipelineConfig(t *testing.T, dir, tableName string) string {
	t.Helper()

	prototest.WriteDescriptor(t, dir)

	configPath := filepath.Join(dir, "pipeline.yaml")
	require.NoError(t, os.WriteFile(configPath, []byte(protoPipelineYAML(tableName)), 0o600))

	return configPath
}

// createProtoTable creates the QDB output table matching the pipeline's
// output schema; removed via t.Cleanup.
func createProtoTable(t *testing.T, handle qdb.HandleType, tableName string) []qdb.TsColumnInfo {
	t.Helper()

	cols := []qdb.TsColumnInfo{
		qdb.NewTsColumnInfo("stream_token", qdb.TsColumnString),
		qdb.NewTsColumnInfo("value", qdb.TsColumnDouble),
		qdb.NewTsColumnInfo("capability_key", qdb.TsColumnInt64),
		qdb.NewTsColumnInfo("revision", qdb.TsColumnInt64),
		qdb.NewTsColumnInfo("stream_id", qdb.TsColumnString),
		qdb.NewTsColumnInfo("ingested_at", qdb.TsColumnString),
	}

	table := handle.Table(tableName)
	require.NoError(t, table.Create(24*time.Hour, cols...))
	t.Cleanup(func() { _ = table.Remove() })

	return cols
}

// protoEnvelope marshals a pipeline-shaped envelope: name, one blobs entry,
// and both timestamps (updated_at fixed at the protoIngestedAt instant).
func protoEnvelope(t *testing.T, name string, blobKey int32, blobValue []byte, sec int64, nanos int32) []byte {
	t.Helper()

	return prototest.MarshalEnvelope(t, func(m protoreflect.Message) {
		prototest.SetStringField(m, "name", name)
		prototest.SetBlobsEntry(m, blobKey, blobValue)
		prototest.SetTimestampField(m, "created_at", sec, nanos)
		prototest.SetTimestampField(m, "updated_at", 1700000001, 42)
	})
}

// publishProtoMix publishes the interleaved message mix and returns the
// expected rows keyed by index UnixNano: 5 valid + 1 partial = 6 rows;
// 2 malformed + 2 disallowed-key = 4 drops.
func publishProtoMix(t *testing.T, js nats.JetStreamContext) map[int64]protoRowExpect {
	t.Helper()

	expected := map[int64]protoRowExpect{}
	nanosPerValid := []int32{123456789, 42, 1, 999999999, 0}

	publish := func(payload []byte) {
		_, err := js.Publish(protoSubject, payload)
		require.NoError(t, err)
	}

	for i, nanos := range nanosPerValid {
		n := i + 1
		sec := int64(1700000000 + n*10)
		name := fmt.Sprintf("stream:token:%d", n)
		reading := float64(n) + 0.5

		publish(protoEnvelope(t, name, 3, prototest.MarshalInner(t, reading, "mm"), sec, nanos))
		expected[time.Unix(sec, int64(nanos)).UnixNano()] = protoRowExpect{
			token: "ABCDEFG", value: reading, capability: 3,
			revision: int64(n), streamID: name, ingestedAt: protoIngestedAt,
		}

		switch n {
		case 1, 4: // malformed wire data: field-1 LEN claims 5 bytes, 1 present
			publish([]byte{0x0A, 0x05, 0x01})
		case 2, 5: // disallowed capability key -> extract_map_entry drop
			publish(protoEnvelope(t, "drop:me:1", 4, prototest.MarshalInner(t, 1.0, "x"), sec, nanos+1))
		}
	}

	// Wrong inner type: key 3 carries a serialized Envelope; nested decode
	// yields zero populated fields -> attr.reading missing -> partial row.
	wrongInner := prototest.MarshalEnvelope(t, func(m protoreflect.Message) {
		prototest.SetStringField(m, "name", "not-an-inner")
	})
	publish(protoEnvelope(t, "x:y:9", 3, wrongInner, 1700000100, 777000111))
	expected[time.Unix(1700000100, 777000111).UnixNano()] = protoRowExpect{
		token: "ABCDEFG", value: math.NaN(), capability: 3,
		revision: 9, streamID: "x:y:9", ingestedAt: protoIngestedAt,
	}

	return expected
}

// startConnector loads options from args and runs the connector in a
// goroutine. The returned stop func cancels, waits for RunWithContext to
// return (context.Canceled = clean), and Closes. The done channel surfaces
// early startup failures to poll loops.
func startConnector(t *testing.T, args []string) (*connector.Connector, <-chan error, func()) {
	t.Helper()

	opts, err := connector.LoadConfig(args, func() {})
	require.NoError(t, err)

	conn, err := connector.NewConnector(opts)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- conn.RunWithContext(ctx) }()

	stop := func() {
		cancel()
		select {
		case runErr := <-done:
			if runErr != nil && !errors.Is(runErr, context.Canceled) {
				t.Errorf("RunWithContext returned: %v", runErr)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("connector did not stop within 30s")
		}
		conn.Close()
	}

	return conn, done, stop
}

// pollProtoState polls until the table holds wantRows rows AND the connector
// counted wantDropped drops, failing on deadline or early connector exit.
func pollProtoState(t *testing.T, handle qdb.HandleType, tableName string,
	conn *connector.Connector, done <-chan error, wantRows int64, wantDropped uint64,
) {
	t.Helper()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case runErr := <-done:
			t.Fatalf("connector exited early: %v", runErr)
		default:
		}

		result, err := handle.Query(fmt.Sprintf(`SELECT * FROM "%s"`, tableName)).Execute()
		if err == nil && result.RowCount() == wantRows &&
			conn.Stats().MessagesDropped == wantDropped {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatalf("timed out: stats %+v", conn.Stats())
}

// readProtoRows bulk-reads all rows typed, keyed by index UnixNano.
func readProtoRows(t *testing.T, handle qdb.HandleType, tableName string,
	cols []qdb.TsColumnInfo,
) map[int64]protoRowExpect {
	t.Helper()

	table := handle.Table(tableName)
	bulk, err := table.Bulk(cols...)
	require.NoError(t, err)
	defer bulk.Release()

	require.NoError(t, bulk.GetRanges(qdb.NewRange(time.Unix(0, 0), time.Now().Add(time.Hour))))

	rows := map[int64]protoRowExpect{}
	for {
		ts, rowErr := bulk.NextRow()
		if rowErr != nil {
			break
		}

		var row protoRowExpect
		row.token = mustGetString(t, bulk)
		row.value, err = bulk.GetDouble()
		require.NoError(t, err)
		row.capability = mustGetInt64(t, bulk)
		row.revision = mustGetInt64(t, bulk)
		row.streamID = mustGetString(t, bulk)
		row.ingestedAt = mustGetString(t, bulk)

		rows[ts.UnixNano()] = row
	}

	return rows
}

func mustGetString(t *testing.T, bulk *qdb.TsBulk) string {
	t.Helper()

	v, err := bulk.GetString()
	require.NoError(t, err)

	return v
}

func mustGetInt64(t *testing.T, bulk *qdb.TsBulk) int64 {
	t.Helper()

	v, err := bulk.GetInt64()
	require.NoError(t, err)

	return v
}

// assertProtoRows compares actual rows to expected: nanosecond-exact index
// keys, exact values, and the NaN sentinel for the partial row.
func assertProtoRows(t *testing.T, expected, actual map[int64]protoRowExpect) {
	t.Helper()

	require.Len(t, actual, len(expected))

	for key, want := range expected {
		got, ok := actual[key]
		require.True(t, ok, "missing row at index %v", time.Unix(0, key).UTC())

		assert.Equal(t, want.token, got.token, "stream_token at %d", key)
		assert.Equal(t, want.capability, got.capability, "capability_key at %d", key)
		assert.Equal(t, want.revision, got.revision, "revision at %d", key)
		assert.Equal(t, want.streamID, got.streamID, "stream_id at %d", key)
		assert.Equal(t, want.ingestedAt, got.ingestedAt, "ingested_at at %d", key)

		if math.IsNaN(want.value) {
			assert.True(t, math.IsNaN(got.value), "value at %d: want NaN sentinel, got %v", key, got.value)
		} else {
			assert.Equal(t, want.value, got.value, "value at %d", key)
		}
	}
}

// TestProtobufPipelineEndToEnd is the M3 exit criterion: the composed
// protobuf pipeline against real NATS JetStream and QuasarDB, with exact
// drop accounting via Connector.Stats after a drained shutdown.
func TestProtobufPipelineEndToEnd(t *testing.T) {
	url := startNatsServer(t, t.TempDir())
	js := jetStreamContext(t, url)

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     protoStreamName,
		Subjects: []string{"it.proto.>"},
	})
	require.NoError(t, err)
	addDurablePullConsumer(t, js, protoStreamName, protoConsumerName, "it.proto.1.0.ion.streams.*.value")

	handle, err := qdb.NewHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()
	require.NoError(t, handle.Connect(protoQDBEndpoint))

	tableName := fmt.Sprintf("test_proto_pipeline_%d", time.Now().UnixNano())
	cols := createProtoTable(t, handle, tableName)

	expected := publishProtoMix(t, js)

	configPath := writeProtoPipelineConfig(t, t.TempDir(), tableName)
	conn, done, stop := startConnector(t, []string{
		"--nats", url,
		"--stream", protoStreamName,
		"--consumer", protoConsumerName,
		"--qdb", protoQDBEndpoint,
		"--parser", "yaml",
		"--parser-config", configPath,
		"--qdb-push-mode", "fast",
		"--parse-error-action", "drop",
	})

	// 6 rows (5 valid + 1 sentinel-filled partial); 4 drops (2 malformed +
	// 2 disallowed keys). The 2 stream-seeding probes on it.proto.other are
	// outside the consumer's FilterSubject and never reach the pipeline.
	pollProtoState(t, handle, tableName, conn, done, int64(len(expected)), 4)

	stop()

	// Workers are drained after stop: counters are exact.
	stats := conn.Stats()
	assert.Equal(t, uint64(4), stats.MessagesDropped, "dropped = malformed + disallowed")
	assert.Equal(t, uint64(0), stats.ParseFailures, "nothing may be NACKed")
	assert.Equal(t, uint64(10), stats.MessagesProcessed, "all pipeline messages ACKed")

	assertProtoRows(t, expected, readProtoRows(t, handle, tableName, cols))
}
