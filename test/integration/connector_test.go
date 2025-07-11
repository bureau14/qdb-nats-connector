// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package integration provides integration test helpers.
package integration

import (
	"context"
	"testing"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/nats-io/nats.go"
	"pgregory.net/rapid"
)

// getTestConfig returns test configuration.
// Internal helper for connection setup.
func getTestConfig() ConnectorConfig {
	return ConnectorConfig{
		NATSEndpoint: getEnvOrDefault("NATS_ENDPOINT", defaultNATSEndpoint),
		QDBEndpoint:  getEnvOrDefault("QDB_CLUSTER_URI", defaultQDBCluster),
	}
}

// TestConnectorJSONToQuasarDB tests complete NATS→JSON→QuasarDB flow.
func TestConnectorJSONToQuasarDB(t *testing.T) {
	cfg := getTestConfig()

	// Connect to NATS
	nc, err := nats.Connect(cfg.NATSEndpoint)
	if err != nil {
		t.Fatalf("NATS not available at %s: %v", cfg.NATSEndpoint, err)
	}
	defer nc.Close()

	// Connect to QuasarDB
	handle, err := qdb.NewHandle()
	if err != nil {
		t.Fatalf("QuasarDB handle creation failed: %v", err)
	}
	defer handle.Close()

	err = handle.Connect(cfg.QDBEndpoint)
	if err != nil {
		t.Fatalf("QuasarDB not available at %s: %v", cfg.QDBEndpoint, err)
	}

	// Property-based test using rapid
	rapid.Check(t, func(t *rapid.T) {
		testConnectorFlow(t, nc, handle, cfg)
	})
}

// testConnectorFlow implements the main test logic for property-based testing
func testConnectorFlow(t *rapid.T, nc *nats.Conn, handle qdb.HandleType, cfg ConnectorConfig) {
	// Step 1: Generate test input using rapid
	t.Logf("Step 1: Generating test data")
	inputData := ColumnDataSetGen().Draw(t, "input_data")

	// Generate unique subject for this test
	subject := GenerateRandomSubject().Draw(t, "nats_subject")

	t.Logf("Generated test case: table=%s, columns=%d, rows=%d, subject=%s",
		inputData.Schema.Name, inputData.NumColumns(), inputData.NumRows(), subject)

	// Step 2: Create QuasarDB table with generated schema
	t.Logf("Step 2: Creating QuasarDB table")
	err := CreateTableFromSchema(handle, &inputData.Schema)
	if err != nil {
		t.Fatalf("Failed to create QuasarDB table: %v", err)
	}

	// Ensure cleanup of table after test
	defer func() {
		// Cleanup table after test
		table := handle.Timeseries(inputData.Schema.Name)
		err := table.Remove()
		if err != nil {
			t.Logf("Warning: Failed to cleanup table %s: %v", inputData.Schema.Name, err)
		}
	}()

	// Step 3: Convert ColumnData to JSON and publish to NATS
	t.Logf("Step 3: Converting data to JSON and publishing to NATS")
	jsonMessages := ColumnDataToJSON(&inputData)
	if len(jsonMessages) == 0 {
		t.Fatalf("No JSON messages generated from input data")
	}

	err = PublishJSONMessages(nc, subject, jsonMessages)
	if err != nil {
		t.Fatalf("Failed to publish JSON messages: %v", err)
	}

	t.Logf("Published %d JSON messages to subject %s", len(jsonMessages), subject)

	// Step 4: Run connector with JSON parser
	t.Logf("Step 4: Running connector to process messages")

	// Run connector in a goroutine with a timeout
	connectorDone := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		connectorDone <- RunConnectorForSubject(subject, cfg)
	}()

	// Wait for connector to process messages or timeout
	select {
	case err := <-connectorDone:
		if err != nil && err != context.DeadlineExceeded {
			t.Fatalf("Connector failed: %v", err)
		}
		t.Logf("Connector completed processing")
	case <-ctx.Done():
		t.Logf("Connector timed out (expected for successful processing)")
	}

	// Give a brief moment for final writes to complete
	time.Sleep(100 * time.Millisecond)

	// Step 5: Read data from QuasarDB
	t.Logf("Step 5: Reading data from QuasarDB")
	outputData, err := ReadAllData(handle, inputData.Schema.Name)
	if err != nil {
		t.Fatalf("Failed to read data from QuasarDB: %v", err)
	}

	t.Logf("Read %d rows from QuasarDB table %s", outputData.NumRows(), outputData.Schema.Name)

	// Step 6: Compare input vs output ColumnData
	t.Logf("Step 6: Comparing input and output data")
	err = CompareColumnData(&inputData, outputData)
	if err != nil {
		t.Fatalf("Data comparison failed: %v", err)
	}

	t.Logf("✓ Test passed: All data correctly processed through NATS→JSON→QuasarDB flow")
}
