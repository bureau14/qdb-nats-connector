//go:build integration
// +build integration

// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.

// Package integration provides integration test helpers.
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/connector"
	"github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/nats-io/nats.go"
	"pgregory.net/rapid"
)

// ConnectorConfig holds the configuration parameters for integration tests.
// It provides all necessary connection details for both NATS and QuasarDB,
// along with optional security credentials and parser selection.
type ConnectorConfig struct {
	// NATSEndpoint is the NATS server URL (e.g., "nats://localhost:4222")
	NATSEndpoint    string
	
	// QDBEndpoint is the QuasarDB cluster URI (e.g., "qdb://localhost:2836")
	QDBEndpoint     string
	
	// QDBPublicKey is the optional QuasarDB cluster public key for secure connections
	QDBPublicKey    string
	
	// QDBUserSecurity is the optional QuasarDB user security credentials
	QDBUserSecurity string
	
	// Parser specifies which message parser to use (currently only "json" is supported)
	Parser          string
}

// Test environment configuration
const (
	defaultNATSEndpoint = "nats://localhost:4222"
	defaultQDBCluster   = "qdb://127.0.0.1:2836"
)

// getEnvOrDefault returns env var or default.
// Internal helper for test configuration.
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// =============================================================================
// Data Conversion Helpers
// =============================================================================

// ColumnDataToJSON converts dataset rows to JSON messages with $table/$timestamp fields.
// Returns JSON array for NATS publishing.
func ColumnDataToJSON(columnDataSet *ColumnDataSet) [][]byte {
	if columnDataSet == nil {
		return nil
	}

	numRows := columnDataSet.NumRows()
	messages := make([][]byte, numRows)

	for i := 0; i < numRows; i++ {
		// Create JSON object for this row
		jsonObj := make(map[string]interface{})

		// Add $table field
		jsonObj["$table"] = columnDataSet.Schema.Name

		// Add $timestamp field in ISO 8601 format with nanosecond precision
		jsonObj["$timestamp"] = columnDataSet.Timestamps[i].Format("2006-01-02T15:04:05.999999999Z")

		// Add column data
		for j, column := range columnDataSet.Schema.Columns {
			jsonObj[column.ColumnName] = columnDataSet.ColumnData[j][i]
		}

		// Marshal to JSON
		jsonBytes, err := json.Marshal(jsonObj)
		if err != nil {
			// This should not happen with our controlled data
			panic(fmt.Sprintf("failed to marshal JSON: %v", err))
		}

		messages[i] = jsonBytes
	}

	return messages
}

// =============================================================================
// NATS Helper Functions
// =============================================================================

// GenerateRandomSubject creates rapid generator for NATS subjects (1-3 parts, dots).
// Parts: 2-10 chars, alphanumeric, letter start.
func GenerateRandomSubject() *rapid.Generator[string] {
	return rapid.Custom(func(t *rapid.T) string {
		// Generate 1-3 subject parts separated by dots
		numParts := rapid.IntRange(1, 3).Draw(t, "num_parts")
		parts := make([]string, numParts)

		for i := 0; i < numParts; i++ {
			// Each part is alphanumeric, 2-10 characters
			part := rapid.StringMatching(`^[a-zA-Z][a-zA-Z0-9]*$`).
				Filter(func(s string) bool {
					return len(s) >= 2 && len(s) <= 10
				}).Draw(t, fmt.Sprintf("part_%d", i))
			parts[i] = part
		}

		return strings.Join(parts, ".")
	})
}

// PublishJSONMessages publishes JSON messages to NATS subject with flush.
// Sequential publish for test synchronization.
func PublishJSONMessages(nc *nats.Conn, subject string, messages [][]byte) error {
	if nc == nil {
		return errors.NewConnectionFailedError("nats_helper", "nil connection", nil)
	}

	for i, msg := range messages {
		if err := nc.Publish(subject, msg); err != nil {
			return errors.NewConnectionFailedError("nats_helper",
				fmt.Sprintf("failed to publish message %d", i), err)
		}
	}

	// Flush to ensure all messages are sent
	if err := nc.Flush(); err != nil {
		return errors.NewConnectionFailedError("nats_helper", "failed to flush", err)
	}

	return nil
}

// PublishJSONMessagesSync publishes messages with proper synchronization.
// Instead of time.Sleep, this uses JetStream's synchronous guarantees.
func PublishJSONMessagesSync(nc *nats.Conn, subject string, messages [][]byte) error {
	if nc == nil {
		return errors.NewConnectionFailedError("nats_helper", "nil connection", nil)
	}

	// Get JetStream context for synchronous publishing
	js, err := nc.JetStream()
	if err != nil {
		return errors.NewConnectionFailedError("nats_helper", "failed to get JetStream context", err)
	}

	// Publish messages synchronously with ack wait
	for i, msg := range messages {
		_, err := js.Publish(subject, msg)
		if err != nil {
			return errors.NewConnectionFailedError("nats_helper",
				fmt.Sprintf("failed to publish message %d", i), err)
		}
	}

	// No additional flush needed - JetStream publish waits for ack
	return nil
}

// =============================================================================
// QuasarDB Helper Functions
// =============================================================================

// CreateTableFromSchema creates QDB timeseries table from schema.
// Uses 24-hour shards for testing.
func CreateTableFromSchema(handle qdb.HandleType, schema *TableSchema) error {
	// Note: HandleType is a struct, so we can't check for nil
	// Instead we'll let any invalid handle fail during table operations

	// Get the timeseries entry for this table name
	table := handle.Timeseries(schema.Name)

	// Convert schema.Columns to []qdb.TsColumnInfo
	columns := make([]qdb.TsColumnInfo, len(schema.Columns))
	for i, col := range schema.Columns {
		columns[i] = qdb.NewTsColumnInfo(col.ColumnName, col.ColumnType)
	}

	// Create table with 24-hour shard size
	if err := table.Create(24*time.Hour, columns...); err != nil {
		return errors.NewWriteFailedError("CreateTableFromSchema", err)
	}

	return nil
}

// ReadAllData reads all QDB table data from epoch to now+1h.
// Supports blob/string types, returns strings for comparison.
func ReadAllData(handle qdb.HandleType, tableName string) (*ColumnDataSet, error) {
	// Note: HandleType is a struct, so we can't check for nil
	// Instead we'll let any invalid handle fail during operations

	// Get the timeseries table
	table := handle.Timeseries(tableName)

	// Get table schema (columns info)
	columnsInfo, err := table.ColumnsInfo()
	if err != nil {
		return nil, errors.NewConnectionFailedError("ReadAllData",
			fmt.Sprintf("failed to get columns info for table %s", tableName), err)
	}

	// Convert to WriterColumn format for schema
	columns := make([]qdb.WriterColumn, len(columnsInfo))
	for i, colInfo := range columnsInfo {
		columns[i] = qdb.WriterColumn{
			ColumnName: colInfo.Name(),
			ColumnType: colInfo.Type(),
		}
	}

	schema := TableSchema{
		Name:    tableName,
		Columns: columns,
	}

	// Get the time range for the table
	// Start from epoch to get all data
	beginTime := time.Unix(0, 0)
	endTime := time.Now().Add(time.Hour) // Add buffer for future timestamps

	// Since the Reader API with private fields is not accessible, we need to use a different approach.
	// For now, let's use a simple approach: use the old bulk API with proper error handling
	// and note that this migration will need the Reader API to be updated to provide public access.
	// TODO: This is a temporary solution until the Reader API provides public field access.
	
	// Create bulk reader with all columns (keeping old approach for now)
	bulk, err := table.Bulk(columnsInfo...)
	if err != nil {
		return nil, errors.NewConnectionFailedError("ReadAllData",
			fmt.Sprintf("failed to create bulk reader for table %s", tableName), err)
	}
	defer bulk.Release()

	// Get data for the time range
	ranges := []qdb.TsRange{qdb.NewRange(beginTime, endTime)}
	if err := bulk.GetRanges(ranges...); err != nil {
		return nil, errors.NewConnectionFailedError("ReadAllData",
			fmt.Sprintf("failed to get ranges for table %s", tableName), err)
	}

	var timestamps []time.Time
	columnData := make([][]string, len(columns))

	// Initialize column data slices
	for i := range columnData {
		columnData[i] = make([]string, 0)
	}

	// Read all rows
	for {
		timestamp, err := bulk.NextRow()
		if err != nil {
			break // End of data or error
		}

		timestamps = append(timestamps, timestamp)

		// Read data for each column
		for i, col := range columns {
			var value string

			// Read column data based on type
			switch col.ColumnType {
			case qdb.TsColumnTypes[0]: // blob type
				blobData, err := bulk.GetBlob()
				if err != nil {
					value = "" // Default for missing/error values
				} else {
					value = string(blobData)
				}
			case qdb.TsColumnTypes[3]: // string type
				stringData, err := bulk.GetString()
				if err != nil {
					value = "" // Default for missing/error values
				} else {
					value = stringData
				}
			default:
				// For other types, convert to string representation
				value = fmt.Sprintf("unsupported_type_%v", col.ColumnType)
			}

			columnData[i] = append(columnData[i], value)
		}
	}

	return &ColumnDataSet{
		Schema:     schema,
		Timestamps: timestamps,
		ColumnData: columnData,
	}, nil
}

// CompareColumnData compares datasets for equality with timestamp sorting.
// Validates schema, row count, and values after sorting.
func CompareColumnData(expected, actual *ColumnDataSet) error {
	if expected == nil && actual == nil {
		return nil
	}
	if expected == nil {
		return fmt.Errorf("expected is nil but actual is not")
	}
	if actual == nil {
		return fmt.Errorf("actual is nil but expected is not")
	}

	// Compare table names
	if expected.Schema.Name != actual.Schema.Name {
		return fmt.Errorf("table names differ: expected %s, got %s",
			expected.Schema.Name, actual.Schema.Name)
	}

	// Compare number of columns
	if len(expected.Schema.Columns) != len(actual.Schema.Columns) {
		return fmt.Errorf("column count differs: expected %d, got %d",
			len(expected.Schema.Columns), len(actual.Schema.Columns))
	}

	// Compare column schemas
	for i, expectedCol := range expected.Schema.Columns {
		actualCol := actual.Schema.Columns[i]
		if expectedCol.ColumnName != actualCol.ColumnName {
			return fmt.Errorf("column %d name differs: expected %s, got %s",
				i, expectedCol.ColumnName, actualCol.ColumnName)
		}
		if expectedCol.ColumnType != actualCol.ColumnType {
			return fmt.Errorf("column %d type differs: expected %v, got %v",
				i, expectedCol.ColumnType, actualCol.ColumnType)
		}
	}

	// Compare number of rows
	if len(expected.Timestamps) != len(actual.Timestamps) {
		return fmt.Errorf("row count differs: expected %d, got %d",
			len(expected.Timestamps), len(actual.Timestamps))
	}

	// Sort both datasets by timestamp for comparison
	expectedSorted := sortColumnDataByTimestamp(expected)
	actualSorted := sortColumnDataByTimestamp(actual)

	// Compare sorted data
	for i, expectedTime := range expectedSorted.Timestamps {
		actualTime := actualSorted.Timestamps[i]
		if !expectedTime.Equal(actualTime) {
			return fmt.Errorf("timestamp %d differs: expected %s, got %s",
				i, expectedTime.Format(time.RFC3339Nano), actualTime.Format(time.RFC3339Nano))
		}

		// Compare column data for this row
		for j := range expectedSorted.Schema.Columns {
			expectedValue := expectedSorted.ColumnData[j][i]
			actualValue := actualSorted.ColumnData[j][i]
			if expectedValue != actualValue {
				return fmt.Errorf("row %d, column %s differs: expected %s, got %s",
					i, expectedSorted.Schema.Columns[j].ColumnName, expectedValue, actualValue)
			}
		}
	}

	return nil
}

// sortColumnDataByTimestamp sorts dataset by timestamp.
// Internal helper for consistent comparison.
func sortColumnDataByTimestamp(data *ColumnDataSet) *ColumnDataSet {
	if len(data.Timestamps) <= 1 {
		return data
	}

	// Create index array for sorting
	indices := make([]int, len(data.Timestamps))
	for i := range indices {
		indices[i] = i
	}

	// Sort indices by timestamp
	sort.Slice(indices, func(i, j int) bool {
		return data.Timestamps[indices[i]].Before(data.Timestamps[indices[j]])
	})

	// Create sorted result
	sorted := &ColumnDataSet{
		Schema:     data.Schema,
		Timestamps: make([]time.Time, len(data.Timestamps)),
		ColumnData: make([][]string, len(data.ColumnData)),
	}

	// Initialize column data slices
	for j := range sorted.ColumnData {
		sorted.ColumnData[j] = make([]string, len(data.Timestamps))
	}

	// Rearrange data according to sorted indices
	for newIdx, originalIdx := range indices {
		sorted.Timestamps[newIdx] = data.Timestamps[originalIdx]
		for j := range data.ColumnData {
			sorted.ColumnData[j][newIdx] = data.ColumnData[j][originalIdx]
		}
	}

	return sorted
}

// =============================================================================
// Connector Helper Functions
// =============================================================================

// RunConnectorForSubject runs connector for subject with 30s timeout.
// Uses async mode, TEST_STREAM for testing.
func RunConnectorForSubject(subject string, cfg ConnectorConfig) error {
	// Build command line arguments for configuration
	args := []string{
		"--nats", cfg.NATSEndpoint,
		"--topic", subject,
		"--stream", "TEST_STREAM", // Required for JetStream
		"--qdb", cfg.QDBEndpoint,
	}

	// Add performance settings
	args = append(args, "--qdb-push-mode", "async")

	// Load configuration using the proper method
	loadedOpts, err := connector.LoadConfig(args, func() {})
	if err != nil {
		return errors.NewConnectionFailedError("RunConnectorForSubject", "failed to load config", err)
	}

	// Create connector (parser is created internally by workers)
	conn, err := connector.NewConnector(loadedOpts)
	if err != nil {
		return errors.NewConnectionFailedError("RunConnectorForSubject", "failed to create connector", err)
	}
	defer conn.Close()

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Run connector with context
	return conn.RunWithContext(ctx)
}
