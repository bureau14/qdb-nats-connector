//go:build integration
// +build integration

// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package integration provides integration test helpers.
package integration

import (
	"testing"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/internal/sink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSinkDirectWrite writes directly to sink without NATS/parser.
func TestSinkDirectWrite(t *testing.T) {
	qdbEndpoint := "qdb://127.0.0.1:2836"

	// Connect to QuasarDB for verification
	handle, err := qdb.NewHandle()
	require.NoError(t, err, "Failed to create QuasarDB handle")
	defer func() { _ = handle.Close() }()

	err = handle.Connect(qdbEndpoint)
	require.NoError(t, err, "Failed to connect to QuasarDB at %s", qdbEndpoint)

	// Create test table
	tableName := "test_sink_direct_" + time.Now().Format("20060102150405")
	table := handle.Table(tableName)

	columns := []qdb.TsColumnInfo{
		qdb.NewTsColumnInfo("temperature", qdb.TsColumnDouble),
		qdb.NewTsColumnInfo("humidity", qdb.TsColumnDouble),
		qdb.NewTsColumnInfo("location", qdb.TsColumnString),
	}

	err = table.Create(24*time.Hour, columns...)
	require.NoError(t, err, "Failed to create test table")
	defer func() { _ = table.Remove() }()

	// Create sink with minimal configuration
	sinkOpts := sink.Options{
		ClusterUri:    qdbEndpoint,
		NumWriters:    1,
		QueueSize:     10,
		RetryAttempts: 3,
		PushMode:      qdb.WriterPushModeAsync,
		Compression:   qdb.CompNone,
	}

	s, err := sink.NewSink(sinkOpts)
	require.NoError(t, err, "Failed to create sink")
	defer s.Close()

	// Prepare test data
	now := time.Now()

	// Create WriterTable using the API
	writerColumns := []qdb.WriterColumn{
		{ColumnName: "temperature", ColumnType: qdb.TsColumnDouble},
		{ColumnName: "humidity", ColumnType: qdb.TsColumnDouble},
		{ColumnName: "location", ColumnType: qdb.TsColumnString},
	}

	writerTable, err := qdb.NewWriterTable(tableName, writerColumns)
	require.NoError(t, err, "Failed to create writer table")

	// Set timestamps
	timestamps := []time.Time{now, now.Add(1 * time.Second)}
	writerTable.SetIndex(timestamps)

	// Set data for each column
	// Temperature column
	tempData := qdb.NewColumnDataDouble([]float64{25.5, 26.0})
	err = writerTable.SetData(0, &tempData)
	require.NoError(t, err, "Failed to set temperature data")

	// Humidity column
	humidityData := qdb.NewColumnDataDouble([]float64{60.0, 65.0})
	err = writerTable.SetData(1, &humidityData)
	require.NoError(t, err, "Failed to set humidity data")

	// Location column
	locationData := qdb.NewColumnDataString([]string{"room1", "room2"})
	err = writerTable.SetData(2, &locationData)
	require.NoError(t, err, "Failed to set location data")

	// Write to sink
	err = s.Write([]qdb.WriterTable{writerTable})
	require.NoError(t, err, "Failed to write to sink")

	// Wait for async write to complete
	time.Sleep(2 * time.Second)

	// Verify data was written using the new Reader API
	reader, err := qdb.NewReader(handle, qdb.NewReaderOptions().
		WithTables([]string{tableName}).
		WithTimeRange(now.Add(-1*time.Second), now.Add(2*time.Second)))
	require.NoError(t, err, "Failed to create reader")
	defer reader.Close()

	// Count rows using FetchAll and RowCount
	chunk, err := reader.FetchAll()
	require.NoError(t, err, "Failed to fetch data")

	rowCount := chunk.RowCount()

	assert.Equal(t, 2, rowCount, "Expected 2 rows written to QuasarDB")
	t.Logf("✓ Sink direct write test passed: %d rows written to table %s", rowCount, tableName)
}
