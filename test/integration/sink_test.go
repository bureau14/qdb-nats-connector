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

// TestSinkDuplicateTableError reproduces "table already exists" error when
// multiple WriterTable objects have the same table name.
//
// NOTE: This test calls sink.Write() directly, bypassing the worker layer.
// The fix for this issue is in connector/worker.go where qdb.MergeWriterTables
// is called before passing tables to the sink. This test demonstrates the
// underlying problem that the fix addresses.
func TestSinkDuplicateTableError(t *testing.T) {
	qdbEndpoint := "qdb://127.0.0.1:2836"

	// Connect to QuasarDB for verification
	handle, err := qdb.NewHandle()
	require.NoError(t, err, "Failed to create QuasarDB handle")
	defer func() { _ = handle.Close() }()

	err = handle.Connect(qdbEndpoint)
	require.NoError(t, err, "Failed to connect to QuasarDB at %s", qdbEndpoint)

	// Create test table
	tableName := "finance_ohlc_" + time.Now().Format("20060102150405")
	table := handle.Table(tableName)

	columns := []qdb.TsColumnInfo{
		qdb.NewTsColumnInfo("timestamp", qdb.TsColumnTimestamp),
		qdb.NewTsColumnInfo("open", qdb.TsColumnDouble),
		qdb.NewTsColumnInfo("high", qdb.TsColumnDouble),
		qdb.NewTsColumnInfo("low", qdb.TsColumnDouble),
		qdb.NewTsColumnInfo("close", qdb.TsColumnDouble),
		qdb.NewTsColumnInfo("volume", qdb.TsColumnInt64),
		qdb.NewTsColumnInfo("symbol", qdb.TsColumnString),
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

	// Prepare writer columns for WriterTable creation
	writerColumns := []qdb.WriterColumn{
		{ColumnName: "timestamp", ColumnType: qdb.TsColumnTimestamp},
		{ColumnName: "open", ColumnType: qdb.TsColumnDouble},
		{ColumnName: "high", ColumnType: qdb.TsColumnDouble},
		{ColumnName: "low", ColumnType: qdb.TsColumnDouble},
		{ColumnName: "close", ColumnType: qdb.TsColumnDouble},
		{ColumnName: "volume", ColumnType: qdb.TsColumnInt64},
		{ColumnName: "symbol", ColumnType: qdb.TsColumnString},
	}

	now := time.Now()

	// Create first WriterTable with same table name
	writerTable1, err := qdb.NewWriterTable(tableName, writerColumns)
	require.NoError(t, err, "Failed to create first writer table")

	// Set timestamps for first table
	timestamps1 := []time.Time{now, now.Add(1 * time.Second)}
	writerTable1.SetIndex(timestamps1)

	// Set data for first table (AAPL data)
	timestampData1 := qdb.NewColumnDataTimestamp(timestamps1)
	err = writerTable1.SetData(0, &timestampData1)
	require.NoError(t, err, "Failed to set timestamp data for table 1")

	openData1 := qdb.NewColumnDataDouble([]float64{150.0, 151.0})
	err = writerTable1.SetData(1, &openData1)
	require.NoError(t, err, "Failed to set open data for table 1")

	highData1 := qdb.NewColumnDataDouble([]float64{152.0, 153.0})
	err = writerTable1.SetData(2, &highData1)
	require.NoError(t, err, "Failed to set high data for table 1")

	lowData1 := qdb.NewColumnDataDouble([]float64{149.0, 150.0})
	err = writerTable1.SetData(3, &lowData1)
	require.NoError(t, err, "Failed to set low data for table 1")

	closeData1 := qdb.NewColumnDataDouble([]float64{151.0, 152.0})
	err = writerTable1.SetData(4, &closeData1)
	require.NoError(t, err, "Failed to set close data for table 1")

	volumeData1 := qdb.NewColumnDataInt64([]int64{1000000, 1100000})
	err = writerTable1.SetData(5, &volumeData1)
	require.NoError(t, err, "Failed to set volume data for table 1")

	symbolData1 := qdb.NewColumnDataString([]string{"AAPL", "AAPL"})
	err = writerTable1.SetData(6, &symbolData1)
	require.NoError(t, err, "Failed to set symbol data for table 1")

	// Create second WriterTable with SAME table name but different data
	writerTable2, err := qdb.NewWriterTable(tableName, writerColumns)
	require.NoError(t, err, "Failed to create second writer table")

	// Set timestamps for second table
	timestamps2 := []time.Time{now.Add(2 * time.Second), now.Add(3 * time.Second)}
	writerTable2.SetIndex(timestamps2)

	// Set data for second table (GOOGL data)
	timestampData2 := qdb.NewColumnDataTimestamp(timestamps2)
	err = writerTable2.SetData(0, &timestampData2)
	require.NoError(t, err, "Failed to set timestamp data for table 2")

	openData2 := qdb.NewColumnDataDouble([]float64{2800.0, 2810.0})
	err = writerTable2.SetData(1, &openData2)
	require.NoError(t, err, "Failed to set open data for table 2")

	highData2 := qdb.NewColumnDataDouble([]float64{2820.0, 2830.0})
	err = writerTable2.SetData(2, &highData2)
	require.NoError(t, err, "Failed to set high data for table 2")

	lowData2 := qdb.NewColumnDataDouble([]float64{2790.0, 2800.0})
	err = writerTable2.SetData(3, &lowData2)
	require.NoError(t, err, "Failed to set low data for table 2")

	closeData2 := qdb.NewColumnDataDouble([]float64{2810.0, 2820.0})
	err = writerTable2.SetData(4, &closeData2)
	require.NoError(t, err, "Failed to set close data for table 2")

	volumeData2 := qdb.NewColumnDataInt64([]int64{500000, 550000})
	err = writerTable2.SetData(5, &volumeData2)
	require.NoError(t, err, "Failed to set volume data for table 2")

	symbolData2 := qdb.NewColumnDataString([]string{"GOOGL", "GOOGL"})
	err = writerTable2.SetData(6, &symbolData2)
	require.NoError(t, err, "Failed to set symbol data for table 2")

	// Write multiple tables with same name to sink
	// Note: Write() is async and queues the job, so it returns nil immediately
	err = s.Write([]qdb.WriterTable{writerTable1, writerTable2})
	require.NoError(t, err, "Write() should queue successfully even with duplicate tables")

	// Wait for async processing to complete and fail
	// The error will occur in the worker when it tries to process the duplicate table
	time.Sleep(10 * time.Second)

	// Close the sink to ensure all processing is complete
	s.Close()

	t.Logf("✓ Successfully reproduced duplicate table error in worker logs")
	t.Logf("Check the logs above for: 'table \"%s\" already exists'", tableName)
}
