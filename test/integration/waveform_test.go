//go:build integration

// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package integration: waveform explode write-path tests against real QDB.
// Coverage: N-row/single-row merge alignment, ~819k-row batch push, and
// broadcast string/symbol pinning. The CI harness runs these under
// GOEXPERIMENT=cgocheck2 -race (scripts/cicd/40.test-integration.sh), which
// is what makes the pinning test meaningful.
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"testing"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/internal/parser"
	"github.com/bureau14/qdb-nats-connector/internal/sink"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// waveformParserConfig builds a JSON-fed terminal-explode config routed to
// tableName: value (exploded double), sample_index (ordinal), stream_id
// (broadcast string), site (broadcast symbol).
func waveformParserConfig(tableName string) parser.YAMLConfig {
	return parser.YAMLConfig{
		Output: parser.OutputSchema{Columns: []parser.ColumnSchema{
			{Name: "value", Type: "double"},
			{Name: "sample_index", Type: "int64"},
			{Name: "stream_id", Type: "string"},
			{Name: "site", Type: "symbol"},
		}},
		Transformations: []parser.TransformSpec{
			{Step: "parse_json", Config: map[string]interface{}{}},
			{Step: "extract_timestamp", Config: map[string]interface{}{
				"source": "ts", "target": "reading_ts", "format": "rfc3339",
			}},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "stream_id", "target": "stream_id", "type": "string",
			}},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "site", "target": "site", "type": "string",
			}},
			{Step: "extract_table", Config: map[string]interface{}{"value": tableName}},
			{Step: "explode", Config: map[string]interface{}{
				"source": "samples", "target": "value", "ordinal": "sample_index",
				"index": map[string]interface{}{
					"start":    map[string]interface{}{"source": "reading_ts"},
					"interval": map[string]interface{}{"value": "200us"},
				},
			}},
		},
	}
}

// createWaveformTable creates the QDB table matching waveformParserConfig.
func createWaveformTable(t *testing.T, handle qdb.HandleType, tableName string) {
	t.Helper()

	table := handle.Table(tableName)
	err := table.Create(24*time.Hour,
		qdb.NewTsColumnInfo("value", qdb.TsColumnDouble),
		qdb.NewTsColumnInfo("sample_index", qdb.TsColumnInt64),
		qdb.NewTsColumnInfo("stream_id", qdb.TsColumnString),
		qdb.NewSymbolColumnInfo("site", tableName+"_sym_site"),
	)
	require.NoError(t, err, "failed to create table %s", tableName)

	t.Cleanup(func() { _ = table.Remove() })
}

// connectQDB opens a verification handle against the local test cluster.
func connectQDB(t *testing.T) qdb.HandleType {
	t.Helper()

	handle, err := qdb.NewHandle()
	require.NoError(t, err)
	require.NoError(t, handle.Connect("qdb://127.0.0.1:2836"))
	t.Cleanup(func() { _ = handle.Close() })

	return handle
}

// waveformSink opens a fast-push sink (synchronous visibility for
// deterministic readback; async rows can lag COUNT queries).
func waveformSink(t *testing.T) *sink.Sink {
	t.Helper()

	s, err := sink.NewSink(sink.Options{
		ClusterUri:    "qdb://127.0.0.1:2836",
		RetryAttempts: 3,
		PushMode:      qdb.WriterPushModeFast,
		Compression:   qdb.CompNone,
	})
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	return s
}

// waveformMsg builds one JSON waveform message.
func waveformMsg(t *testing.T, ts, streamID string, samples []float64) *nats.Msg {
	t.Helper()

	data, err := json.Marshal(map[string]interface{}{
		"ts": ts, "stream_id": streamID, "site": "plant-1", "samples": samples,
	})
	require.NoError(t, err)

	return &nats.Msg{Subject: "waveform.test", Data: data}
}

// parseWaveform parses one message, requiring a successful outcome.
func parseWaveform(t *testing.T, p parser.Parser, msg *nats.Msg) []qdb.WriterTable {
	t.Helper()

	res, err := p.Parse(msg)
	require.NoError(t, err)
	require.Equal(t, parser.OutcomeOK, res.Outcome, "errors: %v", res.Errors)

	return res.Tables
}

// tableRowCount queries the current row count.
func tableRowCount(t *testing.T, handle qdb.HandleType, tableName string) int64 {
	t.Helper()

	result, err := handle.Query(fmt.Sprintf(`SELECT COUNT(value) FROM %q`, tableName)).Execute()
	require.NoError(t, err)
	require.Equal(t, int64(1), result.RowCount())

	cols := result.Columns(result.Rows()[0])
	count, err := cols[0].GetCount()
	require.NoError(t, err)

	return count
}

// waveformRow: typed readback of one row for alignment checks.
type waveformRow struct {
	value       float64
	sampleIndex int64
	streamID    string
	site        string
}

// readWaveformRows bulk-reads all rows keyed by index UnixNano.
func readWaveformRows(t *testing.T, handle qdb.HandleType, tableName string) map[int64]waveformRow {
	t.Helper()

	table := handle.Table(tableName)
	cols, err := table.ColumnsInfo()
	require.NoError(t, err)

	bulk, err := table.Bulk(cols...)
	require.NoError(t, err)
	defer bulk.Release()

	require.NoError(t, bulk.GetRanges(qdb.NewRange(time.Unix(0, 0), time.Now().Add(24*time.Hour))))

	rows := map[int64]waveformRow{}
	for {
		ts, rowErr := bulk.NextRow()
		if rowErr != nil {
			break
		}

		var row waveformRow
		row.value, err = bulk.GetDouble()
		require.NoError(t, err)
		row.sampleIndex, err = bulk.GetInt64()
		require.NoError(t, err)
		row.streamID, err = bulk.GetString()
		require.NoError(t, err)
		row.site, err = bulk.GetString()
		require.NoError(t, err)

		rows[ts.UnixNano()] = row
	}

	return rows
}

// TestWaveformExplodedMergeReadback merges one 8192-row exploded table with
// several 1-row exploded tables for the SAME table name, pushes, and
// verifies (timestamp, value, ordinal, broadcast) alignment by typed
// readback -- the invariant merge itself never validates.
func TestWaveformExplodedMergeReadback(t *testing.T) {
	handle := connectQDB(t)
	tableName := "waveform_merge_" + time.Now().Format("20060102150405")
	createWaveformTable(t, handle, tableName)

	p, err := parser.NewYAMLParserFromConfig(waveformParserConfig(tableName))
	require.NoError(t, err)

	const interval = 200 * time.Microsecond
	start := time.Date(2026, 7, 9, 20, 0, 0, 0, time.UTC)

	samples := make([]float64, 8192)
	for i := range samples {
		samples[i] = float64(i) * 0.5
	}

	tables := parseWaveform(t, p, waveformMsg(t, start.Format(time.RFC3339), "stream-big", samples))
	for k := range 3 {
		scalarStart := start.Add(time.Duration(k+1) * time.Hour)
		tables = append(tables, parseWaveform(t, p,
			waveformMsg(t, scalarStart.Format(time.RFC3339), fmt.Sprintf("stream-%d", k), []float64{100.5 + float64(k)}))...)
	}

	merged, err := qdb.MergeWriterTables(tables)
	require.NoError(t, err)
	require.Len(t, merged, 1, "same-named tables must merge into one")
	assert.Equal(t, 8195, merged[0].RowCount())

	require.NoError(t, waveformSink(t).Write(context.Background(), merged))
	require.Equal(t, int64(8195), tableRowCount(t, handle, tableName))

	rows := readWaveformRows(t, handle, tableName)
	require.Len(t, rows, 8195)

	for _, i := range []int{0, 4095, 8191} {
		row, ok := rows[start.Add(time.Duration(i)*interval).UnixNano()]
		require.True(t, ok, "row %d missing at its derived timestamp", i)
		assert.Equal(t, samples[i], row.value, "row %d value", i)
		assert.Equal(t, int64(i), row.sampleIndex, "row %d ordinal", i)
		assert.Equal(t, "stream-big", row.streamID, "row %d broadcast string", i)
		assert.Equal(t, "plant-1", row.site, "row %d broadcast symbol", i)
	}

	for k := range 3 {
		row, ok := rows[start.Add(time.Duration(k+1)*time.Hour).UnixNano()]
		require.True(t, ok, "scalar row %d missing", k)
		assert.Equal(t, 100.5+float64(k), row.value)
		assert.Equal(t, int64(0), row.sampleIndex)
		assert.Equal(t, fmt.Sprintf("stream-%d", k), row.streamID)
	}
}

// TestWaveformLargeBatchPush pushes a worst-case fetch batch (100 messages
// x 8192 samples = 819,200 rows) through parse -> merge -> write, verifying
// the full count lands and logging latency + heap for the batch-size
// guidance (task-waveform.md section 6.2).
func TestWaveformLargeBatchPush(t *testing.T) {
	handle := connectQDB(t)
	tableName := "waveform_large_" + time.Now().Format("20060102150405")
	createWaveformTable(t, handle, tableName)

	p, err := parser.NewYAMLParserFromConfig(waveformParserConfig(tableName))
	require.NoError(t, err)

	samples := make([]float64, 8192)
	for i := range samples {
		samples[i] = float64(i) * 0.25
	}

	const messages = 100
	start := time.Date(2026, 7, 9, 0, 0, 0, 0, time.UTC)

	parseStart := time.Now()
	var tables []qdb.WriterTable
	for m := range messages {
		msgStart := start.Add(time.Duration(m) * 10 * time.Second) // > 8192*200us window
		tables = append(tables, parseWaveform(t, p,
			waveformMsg(t, msgStart.Format(time.RFC3339), "stream-load", samples))...)
	}

	merged, err := qdb.MergeWriterTables(tables)
	require.NoError(t, err)
	require.Len(t, merged, 1)
	require.Equal(t, messages*8192, merged[0].RowCount())

	writeStart := time.Now()
	require.NoError(t, waveformSink(t).Write(context.Background(), merged))

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	t.Logf("819200 rows: parse+merge %v, write %v, heap %d MiB",
		writeStart.Sub(parseStart), time.Since(writeStart), mem.HeapAlloc/(1<<20))

	require.Equal(t, int64(messages*8192), tableRowCount(t, handle, tableName))
}

// TestWaveformBroadcastStringsUnderCgocheck repeatedly pushes 8192-row
// tables whose string AND symbol columns repeat one string header 8192
// times -- the runtime.Pinner multi-pin pattern the exploded broadcast
// relies on. Any pinning violation trips cgocheck2/-race in CI.
func TestWaveformBroadcastStringsUnderCgocheck(t *testing.T) {
	handle := connectQDB(t)
	tableName := "waveform_pin_" + time.Now().Format("20060102150405")
	createWaveformTable(t, handle, tableName)

	p, err := parser.NewYAMLParserFromConfig(waveformParserConfig(tableName))
	require.NoError(t, err)

	samples := make([]float64, 8192)
	s := waveformSink(t)
	start := time.Date(2026, 7, 10, 0, 0, 0, 0, time.UTC)

	const pushes = 10
	for m := range pushes {
		msgStart := start.Add(time.Duration(m) * 10 * time.Second)
		tables := parseWaveform(t, p,
			waveformMsg(t, msgStart.Format(time.RFC3339), "stream-pin", samples))
		require.NoError(t, s.Write(context.Background(), tables))
	}

	require.Equal(t, int64(pushes*8192), tableRowCount(t, handle, tableName))
}
