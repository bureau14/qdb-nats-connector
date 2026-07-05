//go:build integration

// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
package connector

import (
	"context"
	"fmt"
	"testing"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/connector/hooks"
	"github.com/bureau14/qdb-nats-connector/connector/resilience"
	"github.com/bureau14/qdb-nats-connector/internal/filter"
	"github.com/bureau14/qdb-nats-connector/internal/sink"
	"github.com/bureau14/qdb-nats-connector/internal/source"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// integrationParser produces one single-row WriterTable per message. The first
// byte of msg.Data is cast to int64 and stored as the data_type value;
// val is also used as a unique timestamp offset so rows never collide.
type integrationParser struct {
	cols      []qdb.WriterColumn
	tableName string
}

// Parse returns a single-row table carrying data_type == msg.Data[0].
func (p *integrationParser) Parse(msg *nats.Msg) ([]qdb.WriterTable, error) {
	val := int64(msg.Data[0])
	tbl, err := qdb.NewWriterTable(p.tableName, p.cols)
	if err != nil {
		return nil, err
	}
	cd := qdb.NewColumnDataInt64([]int64{val})
	err = tbl.SetData(0, &cd)
	if err != nil {
		return nil, err
	}
	// Use val as a unique sub-second timestamp offset (nanoseconds).
	tbl.SetIndex([]time.Time{time.Unix(0, val)})

	return []qdb.WriterTable{tbl}, nil
}

// TestPartialFilterWorkerIntegration verifies that a batch with data_type values
// {1,2,3} under a whitelist filter {1,2} writes only the kept rows to QuasarDB
// and ACKs all message sequences -- including the data_type==3 row that was dropped.
func TestPartialFilterWorkerIntegration(t *testing.T) {
	qdbEndpoint := "qdb://127.0.0.1:2836"
	tableName := "test_partial_filter_" + time.Now().Format("20060102150405")

	// Use a fixed int64 column matching the data_type schema.
	// The \x00 terminator mirrors what createColumnMapping appends.
	cols := []qdb.WriterColumn{
		{ColumnName: "data_type\x00", ColumnType: qdb.TsColumnInt64},
	}

	// Build a whitelist filter that keeps data_type in {1,2} and drops 3.
	rf, err := filter.New(filter.Spec{
		Mode: "whitelist",
		Match: []filter.MatchEntry{
			{Column: "data_type", Value: 1},
			{Column: "data_type", Value: 2},
		},
	}, cols)
	require.NoError(t, err)
	require.NotNil(t, rf)

	// Connect to QuasarDB and create the test table.
	handle, err := qdb.NewHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()
	require.NoError(t, handle.Connect(qdbEndpoint))

	table := handle.Table(tableName)
	require.NoError(t, table.Create(0,
		qdb.NewTsColumnInfo("data_type", qdb.TsColumnInt64),
	))
	defer func() { _ = table.Remove() }()

	// Build a real sink connected to the local QuasarDB instance.
	qdbSink, err := sink.NewSink(sink.Options{ClusterUri: qdbEndpoint})
	require.NoError(t, err)
	defer qdbSink.Close()

	// Create a minimal circuit breaker so the write path doesn't panic.
	cb := resilience.NewCircuitBreaker(5, 2, time.Minute)

	w := &Worker{
		id:             0,
		workerID:       "worker-0",
		parser:         &integrationParser{cols: cols, tableName: tableName},
		rowFilter:      rf,
		sink:           qdbSink,
		circuitBreaker: cb,
		hooks:          hooks.NewHookRegistry(),
	}

	var acked, nacked []uint64
	batch := &source.MessageBatch{
		Messages: []source.MessageInfo{
			{Msg: &nats.Msg{Subject: "s", Data: []byte{1}}, Sequence: 1},
			{Msg: &nats.Msg{Subject: "s", Data: []byte{2}}, Sequence: 2},
			{Msg: &nats.Msg{Subject: "s", Data: []byte{3}}, Sequence: 3},
		},
		AckFunc: func(seqs []uint64) error {
			acked = append(acked, seqs...)
			return nil
		},
		NackFunc: func(seqs []uint64) error {
			nacked = append(nacked, seqs...)
			return nil
		},
	}

	require.NoError(t, w.processBatchFromChannel(context.Background(), batch))

	// All three sequences must be ACKed (including the dropped data_type==3).
	assert.ElementsMatch(t, []uint64{1, 2, 3}, acked, "all sequences must be ACKed")
	assert.Empty(t, nacked, "no sequence must be NACKed")

	// Verify only 2 rows were written (data_type 1 and 2; data_type 3 was filtered).
	// QDB async push is used; give it a moment to flush.
	time.Sleep(500 * time.Millisecond)
	result, queryErr := handle.Query(
		fmt.Sprintf(`SELECT * FROM "%s"`, tableName),
	).Execute()
	require.NoError(t, queryErr)
	assert.Equal(t, int64(2), result.RowCount(),
		"expected 2 rows written (data_type 1 and 2); data_type 3 was filtered")
}
