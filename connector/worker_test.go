// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
package connector

import (
	"context"
	"testing"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/connector/hooks"
	"github.com/bureau14/qdb-nats-connector/internal/filter"
	"github.com/bureau14/qdb-nats-connector/internal/source"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeParser returns one fixed single-row table per message, ignoring content.
type fakeParser struct {
	cols []qdb.WriterColumn
	val  int64
}

// Parse returns a single-row table carrying p.val in its first column.
func (p *fakeParser) Parse(_ *nats.Msg) ([]qdb.WriterTable, error) {
	tbl, err := qdb.NewWriterTable("t1", p.cols)
	if err != nil {
		return nil, err
	}
	cd := qdb.NewColumnDataInt64([]int64{p.val})
	err = tbl.SetData(0, &cd)
	if err != nil {
		return nil, err
	}
	tbl.SetIndex([]time.Time{time.Unix(0, 0)})

	return []qdb.WriterTable{tbl}, nil
}

// TestProcessBatchAllRowsFilteredStillAcks is the regression test for the
// early-return ACK gap: a batch whose rows are ALL dropped by the filter must
// still ACK every (non-parse-failed) sequence and NACK none.
func TestProcessBatchAllRowsFilteredStillAcks(t *testing.T) {
	cols := []qdb.WriterColumn{{ColumnName: "value\x00", ColumnType: qdb.TsColumnInt64}}

	// whitelist value==999 never matches the parser's value==1 -> all dropped.
	rf, err := filter.New(filter.Spec{
		Mode:  "whitelist",
		Match: []filter.MatchEntry{{Column: "value", Value: 999}},
	}, cols)
	require.NoError(t, err)
	require.NotNil(t, rf)

	w := &Worker{
		id:        0,
		workerID:  "worker-0",
		parser:    &fakeParser{cols: cols, val: 1},
		rowFilter: rf,
		hooks:     hooks.NewHookRegistry(),
	}

	var acked, nacked []uint64
	batch := &source.MessageBatch{
		Messages: []source.MessageInfo{
			{Msg: &nats.Msg{Subject: "s", Data: []byte("x")}, Sequence: 1},
			{Msg: &nats.Msg{Subject: "s", Data: []byte("x")}, Sequence: 2},
			{Msg: &nats.Msg{Subject: "s", Data: []byte("x")}, Sequence: 3},
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
	assert.ElementsMatch(t, []uint64{1, 2, 3}, acked, "all sequences must be ACKed")
	assert.Empty(t, nacked, "no sequence must be NACKed")
}
