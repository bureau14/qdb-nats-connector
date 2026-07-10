// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
package connector

import (
	"context"
	"fmt"
	"testing"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/connector/hooks"
	"github.com/bureau14/qdb-nats-connector/internal/filter"
	"github.com/bureau14/qdb-nats-connector/internal/parser"
	"github.com/bureau14/qdb-nats-connector/internal/source"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeParser returns a fixed classified result per message, ignoring content.
// outcome selects the classification; for OK/Partial a single-row table
// carrying val in its first column is produced, or zero tables when
// zeroTables is set (a valid zero-row parse, e.g. explode over an empty
// array).
type fakeParser struct {
	cols       []qdb.WriterColumn
	val        int64
	outcome    parser.Outcome
	zeroTables bool
}

// Parse returns the configured outcome with a single-row table for OK/Partial.
func (p *fakeParser) Parse(_ *nats.Msg) (parser.ParseResult, error) {
	if p.outcome == parser.OutcomeUnusable {
		return parser.ParseResult{
			Outcome: parser.OutcomeUnusable,
			Errors:  []error{fmt.Errorf("undecodable payload")},
		}, nil
	}

	if p.zeroTables {
		return parser.ParseResult{Outcome: p.outcome}, nil
	}

	tbl, err := qdb.NewWriterTable("t1", p.cols)
	if err != nil {
		return parser.ParseResult{}, err
	}
	cd := qdb.NewColumnDataInt64([]int64{p.val})
	err = tbl.SetData(0, &cd)
	if err != nil {
		return parser.ParseResult{}, err
	}
	tbl.SetIndex([]time.Time{time.Unix(0, 0)})

	res := parser.ParseResult{Tables: []qdb.WriterTable{tbl}, Outcome: p.outcome}
	if p.outcome == parser.OutcomePartial {
		res.Errors = []error{fmt.Errorf("field missing")}
	}

	return res, nil
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
	}, cols, nil)
	require.NoError(t, err)
	require.NotNil(t, rf)

	w := &Worker{
		id:               0,
		workerID:         "worker-0",
		parser:           &fakeParser{cols: cols, val: 1},
		rowFilter:        rf,
		hooks:            hooks.NewHookRegistry(),
		parseErrorAction: "drop",
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

// makeBatch builds a 3-message batch capturing ACKs/NACKs into the given slices.
func makeBatch(acked, nacked *[]uint64) *source.MessageBatch {
	return &source.MessageBatch{
		Messages: []source.MessageInfo{
			{Msg: &nats.Msg{Subject: "s", Data: []byte("x")}, Sequence: 1},
			{Msg: &nats.Msg{Subject: "s", Data: []byte("x")}, Sequence: 2},
			{Msg: &nats.Msg{Subject: "s", Data: []byte("x")}, Sequence: 3},
		},
		AckFunc: func(seqs []uint64) error {
			*acked = append(*acked, seqs...)

			return nil
		},
		NackFunc: func(seqs []uint64) error {
			*nacked = append(*nacked, seqs...)

			return nil
		},
	}
}

// TestProcessBatchUnusablePolicy mirrors the all-filtered test for batches
// whose messages are ALL structurally unusable: drop mode counts them as
// dropped and ACKs without a write; fail mode NACKs them for redelivery.
func TestProcessBatchUnusablePolicy(t *testing.T) {
	testCases := []struct {
		name              string
		mode              string
		wantAcked         []uint64
		wantNacked        []uint64
		wantDropped       uint64
		wantParseFailures uint64
	}{
		{"drop acks without write", "drop", []uint64{1, 2, 3}, nil, 3, 0},
		{"fail nacks for redelivery", "fail", nil, []uint64{1, 2, 3}, 0, 3},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cols := []qdb.WriterColumn{{ColumnName: "value\x00", ColumnType: qdb.TsColumnInt64}}

			w := &Worker{
				id:               0,
				workerID:         "worker-0",
				parser:           &fakeParser{cols: cols, outcome: parser.OutcomeUnusable},
				hooks:            hooks.NewHookRegistry(),
				parseErrorAction: tc.mode,
			}

			var acked, nacked []uint64
			batch := makeBatch(&acked, &nacked)

			// nil sink: a write attempt would panic, so completion proves no write.
			require.NoError(t, w.processBatchFromChannel(context.Background(), batch))
			assert.ElementsMatch(t, tc.wantAcked, acked)
			assert.ElementsMatch(t, tc.wantNacked, nacked)

			stats := w.GetStats()
			assert.Equal(t, tc.wantDropped, stats.MessagesDropped)
			assert.Equal(t, tc.wantParseFailures, stats.ParseFailures)
		})
	}
}

// singleRowTable builds a one-row WriterTable named as given (the parser
// convention: names carry the trailing "\x00" pinning terminator).
func singleRowTable(t *testing.T, name string) qdb.WriterTable {
	t.Helper()

	cols := []qdb.WriterColumn{{ColumnName: "value\x00", ColumnType: qdb.TsColumnInt64}}
	tbl, err := qdb.NewWriterTable(name, cols)
	require.NoError(t, err)

	cd := qdb.NewColumnDataInt64([]int64{1})
	require.NoError(t, tbl.SetData(0, &cd))
	tbl.SetIndex([]time.Time{time.Unix(0, 0)})

	return tbl
}

// TestTableWritesForAggregatesAndTrims pins the per-table event contract:
// same-name single-row tables aggregate into one event, and the "\x00"
// pinning terminator never leaks into event names.
func TestTableWritesForAggregatesAndTrims(t *testing.T) {
	w := &Worker{onTableWrites: func([]TableWrite) {}}

	events := w.tableWritesFor([]qdb.WriterTable{
		singleRowTable(t, "a\x00"),
		singleRowTable(t, "a\x00"),
		singleRowTable(t, "b\x00"),
	})

	assert.ElementsMatch(t, []TableWrite{
		{Table: "a", Rows: 2, Msgs: 2},
		{Table: "b", Rows: 1, Msgs: 1},
	}, events)
}

// TestTableWritesForNilCallback pins the disabled hot path: no callback
// means no slice is built at all.
func TestTableWritesForNilCallback(t *testing.T) {
	w := &Worker{}

	assert.Nil(t, w.tableWritesFor([]qdb.WriterTable{singleRowTable(t, "a\x00")}))
}

// TestEmitTableWritesGates pins that the callback only fires with a
// non-nil callback and non-empty events.
func TestEmitTableWritesGates(t *testing.T) {
	var calls int
	w := &Worker{onTableWrites: func(events []TableWrite) {
		calls++
		assert.NotEmpty(t, events)
	}}

	w.emitTableWrites(nil)
	assert.Equal(t, 0, calls)

	w.emitTableWrites([]TableWrite{{Table: "a", Rows: 1, Msgs: 1}})
	assert.Equal(t, 1, calls)

	unset := &Worker{}
	assert.NotPanics(t, func() {
		unset.emitTableWrites([]TableWrite{{Table: "a", Rows: 1, Msgs: 1}})
	})
}

// TestParseMessagesPolicyTable pins the full mode x outcome policy matrix
// at the parseMessages level.
func TestParseMessagesPolicyTable(t *testing.T) {
	cols := []qdb.WriterColumn{{ColumnName: "value\x00", ColumnType: qdb.TsColumnInt64}}

	testCases := []struct {
		name        string
		mode        string
		outcome     parser.Outcome
		wantTables  int
		wantFailed  int
		wantDropped uint64
	}{
		{"drop keeps ok", "drop", parser.OutcomeOK, 3, 0, 0},
		{"drop keeps partial sentinel rows", "drop", parser.OutcomePartial, 3, 0, 0},
		{"drop discards unusable", "drop", parser.OutcomeUnusable, 0, 0, 3},
		{"fail keeps ok", "fail", parser.OutcomeOK, 3, 0, 0},
		{"fail nacks partial", "fail", parser.OutcomePartial, 0, 3, 0},
		{"fail nacks unusable", "fail", parser.OutcomeUnusable, 0, 3, 0},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			w := &Worker{
				id:               0,
				workerID:         "worker-0",
				parser:           &fakeParser{cols: cols, val: 1, outcome: tc.outcome},
				parseErrorAction: tc.mode,
			}

			var acked, nacked []uint64
			validTables, failedSeqs := w.parseMessages(makeBatch(&acked, &nacked))

			assert.Len(t, validTables, tc.wantTables)
			assert.Len(t, failedSeqs, tc.wantFailed)
			stats := w.GetStats()
			assert.Equal(t, tc.wantDropped, stats.MessagesDropped)
			assert.Equal(t, uint64(tc.wantFailed), stats.ParseFailures) //nolint:gosec // test count, no overflow
		})
	}
}

// TestProcessBatchZeroRowParseAcksAndCounts is the regression test for valid
// zero-row parses (OutcomeOK with zero tables, e.g. a terminal explode over
// an empty sample array): every sequence must ACK, none must NACK, no write
// is attempted, and the parses_zero_rows counter must observe each one.
func TestProcessBatchZeroRowParseAcksAndCounts(t *testing.T) {
	cols := []qdb.WriterColumn{{ColumnName: "value\x00", ColumnType: qdb.TsColumnInt64}}

	w := &Worker{
		id:               0,
		workerID:         "worker-0",
		parser:           &fakeParser{cols: cols, outcome: parser.OutcomeOK, zeroTables: true},
		hooks:            hooks.NewHookRegistry(),
		parseErrorAction: "drop",
	}

	var acked, nacked []uint64
	batch := makeBatch(&acked, &nacked)

	// nil sink: a write attempt would panic, so completion proves no write.
	require.NoError(t, w.processBatchFromChannel(context.Background(), batch))
	assert.ElementsMatch(t, []uint64{1, 2, 3}, acked, "all sequences must be ACKed")
	assert.Empty(t, nacked, "no sequence must be NACKed")

	stats := w.GetStats()
	assert.Equal(t, uint64(3), stats.ParsesZeroRows)
	assert.Equal(t, uint64(3), stats.MessagesProcessed)
	assert.Equal(t, uint64(0), stats.MessagesDropped)
	assert.Equal(t, uint64(0), stats.RowsWritten)
}
