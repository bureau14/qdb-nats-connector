// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
package filter

import (
	"errors"
	"testing"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// int64Col / strCol build \x00-terminated columns mirroring createColumnMapping.
func int64Col(name string) qdb.WriterColumn {
	return qdb.WriterColumn{ColumnName: name + "\x00", ColumnType: qdb.TsColumnInt64}
}

func strCol(name string) qdb.WriterColumn {
	return qdb.WriterColumn{ColumnName: name + "\x00", ColumnType: qdb.TsColumnString}
}

// singleRowTable builds a one-row table; vals[i] must match cols[i]'s type
// (int64 or string).
func singleRowTable(t *testing.T, cols []qdb.WriterColumn, vals ...interface{}) qdb.WriterTable {
	t.Helper()
	tbl, err := qdb.NewWriterTable("t", cols)
	require.NoError(t, err)
	for i, v := range vals {
		switch x := v.(type) {
		case int64:
			cd := qdb.NewColumnDataInt64([]int64{x})
			require.NoError(t, tbl.SetData(i, &cd))
		case string:
			cd := qdb.NewColumnDataString([]string{x})
			require.NoError(t, tbl.SetData(i, &cd))
		default:
			t.Fatalf("unsupported test value type %T", v)
		}
	}
	tbl.SetIndex([]time.Time{time.Unix(0, 0)})

	return tbl
}

// assertInvalidConfig checks that err wraps a ConnectorError with ErrCodeInvalidConfig.
func assertInvalidConfig(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var connErr *connectorErrors.ConnectorError
	require.True(t, errors.As(err, &connErr), "expected *ConnectorError, got %T", err)
	assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
}

// TestNewEmptySpec verifies that an empty Spec produces a nil filter (pass-through).
func TestNewEmptySpec(t *testing.T) {
	f, err := New(Spec{}, []qdb.WriterColumn{int64Col("x")}, nil)
	require.NoError(t, err)
	assert.Nil(t, f)
}

// TestNewInvalidMode verifies that an unrecognised mode string is rejected.
func TestNewInvalidMode(t *testing.T) {
	_, err := New(
		Spec{Mode: "bogus", Match: []MatchEntry{{Column: "x", Value: 1}}},
		[]qdb.WriterColumn{int64Col("x")},
		nil,
	)
	assertInvalidConfig(t, err)
}

// TestNewEmptyMatchList verifies that a mode without match entries is rejected.
func TestNewEmptyMatchList(t *testing.T) {
	_, err := New(
		Spec{Mode: "whitelist"},
		[]qdb.WriterColumn{int64Col("x")},
		nil,
	)
	assertInvalidConfig(t, err)
}

// TestNewUnknownColumn verifies that a match referencing an absent column is rejected.
func TestNewUnknownColumn(t *testing.T) {
	_, err := New(
		Spec{Mode: "whitelist", Match: []MatchEntry{{Column: "missing", Value: 1}}},
		[]qdb.WriterColumn{int64Col("x")},
		nil,
	)
	assertInvalidConfig(t, err)
}

// TestNewUnsupportedColumnType verifies that double and timestamp columns are rejected.
func TestNewUnsupportedColumnType(t *testing.T) {
	for _, col := range []qdb.WriterColumn{
		{ColumnName: "d\x00", ColumnType: qdb.TsColumnDouble},
		{ColumnName: "ts\x00", ColumnType: qdb.TsColumnTimestamp},
	} {
		_, err := New(
			Spec{Mode: "whitelist", Match: []MatchEntry{{Column: col.ColumnName[:len(col.ColumnName)-1], Value: 1}}},
			[]qdb.WriterColumn{col},
			nil,
		)
		assertInvalidConfig(t, err)
	}
}

// TestNewValueTypeMismatch verifies that a string value for an int64 column
// and an int value for a string column are each rejected.
func TestNewValueTypeMismatch(t *testing.T) {
	_, err := New(
		Spec{Mode: "whitelist", Match: []MatchEntry{{Column: "x", Value: "string-for-int64"}}},
		[]qdb.WriterColumn{int64Col("x")},
		nil,
	)
	assertInvalidConfig(t, err)

	_, err = New(
		Spec{Mode: "whitelist", Match: []MatchEntry{{Column: "s", Value: 42}}},
		[]qdb.WriterColumn{strCol("s")},
		nil,
	)
	assertInvalidConfig(t, err)
}

// TestNewNullTerminatorStripping verifies that a filter predicate for "data_type"
// correctly matches against a \x00-terminated column name, as produced by createColumnMapping.
func TestNewNullTerminatorStripping(t *testing.T) {
	f, err := New(
		Spec{Mode: "whitelist", Match: []MatchEntry{{Column: "data_type", Value: 1}}},
		[]qdb.WriterColumn{int64Col("data_type")},
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, f)
}

// TestApplyNilFilter verifies that a nil *RowFilter returns its input unchanged.
func TestApplyNilFilter(t *testing.T) {
	cols := []qdb.WriterColumn{int64Col("v")}
	tbl := singleRowTable(t, cols, int64(1))
	tables := []qdb.WriterTable{tbl}

	var f *RowFilter
	result := f.Apply(tables)
	assert.Equal(t, tables, result)
}

// TestModeFilterInt64 verifies whitelist and blacklist semantics for int64 columns.
// Both modes share the same predicate; the mode determines what is kept vs dropped.
func TestModeFilterInt64(t *testing.T) {
	cols := []qdb.WriterColumn{int64Col("v")}

	t.Run("whitelist keeps matching row and drops non-matching row", func(t *testing.T) {
		f, err := New(Spec{Mode: "whitelist", Match: []MatchEntry{{Column: "v", Value: 1}}}, cols, nil)
		require.NoError(t, err)

		kept := singleRowTable(t, cols, int64(1))
		dropped := singleRowTable(t, cols, int64(2))

		result := f.Apply([]qdb.WriterTable{kept, dropped})
		require.Len(t, result, 1)
		assert.Equal(t, kept, result[0], "whitelist must retain the matching row")
	})

	t.Run("blacklist drops matching row and keeps non-matching row", func(t *testing.T) {
		f, err := New(Spec{Mode: "blacklist", Match: []MatchEntry{{Column: "v", Value: 1}}}, cols, nil)
		require.NoError(t, err)

		dropped := singleRowTable(t, cols, int64(1))
		kept := singleRowTable(t, cols, int64(2))

		result := f.Apply([]qdb.WriterTable{dropped, kept})
		require.Len(t, result, 1)
		assert.Equal(t, kept, result[0], "blacklist must drop the matching row")
	})
}

// TestORSemanticsMultipleMatches verifies that a whitelist with multiple predicates
// keeps a row if it matches any one of them (OR semantics).
func TestORSemanticsMultipleMatches(t *testing.T) {
	cols := []qdb.WriterColumn{int64Col("dt")}
	f, err := New(
		Spec{
			Mode: "whitelist",
			Match: []MatchEntry{
				{Column: "dt", Value: 1},
				{Column: "dt", Value: 2},
			},
		},
		cols,
		nil,
	)
	require.NoError(t, err)

	t1 := singleRowTable(t, cols, int64(1))
	t2 := singleRowTable(t, cols, int64(2))
	t3 := singleRowTable(t, cols, int64(3))

	result := f.Apply([]qdb.WriterTable{t1, t2, t3})
	require.Len(t, result, 2)
	assert.Equal(t, t1, result[0])
	assert.Equal(t, t2, result[1])
}

// TestStringColumnWhitelist verifies whitelist filtering on a string column and
// confirms that row-0 string values read via GetColumnDataString are clean (no \x00).
func TestStringColumnWhitelist(t *testing.T) {
	cols := []qdb.WriterColumn{strCol("tag")}
	f, err := New(
		Spec{Mode: "whitelist", Match: []MatchEntry{{Column: "tag", Value: "keep"}}},
		cols,
		nil,
	)
	require.NoError(t, err)

	keep := singleRowTable(t, cols, "keep")
	drop := singleRowTable(t, cols, "drop")

	result := f.Apply([]qdb.WriterTable{keep, drop})
	require.Len(t, result, 1)
	assert.Equal(t, keep, result[0])
}

// TestNewRejectsExplodedColumns verifies that predicates referencing
// per-sample (exploded) columns are rejected at config load: Apply
// evaluates row 0 only, which would be silently wrong for them. Broadcast
// columns stay filterable.
func TestNewRejectsExplodedColumns(t *testing.T) {
	cols := []qdb.WriterColumn{int64Col("sample_index"), int64Col("capability_key")}

	_, err := New(
		Spec{Mode: "whitelist", Match: []MatchEntry{{Column: "sample_index", Value: 0}}},
		cols,
		[]string{"value", "sample_index"},
	)
	assertInvalidConfig(t, err)
	assert.Contains(t, err.Error(), "exploded column")

	f, err := New(
		Spec{Mode: "whitelist", Match: []MatchEntry{{Column: "capability_key", Value: 4}}},
		cols,
		[]string{"value", "sample_index"},
	)
	require.NoError(t, err)
	assert.NotNil(t, f)
}
