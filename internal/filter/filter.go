// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package filter: pre-merge row filtering by typed column equality.
// Types: Mode, Spec, MatchEntry, RowFilter
// Ex: filter.New(spec, columns).Apply(tables) -> kept tables
package filter

import (
	"fmt"
	"strings"

	qdb "github.com/bureau14/qdb-api-go/v3"
	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
)

// Mode selects whitelist (keep matches) or blacklist (drop matches).
type Mode int

const (
	// ModeWhitelist keeps a row iff it matches at least one predicate.
	ModeWhitelist Mode = iota
	// ModeBlacklist drops a row iff it matches at least one predicate.
	ModeBlacklist
)

// Spec is the YAML-facing filter configuration block.
type Spec struct {
	Mode  string       `yaml:"mode"`
	Match []MatchEntry `yaml:"match"`
}

// MatchEntry is one "column == value" equality predicate from the config.
type MatchEntry struct {
	Column string      `yaml:"column"`
	Value  interface{} `yaml:"value"`
}

// match is one resolved equality predicate against a column offset. Exactly
// one of i64/str is non-nil, per the column's declared type.
type match struct {
	offset int
	i64    *int64
	str    *string
}

// RowFilter keeps or drops single-row WriterTables by column equality.
type RowFilter struct {
	mode    Mode
	matches []match
}

// New builds a RowFilter from a Spec and the ordered output columns. columns is
// the same slice the parser passes to NewWriterTable, so a column's index in
// the slice equals its GetData offset; column names carry the parser's \x00
// terminator, which New strips before matching. exploded lists the columns
// (without \x00) bound per-element by a terminal explode step (target +
// ordinal); nil/empty for scalar configs. Predicates evaluate row 0 only --
// exact for broadcast columns, silently wrong for per-sample ones -- so
// specs referencing exploded columns are rejected here at config load.
// Returns (nil, nil) when no filtering is configured (empty Spec ==
// pass-through). Rejects an invalid mode, an empty match list when a mode
// is set, unknown or exploded columns, unsupported column types (only int64
// and string), and values that do not match the column's declared type,
// each via connectorErrors.NewInvalidConfigError.
func New(spec Spec, columns []qdb.WriterColumn, exploded []string) (*RowFilter, error) {
	if spec.Mode == "" && len(spec.Match) == 0 {
		return nil, nil
	}

	mode, err := parseMode(spec.Mode)
	if err != nil {
		return nil, err
	}

	if len(spec.Match) == 0 {
		return nil, connectorErrors.NewInvalidConfigError("filter",
			"filters.match must be non-empty when filters.mode is set")
	}

	offsetByName, typeByName := indexColumns(columns)

	explodedSet := make(map[string]bool, len(exploded))
	for _, name := range exploded {
		explodedSet[name] = true
	}

	matches := make([]match, 0, len(spec.Match))
	for _, entry := range spec.Match {
		if explodedSet[entry.Column] {
			return nil, connectorErrors.NewInvalidConfigError("filter",
				fmt.Sprintf("filters.match references exploded column %q; filters evaluate row 0 only and cannot filter per-sample columns", entry.Column))
		}

		m, err := resolveMatch(entry, offsetByName, typeByName)
		if err != nil {
			return nil, err
		}
		matches = append(matches, m)
	}

	return &RowFilter{mode: mode, matches: matches}, nil
}

// parseMode maps the YAML mode string to a Mode, rejecting any other value.
func parseMode(s string) (Mode, error) {
	switch s {
	case "whitelist":
		return ModeWhitelist, nil
	case "blacklist":
		return ModeBlacklist, nil
	default:
		return 0, connectorErrors.NewInvalidConfigError("filter",
			fmt.Sprintf("filters.mode must be 'whitelist' or 'blacklist', got %q", s))
	}
}

// indexColumns builds name->offset and name->type maps from the ordered
// columns, stripping the parser's trailing \x00 terminator from each name.
func indexColumns(columns []qdb.WriterColumn) (offsetByName map[string]int, typeByName map[string]qdb.TsColumnType) {
	offsetByName = make(map[string]int, len(columns))
	typeByName = make(map[string]qdb.TsColumnType, len(columns))
	for i, col := range columns {
		name := strings.TrimSuffix(col.ColumnName, "\x00")
		offsetByName[name] = i
		typeByName[name] = col.ColumnType
	}

	return offsetByName, typeByName
}

// resolveMatch resolves one config predicate to a typed match against a column
// offset. Supports int64 and string columns only.
func resolveMatch(entry MatchEntry, offsetByName map[string]int, typeByName map[string]qdb.TsColumnType) (match, error) {
	offset, ok := offsetByName[entry.Column]
	if !ok {
		return match{}, connectorErrors.NewInvalidConfigError("filter",
			fmt.Sprintf("filters.match references unknown column %q", entry.Column))
	}

	switch typeByName[entry.Column] {
	case qdb.TsColumnInt64:
		v, err := coerceInt64(entry.Value, entry.Column)
		if err != nil {
			return match{}, err
		}

		return match{offset: offset, i64: &v}, nil
	case qdb.TsColumnString:
		v, err := coerceString(entry.Value, entry.Column)
		if err != nil {
			return match{}, err
		}

		return match{offset: offset, str: &v}, nil
	// timestamp, double, blob, symbol, and uninitialized are not supported
	// for equality filtering this iteration (see brief: Decision 3).
	case qdb.TsColumnTimestamp, qdb.TsColumnDouble, qdb.TsColumnBlob, qdb.TsColumnSymbol, qdb.TsColumnUninitialized:
		return match{}, connectorErrors.NewInvalidConfigError("filter",
			fmt.Sprintf("filters.match column %q has an unsupported type for filtering (only int64 and string are supported)", entry.Column))
	}
	// All TsColumnType values are handled above; this return is required by the
	// Go compiler which does not track switch exhaustiveness statically.
	return match{}, connectorErrors.NewInvalidConfigError("filter",
		fmt.Sprintf("filters.match column %q has an unsupported type for filtering (only int64 and string are supported)", entry.Column))
}

// coerceInt64 converts a YAML-decoded value to int64 for an int64 column.
func coerceInt64(v interface{}, column string) (int64, error) {
	switch n := v.(type) {
	case int:
		return int64(n), nil
	case int64:
		return n, nil
	default:
		return 0, connectorErrors.NewInvalidConfigError("filter",
			fmt.Sprintf("filters.match value for int64 column %q must be an integer, got %T", column, v))
	}
}

// coerceString converts a YAML-decoded value to string for a string column.
func coerceString(v interface{}, column string) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", connectorErrors.NewInvalidConfigError("filter",
			fmt.Sprintf("filters.match value for string column %q must be a string, got %T", column, v))
	}

	return s, nil
}

// Apply returns the subset of tables to keep. It runs pre-merge (before
// MergeWriterTables, never after) and evaluates row 0 only: exact for
// scalar configs (single-row tables) AND for exploded N-row tables, because
// every filterable column there is a broadcast column -- New rejects specs
// referencing per-sample (exploded) columns at config load. A nil receiver
// is pass-through (returns tables unchanged), covering the noop-parser and
// no-filter cases.
func (f *RowFilter) Apply(tables []qdb.WriterTable) []qdb.WriterTable {
	if f == nil {
		return tables
	}

	kept := make([]qdb.WriterTable, 0, len(tables))
	for i := range tables {
		matched := f.rowMatches(&tables[i])
		// whitelist keeps matches; blacklist keeps non-matches.
		if (f.mode == ModeWhitelist) == matched {
			kept = append(kept, tables[i])
		}
	}

	return kept
}

// rowMatches reports whether row 0 of the table satisfies at least one
// predicate (OR semantics across matches).
func (f *RowFilter) rowMatches(table *qdb.WriterTable) bool {
	for _, m := range f.matches {
		if matchRow0(table, m) {
			return true
		}
	}

	return false
}

// matchRow0 reports whether row 0 of the table equals the predicate's typed
// value at its column offset. A read error or empty column is treated as
// no-match.
func matchRow0(table *qdb.WriterTable, m match) bool {
	cd, err := table.GetData(m.offset)
	if err != nil {
		return false
	}

	switch {
	case m.i64 != nil:
		xs, err := qdb.GetColumnDataInt64(cd)
		if err != nil || len(xs) == 0 {
			return false
		}

		return xs[0] == *m.i64
	case m.str != nil:
		xs, err := qdb.GetColumnDataString(cd)
		if err != nil || len(xs) == 0 {
			return false
		}

		return xs[0] == *m.str
	default:
		return false
	}
}
