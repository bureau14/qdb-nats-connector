// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package integration provides integration test helpers.
package integration

import (
	"fmt"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"pgregory.net/rapid"
)

// TableSchema: test table schema
type TableSchema struct {
	Name    string
	Columns []qdb.WriterColumn
}

// ColumnDataSet: table data with timestamps
type ColumnDataSet struct {
	Schema     TableSchema
	Timestamps []time.Time
	ColumnData [][]string // string data for each column
}

// AlphanumericString generates alphanum strings.
// Internal generator for test data.
func AlphanumericString(minLen, maxLen int) *rapid.Generator[string] {
	return rapid.StringMatching(`^[a-zA-Z0-9]+$`).
		Filter(func(s string) bool {
			return len(s) >= minLen && len(s) <= maxLen
		})
}

// UniqueTableName generates unique test table names.
// Prefix + random suffix.
func UniqueTableName() *rapid.Generator[string] {
	return rapid.Custom(func(t *rapid.T) string {
		prefix := rapid.StringMatching(`^[a-zA-Z][a-zA-Z0-9_]*$`).
			Filter(func(s string) bool { return len(s) >= 3 && len(s) <= 10 }).
			Draw(t, "table_prefix")
		suffix := rapid.Uint64().Draw(t, "table_suffix")

		return fmt.Sprintf("%s_%d", prefix, suffix)
	})
}

// ColumnName generates valid column names.
// 2-20 chars, alphanum.
func ColumnName() *rapid.Generator[string] {
	return rapid.StringMatching(`^[a-zA-Z][a-zA-Z0-9_]*$`).
		Filter(func(s string) bool {
			return len(s) >= 2 && len(s) <= 20
		})
}

// TableSchemaGen generates schemas with 1-10 blob columns.
// Unique names, blob type only.
func TableSchemaGen() *rapid.Generator[TableSchema] {
	return rapid.Custom(func(t *rapid.T) TableSchema {
		tableName := UniqueTableName().Draw(t, "table_name")

		// Generate 1-10 columns
		numColumns := rapid.IntRange(1, 10).Draw(t, "num_columns")

		columns := make([]qdb.WriterColumn, numColumns)
		usedNames := make(map[string]bool)

		for i := range numColumns {
			var colName string
			// Ensure unique column names
			for {
				colName = ColumnName().Draw(t, fmt.Sprintf("column_%d_name", i))
				if !usedNames[colName] {
					usedNames[colName] = true

					break
				}
			}

			// All columns are blob type to match JSON parser output
			// JSON parser converts strings to blob type (TsColumnTypes[0])
			columns[i] = qdb.WriterColumn{
				ColumnName: colName,
				ColumnType: qdb.TsColumnTypes[0], // blob type
			}
		}

		return TableSchema{
			Name:    tableName,
			Columns: columns,
		}
	})
}

// TimestampArray generates timestamp arrays from 2024-01-01.
// Nanosecond precision, unique values.
func TimestampArray(minRows, maxRows int) *rapid.Generator[[]time.Time] {
	return rapid.Custom(func(t *rapid.T) []time.Time {
		numRows := rapid.IntRange(minRows, maxRows).Draw(t, "num_rows")

		// Start from a base time and increment
		baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		timestamps := make([]time.Time, numRows)

		for i := range numRows {
			// Add random nanoseconds to ensure unique timestamps
			nanoOffset := rapid.Int64Range(0, 1000000000).Draw(t, fmt.Sprintf("nano_offset_%d", i))
			timestamps[i] = baseTime.Add(time.Duration(int64(i)*1000000000 + nanoOffset))
		}

		return timestamps
	})
}

// StringDataArray generates string arrays.
// Alphanum, 1-50 chars.
func StringDataArray(numRows int) *rapid.Generator[[]string] {
	return rapid.Custom(func(t *rapid.T) []string {
		data := make([]string, numRows)
		for i := range numRows {
			data[i] = AlphanumericString(1, 50).Draw(t, fmt.Sprintf("string_data_%d", i))
		}

		return data
	})
}

// ColumnDataSetGen generates full datasets with 1-1000 rows.
// Schema + timestamps + column data.
func ColumnDataSetGen() *rapid.Generator[ColumnDataSet] {
	return rapid.Custom(func(t *rapid.T) ColumnDataSet {
		schema := TableSchemaGen().Draw(t, "schema")

		// Generate 1-1000 rows
		numRows := rapid.IntRange(1, 1000).Draw(t, "num_rows")

		timestamps := TimestampArray(numRows, numRows).Draw(t, "timestamps")

		// Generate string data for each column
		columnData := make([][]string, len(schema.Columns))
		for i := range schema.Columns {
			columnData[i] = StringDataArray(numRows).Draw(t, fmt.Sprintf("column_data_%d", i))
		}

		return ColumnDataSet{
			Schema:     schema,
			Timestamps: timestamps,
			ColumnData: columnData,
		}
	})
}

// ToWriterTable converts to QDB writer table.
// Sets timestamps and column data.
func (cds *ColumnDataSet) ToWriterTable() (qdb.WriterTable, error) {
	table, err := qdb.NewWriterTable(cds.Schema.Name, cds.Schema.Columns)
	if err != nil {
		return table, fmt.Errorf("failed to create writer table: %w", err)
	}

	// Set timestamps
	table.SetIndex(cds.Timestamps)

	// Set column data
	for i, colData := range cds.ColumnData {
		columnData := qdb.NewColumnDataString(colData)
		err := table.SetData(i, &columnData)
		if err != nil {
			return table, fmt.Errorf("failed to set data for column %d: %w", i, err)
		}
	}

	return table, nil
}

// NumRows returns row count.
func (cds *ColumnDataSet) NumRows() int {
	return len(cds.Timestamps)
}

// NumColumns returns column count.
func (cds *ColumnDataSet) NumColumns() int {
	return len(cds.Schema.Columns)
}
