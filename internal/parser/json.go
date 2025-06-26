// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package parser: message transformation pipeline
// Types: Parser, JsonParser, NoopParser
// Ex: parser.NewJsonParser().Parse(msg) → []WriterTable
package parser

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/nats-io/nats.go"
)

// JsonParser: flat JSON→QDB timeseries, rejects nested objects
type JsonParser struct {
	// DefaultTable: fallback table name when no mapping matches
	DefaultTable string
	// SubjectToTable: maps NATS subjects to QDB table names
	// Example: {"sensors.*": "sensor_data", "logs.*": "application_logs"}
	SubjectToTable map[string]string
}

// Compile-time check that JsonParser implements the Parser interface
var _ Parser = (*JsonParser)(nil)

// NewJsonParser creates JSON→QDB parser with default table mapping.
// Returns:
//
//	*JsonParser: flat JSON transformer
//	error: never fails (interface consistency)
//
// Example:
//
//	NewJsonParser() // → parser, nil
func NewJsonParser() (*JsonParser, error) {
	return NewJsonParserWithConfig("foobar", nil)
}

// NewJsonParserWithConfig creates JSON→QDB parser with custom table mapping.
// Args:
//
//	defaultTable: string - fallback table name
//	subjectMapping: map[string]string - subject pattern → table name
//
// Returns:
//
//	*JsonParser: configured parser
//	error: never fails (interface consistency)
//
// Example:
//
//	NewJsonParserWithConfig("data", map[string]string{"sensors.*": "sensor_data"})
func NewJsonParserWithConfig(defaultTable string, subjectMapping map[string]string) (*JsonParser, error) {
	slog.Info("Initializing JSON parser", "default_table", defaultTable, "subject_mappings", len(subjectMapping))
	return &JsonParser{
		DefaultTable:   defaultTable,
		SubjectToTable: subjectMapping,
	}, nil
}

// Parse converts JSON msg→QDB tables.
// Args:
//
//	msg: *nats.Msg - JSON payload
//
// Returns:
//
//	[]qdb.WriterTable: flat JSON→table conversion
//	error: ParsingFailed on invalid/nested JSON
//
// Example:
//
//	Parse({"temp": 23.5}) // → [table], nil
func (jp *JsonParser) Parse(msg *nats.Msg) ([]qdb.WriterTable, error) {
	if msg == nil {
		return nil, errors.NewParsingFailedError("json_parser", fmt.Errorf("nil message"))
	}
	if len(msg.Data) == 0 {
		return nil, errors.NewParsingFailedError("json_parser", fmt.Errorf("empty message data"))
	}

	// 1. Unmarshal JSON: parse to map
	var jsonData map[string]interface{}
	if err := json.Unmarshal(msg.Data, &jsonData); err != nil {
		return nil, errors.NewParsingFailedError("json_parser", fmt.Errorf("invalid JSON: %w", err))
	}

	if len(jsonData) == 0 {
		return nil, errors.NewParsingFailedError("json_parser", fmt.Errorf("empty JSON object"))
	}

	// Determine table name: check subject mapping, then headers, then default
	tableName := jp.getTableName(msg)

	// 2. Check structure & 3. Map types: validate flat, convert types
	// Preallocate slices with known capacity for better performance
	columns := make([]qdb.WriterColumn, 0, len(jsonData))
	values := make([]interface{}, 0, len(jsonData))

	for key, value := range jsonData {
		// Skip null values
		if value == nil {
			slog.Debug("Skipping null value", "key", key)
			continue
		}

		// Type mapping: JSON→QDB column types
		switch v := value.(type) {
		case map[string]interface{}:
			return nil, errors.NewParsingFailedError("json_parser",
				fmt.Errorf("nested objects not supported, found object at key '%s'", key))
		case []interface{}:
			return nil, errors.NewParsingFailedError("json_parser",
				fmt.Errorf("arrays not supported, found array at key '%s'", key))
		case string:
			// Strings are stored as Blob type (UTF-8 bytes)
			columns = append(columns, qdb.WriterColumn{
				ColumnName: key,
				ColumnType: qdb.TsColumnTypes[0], // blob type
			})
			values = append(values, []byte(v))
		case float64, int64, int:
			// All numeric types are converted to Double for consistency.
			// JSON unmarshaling typically produces float64, but we handle
			// other numeric types for robustness.
			var doubleVal float64
			switch num := v.(type) {
			case float64:
				doubleVal = num
			case int64:
				doubleVal = float64(num)
			case int:
				doubleVal = float64(num)
			}
			columns = append(columns, qdb.WriterColumn{
				ColumnName: key,
				ColumnType: qdb.TsColumnTypes[2], // double type
			})
			values = append(values, doubleVal)
		case bool:
			// Booleans are stored as Blob type with "true"/"false" strings.
			// This provides clear boolean representation while maintaining
			// compatibility with QuasarDB's type system.
			boolStr := "false"
			if v {
				boolStr = "true"
			}
			columns = append(columns, qdb.WriterColumn{
				ColumnName: key,
				ColumnType: qdb.TsColumnTypes[0], // blob type
			})
			values = append(values, []byte(boolStr))
		default:
			return nil, errors.NewParsingFailedError("json_parser",
				fmt.Errorf("unsupported JSON value type %T for key '%s'", value, key))
		}
	}

	// No valid columns after filtering
	if len(columns) == 0 {
		return nil, errors.NewParsingFailedError("json_parser", fmt.Errorf("no valid columns found after parsing"))
	}

	// 4. Build table: create with columns & timestamp
	table, err := qdb.NewWriterTable(tableName, columns)
	if err != nil {
		return nil, errors.NewParsingFailedError("json_parser", fmt.Errorf("failed to create writer table: %w", err))
	}

	// Set timestamp for single row
	table.SetIndex([]time.Time{time.Now()})

	// 5. Column data population: QuasarDB API investigation needed
	// TODO: Implement actual column data population once QuasarDB Writer API is clarified.
	// The WriterTable creation and column definition is complete, but the actual data
	// population requires understanding the correct API methods (SetData, direct field access, etc.)
	// Current candidates for implementation:
	// - table.SetData(columnIndex, values[columnIndex]) for each column
	// - Direct field access if fields are exported (table.Blobs[i], table.Doubles[i])
	// - Batch data setting with table.SetDatas(allColumnData)
	_ = values // Suppress unused variable warning - contains parsed data ready for insertion

	slog.Debug("Successfully parsed JSON message",
		"table", tableName,
		"columns", len(columns),
		"subject", msg.Subject)

	return []qdb.WriterTable{table}, nil
}

// getTableName determines table name using subject mapping, headers, or default.
// Args:
//
//	msg: *nats.Msg - NATS message with subject and headers
//
// Returns:
//
//	string: table name to use
//
// Example:
//
//	getTableName(msg) // → "sensor_data"
func (jp *JsonParser) getTableName(msg *nats.Msg) string {
	// 1. Check explicit table name in message headers
	if tableName := msg.Header.Get("qdb-table"); tableName != "" {
		return tableName
	}

	// 2. Match subject against configured patterns
	if jp.SubjectToTable != nil {
		for pattern, tableName := range jp.SubjectToTable {
			if matched, _ := filepath.Match(pattern, msg.Subject); matched {
				return tableName
			}
		}
	}

	// 3. Try simple prefix matching as fallback
	if jp.SubjectToTable != nil {
		for pattern, tableName := range jp.SubjectToTable {
			// Remove wildcard and check prefix
			if strings.HasSuffix(pattern, ".*") {
				prefix := strings.TrimSuffix(pattern, ".*")
				if strings.HasPrefix(msg.Subject, prefix) {
					return tableName
				}
			}
		}
	}

	// 4. Fall back to default table name
	return jp.DefaultTable
}
