// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package parser: message transformation pipeline
// Types: Parser, JsonParser, NoopParser
// Ex: parser.NewJsonParser().Parse(msg) → []WriterTable
package parser

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/nats-io/nats.go"
)

// JsonParser: flat JSON→QDB timeseries, requires $table key in JSON data
type JsonParser struct {
	// DefaultTable: deprecated, no longer used (table name comes from $table key)
	DefaultTable string
	// SubjectToTable: deprecated, no longer used (table name comes from $table key)
	SubjectToTable map[string]string
}

// Compile-time check that JsonParser implements the Parser interface
var _ Parser = (*JsonParser)(nil)

// NewJsonParser creates JSON→QDB parser.
// Table names are now extracted from the required $table key in JSON data.
// Returns:
//
//	*JsonParser: flat JSON transformer
//	error: never fails (interface consistency)
//
// Example:
//
//	NewJsonParser() // → parser, nil
func NewJsonParser() (*JsonParser, error) {
	return NewJsonParserWithConfig("", nil)
}

// NewJsonParserWithConfig creates JSON→QDB parser.
// Args:
//
//	defaultTable: string - deprecated, no longer used
//	subjectMapping: map[string]string - deprecated, no longer used
//
// Returns:
//
//	*JsonParser: configured parser
//	error: never fails (interface consistency)
//
// Note: Table names are now extracted from the required $table key in JSON data.
//
// Example:
//
//	NewJsonParserWithConfig("", nil)
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
//	msg: *nats.Msg - JSON payload with required $table key
//
// Returns:
//
//	[]qdb.WriterTable: flat JSON→table conversion
//	error: ParsingFailed on invalid/nested JSON or missing $table
//
// Example:
//
//	Parse({"$table": "sensors", "temp": 23.5}) // → [table], nil
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

	// Extract and validate required $table key
	tableInterface, exists := jsonData["$table"]
	if !exists {
		return nil, errors.NewParsingFailedError("json_parser", fmt.Errorf("missing required $table key in JSON data"))
	}

	tableName, ok := tableInterface.(string)
	if !ok || tableName == "" {
		return nil, errors.NewParsingFailedError("json_parser", fmt.Errorf("$table must be a non-empty string, got %T", tableInterface))
	}

	// Remove $table from data before processing columns
	delete(jsonData, "$table")

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
				ColumnType: qdb.TsColumnTypes[1], // double type
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

	// 5. Column data population: populate table with parsed values
	for i, value := range values {
		switch columns[i].ColumnType {
		case qdb.TsColumnTypes[0]: // blob type
			if blobData, ok := value.([]byte); ok {
				columnData := qdb.NewColumnDataBlob([][]byte{blobData})
				if err := table.SetData(i, &columnData); err != nil {
					return nil, errors.NewParsingFailedError("json_parser", fmt.Errorf("failed to set blob data for column %d: %w", i, err))
				}
			}
		case qdb.TsColumnTypes[1]: // double type
			if doubleData, ok := value.(float64); ok {
				columnData := qdb.NewColumnDataDouble([]float64{doubleData})
				if err := table.SetData(i, &columnData); err != nil {
					return nil, errors.NewParsingFailedError("json_parser", fmt.Errorf("failed to set double data for column %d: %w", i, err))
				}
			}
		default:
			return nil, errors.NewParsingFailedError("json_parser", fmt.Errorf("unsupported column type for column %d", i))
		}
	}

	slog.Debug("Successfully parsed JSON message",
		"table", tableName,
		"columns", len(columns),
		"subject", msg.Subject)

	return []qdb.WriterTable{table}, nil
}

