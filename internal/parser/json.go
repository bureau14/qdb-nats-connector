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

// JsonParser: flat JSON→QDB timeseries, rejects nested objects
type JsonParser struct {
}

// Compile-time check that JsonParser implements the Parser interface
var _ Parser = (*JsonParser)(nil)

// NewJsonParser creates JSON→QDB parser.
// Returns:
//
//	*JsonParser: flat JSON transformer
//	error: never fails (interface consistency)
//
// Example:
//
//	NewJsonParser() // → parser, nil
func NewJsonParser() (*JsonParser, error) {
	slog.Info("Initializing JSON parser")
	return &JsonParser{}, nil
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

	// TODO: CRITICAL - Table name is hardcoded to "foobar" for initial implementation.
	// This MUST be made configurable before production use.
	// Proposed implementation:
	// 1. Add tableMapping configuration to JsonParser struct
	// 2. Match msg.Subject against configured patterns (e.g., "sensors.*" -> "sensor_data")
	// 3. Fall back to defaultTable if no pattern matches
	// 4. Consider allowing table name from message headers for dynamic routing
	tableName := "foobar" // HARDCODED - See TODO above

	// 2. Check structure & 3. Map types: validate flat, convert types
	var columns []qdb.WriterColumn
	var values []interface{}

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

	// TODO: Implement column data population once QuasarDB API usage is clarified.
	// Current implementation:
	// - Correctly parses JSON and creates table structure
	// - Validates data types and creates appropriate columns
	// - Sets timestamps for rows
	// Missing:
	// - Actual data population into ColumnData fields
	// - This requires understanding the complex QuasarDB Writer API
	// Note: values slice contains the parsed data ready for insertion
	_ = values // Suppress unused variable warning - will be used for data population

	slog.Debug("Successfully parsed JSON message",
		"table", tableName,
		"columns", len(columns),
		"subject", msg.Subject)

	return []qdb.WriterTable{table}, nil
}
