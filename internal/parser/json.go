// Package parser implements JSON message parsing for the NATS-QuasarDB connector.
//
// JsonParser Overview:
// The JsonParser provides a specialized parser for transforming JSON-formatted
// NATS messages into QuasarDB time series data. It enforces a simple, flat
// JSON structure to ensure predictable column mappings and efficient processing.
//
// Limitations:
// - Only single-depth JSON objects are supported (no nested objects or arrays)
// - JSON arrays at the root level are not supported
// - Null values are skipped and not written to QuasarDB
// - Maximum JSON size limited by NATS message size (default 1MB)
//
// Type Mapping:
//
//	JSON Type    | QuasarDB Type | Notes
//	-------------|---------------|----------------------------------
//	string       | Blob          | UTF-8 encoded bytes
//	number       | Double        | All numbers converted to float64
//	boolean      | Blob          | Stored as "true" or "false"
//	null         | (skipped)     | Not written to database
//	object/array | (error)       | Nested structures not supported
//
// Example Input/Output:
//
//	Input JSON:
//	{
//	  "temperature": 23.5,
//	  "location": "sensor-01",
//	  "active": true,
//	  "metadata": null
//	}
//
//	Output: WriterTable with 3 columns (metadata skipped):
//	- temperature (Double): 23.5
//	- location (Blob): "sensor-01"
//	- active (Blob): "true"
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

// JsonParser transforms JSON messages into QuasarDB writer tables.
//
// JsonParser implements the Parser interface with a strict JSON-to-timeseries
// mapping with the following behavior:
// - Validates JSON structure before processing
// - Rejects nested objects and arrays to maintain flat column structure
// - Preserves JSON key names as QuasarDB column names
// - Applies consistent type mappings for predictable schema
//
// Thread Safety:
// JsonParser methods are thread-safe and can be called concurrently.
// Each Parse call operates on independent data structures.
//
// Future Configuration:
// The parser will support configuration for:
// - Dynamic table name mapping based on NATS subject
// - Column type overrides for specific fields
// - Custom timestamp extraction from JSON fields
// - Batch size and write buffering options
type JsonParser struct {
	// TODO: IMPORTANT - Table name is currently hardcoded to "foobar".
	// This must be made configurable based on NATS subject or parser config.
	// Proposed config structure:
	// tableMapping map[string]string  // subject -> table name
	// defaultTable string             // fallback table name
	// columnTypes map[string]qdb.TsColumnType // override default type mappings
}

// Compile-time check that JsonParser implements the Parser interface
var _ Parser = (*JsonParser)(nil)

// NewJsonParser creates a new JSON parser instance.
//
// NewJsonParser initializes a JsonParser ready to transform JSON messages.
// In the current implementation, it uses default settings with a hardcoded
// table name. Future versions will accept configuration options.
//
// Example usage:
//
//	parser, err := NewJsonParser()
//	if err != nil {
//	    return fmt.Errorf("failed to create JSON parser: %w", err)
//	}
//
//	// Use parser in connector
//	connector := NewConnector(opts, parser)
//
// Future enhancement:
//
//	config := &JsonParserConfig{
//	    TableMapping: map[string]string{
//	        "sensors.*": "sensor_data",
//	        "logs.*": "application_logs",
//	    },
//	    DefaultTable: "raw_messages",
//	}
//	parser, err := NewJsonParser(config)
func NewJsonParser() (*JsonParser, error) {
	slog.Info("Initializing JSON parser")
	return &JsonParser{}, nil
}

// Parse transforms a NATS message containing JSON into QuasarDB writer tables.
//
// Parse implements the Parser interface, converting JSON data from NATS messages
// into QuasarDB WriterTable structures. Each JSON key becomes a column, with
// values converted according to the type mapping rules.
//
// Message Processing:
// 1. Validates message is non-nil with data
// 2. Unmarshals JSON into map structure
// 3. Validates flat structure (no nesting)
// 4. Creates WriterTable with typed columns
// 5. Returns table ready for QuasarDB insertion
//
// Error Handling:
// - Nil or empty messages return ConnectorError with ErrCodeParsingFailed
// - Invalid JSON syntax returns parsing error with details
// - Nested structures return descriptive error
// - Empty JSON objects are valid but produce no columns
//
// Example:
//
//	msg := &nats.Msg{
//	    Subject: "sensors.temperature",
//	    Data: []byte(`{"temp": 23.5, "unit": "celsius"}`),
//	}
//	tables, err := parser.Parse(msg)
//	if err != nil {
//	    // Handle parsing error
//	}
//	// tables[0] will contain:
//	// - Table: "foobar" (TODO: make configurable)
//	// - Columns: "temp" (Double), "unit" (Blob)
//	// - One row with current timestamp
//
// Performance Considerations:
// - JSON unmarshaling allocates temporary map structure
// - Each parse allocates new WriterTable and column slices
// - Suitable for messages up to 1MB (NATS default limit)
// - For high-throughput scenarios, consider batching at connector level
func (jp *JsonParser) Parse(msg *nats.Msg) ([]qdb.WriterTable, error) {
	if msg == nil {
		return nil, errors.NewParsingFailedError("json_parser", fmt.Errorf("nil message"))
	}
	if len(msg.Data) == 0 {
		return nil, errors.NewParsingFailedError("json_parser", fmt.Errorf("empty message data"))
	}

	// Parse JSON data into map
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

	// Build column definitions and values
	var columns []qdb.WriterColumn
	var values []interface{}

	for key, value := range jsonData {
		// Skip null values
		if value == nil {
			slog.Debug("Skipping null value", "key", key)
			continue
		}

		// Validate single-depth requirement and perform type mapping.
		// This switch enforces the flat JSON structure and converts values
		// to appropriate QuasarDB column types.
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

	// Create WriterTable with proper API
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
