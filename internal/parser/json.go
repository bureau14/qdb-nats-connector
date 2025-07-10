// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package parser: NATS→QuasarDB message transformation
// Types: Parser, JsonParser, ParseResult
// Ex: parser.Parse(msg) → []WriterTable
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

// JsonParser: JSON→QuasarDB tables, requires $table field
type JsonParser struct {
	// DefaultTable: deprecated, no longer used (table name comes from $table key)
	DefaultTable string
	// SubjectToTable: deprecated, no longer used (table name comes from $table key)
	SubjectToTable map[string]string
}

// Compile-time check that JsonParser implements the Parser interface
var _ Parser = (*JsonParser)(nil)

// NewJsonParser creates JSON message parser.
// In: none
// Out: *JsonParser, error - parser, always nil
// Ex: NewJsonParser() → &JsonParser{}, nil
func NewJsonParser() (*JsonParser, error) {
	return NewJsonParserWithConfig("", nil)
}

// Deprecated: Use NewJsonParser() instead. Will be removed in v2.0.0 (2025-08-01)
// NewJsonParserWithConfig creates parser with legacy config.
// In: defaultTable string, subjectMapping map - both ignored
// Out: *JsonParser, error - parser, always nil
// Ex: NewJsonParserWithConfig("", nil) → &JsonParser{}, nil
func NewJsonParserWithConfig(defaultTable string, subjectMapping map[string]string) (*JsonParser, error) {
	slog.Info("Initializing JSON parser", "default_table", defaultTable, "subject_mappings", len(subjectMapping))

	return &JsonParser{
		DefaultTable:   defaultTable,
		SubjectToTable: subjectMapping,
	}, nil
}

// Parse transforms JSON to QDB table, requires $table.
// In: msg *nats.Msg - JSON payload
// Out: []WriterTable, error - single table or parse error
// Ex: Parse(msg) → [{sensors table}], nil
func (jp *JsonParser) Parse(msg *nats.Msg) ([]qdb.WriterTable, error) {
	if msg == nil {
		return nil, errors.NewParsingFailedError("json_parser", fmt.Errorf("nil message"))
	}
	if len(msg.Data) == 0 {
		return nil, errors.NewParsingFailedError("json_parser", fmt.Errorf("empty message data"))
	}

	jsonData, err := jp.parseJSONData(msg)
	if err != nil {
		return nil, err
	}

	tableName, timestamp, err := jp.extractTableAndTimestamp(jsonData)
	if err != nil {
		return nil, err
	}

	columns, values, err := jp.convertJSONToColumns(jsonData)
	if err != nil {
		return nil, err
	}

	table, err := jp.createWriterTable(tableName, columns, timestamp, values)
	if err != nil {
		return nil, err
	}

	slog.Debug("Successfully parsed JSON message",
		"table", tableName,
		"columns", len(columns),
		"subject", msg.Subject,
		"timestamp", timestamp.Format(time.RFC3339Nano))

	return []qdb.WriterTable{table}, nil
}

// ParseBatch delegates to DefaultParseBatch for independent parsing.
// In: msgs []*nats.Msg - message batch
// Out: []ParseResult - results per message
// Ex: ParseBatch(msgs) → [{Tables,nil},{nil,err}]
func (jp *JsonParser) ParseBatch(msgs []*nats.Msg) []ParseResult {
	return DefaultParseBatch(jp, msgs)
}

// parseJSONData validates raw bytes as JSON to catch malformed data early.
// Separated from Parse() to isolate JSON syntax errors from semantic validation.
// In: msg.Data []byte - raw NATS payload
// Out: map[string]any - parsed object, err if invalid JSON
// Ex: parseJSONData(msg) → {"$table":"sensors","temp":23.5}
func (jp *JsonParser) parseJSONData(msg *nats.Msg) (map[string]interface{}, error) {
	slog.Debug("JSON parser received message", "subject", msg.Subject, "data_len", len(msg.Data), "data", string(msg.Data))

	var jsonData map[string]interface{}
	err := json.Unmarshal(msg.Data, &jsonData)
	if err != nil {
		return nil, errors.NewParsingFailedError("json_parser", fmt.Errorf("invalid JSON: %w", err))
	}

	if len(jsonData) == 0 {
		return nil, errors.NewParsingFailedError("json_parser", fmt.Errorf("empty JSON object"))
	}

	slog.Debug("JSON parsed successfully", "keys", len(jsonData), "data", jsonData)

	return jsonData, nil
}

// extractTableAndTimestamp isolates metadata extraction for cleaner column processing.
// Removes $table/$timestamp from jsonData to prevent them becoming columns.
// In: jsonData map with required $table field
// Out: tableName string, timestamp time.Time, error if missing $table
// Ex: extract({"$table":"sensors"}) → "sensors",time.Now(),nil
func (jp *JsonParser) extractTableAndTimestamp(jsonData map[string]interface{}) (string, time.Time, error) {
	tableInterface, exists := jsonData["$table"]
	if !exists {
		return "", time.Time{}, errors.NewParsingFailedError("json_parser", fmt.Errorf("missing required $table key in JSON data"))
	}

	tableName, ok := tableInterface.(string)
	if !ok || tableName == "" {
		return "", time.Time{}, errors.NewParsingFailedError("json_parser", fmt.Errorf("$table must be a non-empty string, got %T", tableInterface))
	}

	timestamp := parseTimestamp(jsonData)
	delete(jsonData, "$timestamp")
	delete(jsonData, "$table")

	return tableName, timestamp, nil
}

// convertJSONToColumns transforms remaining fields into QuasarDB schema.
// Separated to enable future column filtering/transformation features.
// In: jsonData map without $table/$timestamp fields
// Out: []WriterColumn schema, []any values, error if unsupported types
// Ex: {"temp":23.5} → [WriterColumn{"temp",TsColumnDouble}],[23.5]
func (jp *JsonParser) convertJSONToColumns(jsonData map[string]interface{}) ([]qdb.WriterColumn, []interface{}, error) {
	columns := make([]qdb.WriterColumn, 0, len(jsonData))
	values := make([]interface{}, 0, len(jsonData))

	for key, value := range jsonData {
		if value == nil {
			slog.Debug("Skipping null value", "key", key)

			continue
		}

		column, val, err := jp.convertJSONValue(key, value)
		if err != nil {
			return nil, nil, err
		}

		columns = append(columns, column)
		values = append(values, val)
	}

	if len(columns) == 0 {
		return nil, nil, errors.NewParsingFailedError("json_parser", fmt.Errorf("no valid columns found after parsing"))
	}

	return columns, values, nil
}

// convertJSONValue maps JSON types to QuasarDB column types per field.
// Centralized type conversion logic for consistent handling across parsers.
// In: key string, value any - single JSON field
// Out: WriterColumn definition, converted value, error if unsupported
// Ex: ("temp",23.5) → WriterColumn{"temp",TsColumnDouble},23.5
func (jp *JsonParser) convertJSONValue(key string, value interface{}) (qdb.WriterColumn, interface{}, error) {
	switch v := value.(type) {
	case map[string]interface{}:
		return qdb.WriterColumn{}, nil, errors.NewParsingFailedError("json_parser",
			fmt.Errorf("nested objects not supported, found object at key '%s'", key))
	case []interface{}:
		return qdb.WriterColumn{}, nil, errors.NewParsingFailedError("json_parser",
			fmt.Errorf("arrays not supported, found array at key '%s'", key))
	case string:
		return qdb.WriterColumn{
			ColumnName: key,
			ColumnType: qdb.TsColumnBlob,
		}, []byte(v), nil
	case float64, int64, int:
		doubleVal := jp.convertToDouble(v)

		return qdb.WriterColumn{
			ColumnName: key,
			ColumnType: qdb.TsColumnDouble,
		}, doubleVal, nil
	case bool:
		boolStr := "false"
		if v {
			boolStr = "true"
		}

		return qdb.WriterColumn{
			ColumnName: key,
			ColumnType: qdb.TsColumnBlob,
		}, []byte(boolStr), nil
	default:
		return qdb.WriterColumn{}, nil, errors.NewParsingFailedError("json_parser",
			fmt.Errorf("unsupported JSON value type %T for key '%s'", value, key))
	}
}

// convertToDouble ensures numeric consistency for QuasarDB double columns.
// JSON numbers arrive as various types; normalize to float64.
// In: value any - numeric type (float64/int64/int)
// Out: float64 - normalized value, 0 if non-numeric
// Ex: int64(42) → 42.0
func (jp *JsonParser) convertToDouble(value interface{}) float64 {
	switch num := value.(type) {
	case float64:
		return num
	case int64:
		return float64(num)
	case int:
		return float64(num)
	default:
		return 0
	}
}

// createWriterTable constructs final QuasarDB table with validated data.
// Separated from Parse() to enable future multi-table support per message.
// In: tableName, columns schema, timestamp, values array
// Out: WriterTable ready for batch writing, error if construction fails
// Ex: ("sensors",[TempCol],[now],[23.5]) → WriterTable{1 row}
func (jp *JsonParser) createWriterTable(tableName string, columns []qdb.WriterColumn, timestamp time.Time, values []interface{}) (qdb.WriterTable, error) {
	table, err := qdb.NewWriterTable(tableName, columns)
	if err != nil {
		return qdb.WriterTable{}, errors.NewParsingFailedError("json_parser", fmt.Errorf("failed to create writer table: %w", err))
	}

	table.SetIndex([]time.Time{timestamp})

	err = jp.populateTableData(table, columns, values)
	if err != nil {
		return qdb.WriterTable{}, err
	}

	return table, nil
}

// populateTableData applies type-specific data setters for each column.
// Separated to isolate QuasarDB API calls and enable column-level error handling.
// In: table WriterTable, columns []WriterColumn, values []any
// Out: error if any SetData operation fails
// Ex: populate(table,[BlobCol],[[]byte{"data"}]) → nil
func (jp *JsonParser) populateTableData(table qdb.WriterTable, columns []qdb.WriterColumn, values []interface{}) error {
	for i, value := range values {
		switch columns[i].ColumnType {
		case qdb.TsColumnBlob:
			if blobData, ok := value.([]byte); ok {
				columnData := qdb.NewColumnDataBlob([][]byte{blobData})
				err := table.SetData(i, &columnData)
				if err != nil {
					return errors.NewParsingFailedError("json_parser", fmt.Errorf("failed to set blob data for column %d: %w", i, err))
				}
				slog.Debug("Set blob data", "column", columns[i].ColumnName, "value", string(blobData))
			}
		case qdb.TsColumnDouble:
			if doubleData, ok := value.(float64); ok {
				columnData := qdb.NewColumnDataDouble([]float64{doubleData})
				err := table.SetData(i, &columnData)
				if err != nil {
					return errors.NewParsingFailedError("json_parser", fmt.Errorf("failed to set double data for column %d: %w", i, err))
				}
				slog.Debug("Set double data", "column", columns[i].ColumnName, "value", doubleData)
			}
		case qdb.TsColumnUninitialized, qdb.TsColumnInt64, qdb.TsColumnString, qdb.TsColumnTimestamp, qdb.TsColumnSymbol:
			return jp.handleUnsupportedColumnType(columns[i], i)
		default:
			return jp.handleUnsupportedColumnType(columns[i], i)
		}
	}

	return nil
}

// handleUnsupportedColumnType provides detailed errors for type limitations.
// Centralized error messages help users understand parser constraints.
// In: column WriterColumn, index int - problematic column info
// Out: ConnectorError with specific reason per column type
// Ex: (WriterColumn{Type:TsColumnInt64},0) → "int64 not supported by JSON parser"
func (jp *JsonParser) handleUnsupportedColumnType(column qdb.WriterColumn, index int) error {
	switch column.ColumnType {
	case qdb.TsColumnUninitialized:
		return errors.NewParsingFailedError("json_parser", fmt.Errorf("uninitialized column type for column %d (%s)", index, column.ColumnName))
	case qdb.TsColumnInt64:
		return errors.NewParsingFailedError("json_parser", fmt.Errorf("int64 column type not supported by JSON parser for column %d (%s)", index, column.ColumnName))
	case qdb.TsColumnString:
		return errors.NewParsingFailedError("json_parser", fmt.Errorf("string column type not supported by JSON parser for column %d (%s)", index, column.ColumnName))
	case qdb.TsColumnTimestamp:
		return errors.NewParsingFailedError("json_parser", fmt.Errorf("timestamp column type not supported by JSON parser for column %d (%s)", index, column.ColumnName))
	case qdb.TsColumnSymbol:
		return errors.NewParsingFailedError("json_parser", fmt.Errorf("symbol column type not supported by JSON parser for column %d (%s)", index, column.ColumnName))
	case qdb.TsColumnBlob:
		return errors.NewParsingFailedError("json_parser", fmt.Errorf("blob column type should be handled elsewhere for column %d (%s)", index, column.ColumnName))
	case qdb.TsColumnDouble:
		return errors.NewParsingFailedError("json_parser", fmt.Errorf("double column type should be handled elsewhere for column %d (%s)", index, column.ColumnName))
	default:
		return errors.NewParsingFailedError("json_parser", fmt.Errorf("unknown column type %v for column %d (%s)", column.ColumnType, index, column.ColumnName))
	}
}

// parseTimestamp extracts $timestamp or returns now
// In: data map[string]any - JSON object
// Out: time.Time - parsed or current time
// Ex: parseTimestamp({"$timestamp":"2024-01-01T12:00:00Z"}) → time
func parseTimestamp(data map[string]interface{}) time.Time {
	timestampInterface, exists := data["$timestamp"]
	if !exists {
		slog.Debug("No $timestamp provided, using current time")

		return time.Now()
	}

	timestampStr, ok := timestampInterface.(string)
	if !ok || timestampStr == "" {
		slog.Debug("$timestamp must be a non-empty string, using current time",
			"type", fmt.Sprintf("%T", timestampInterface))

		return time.Now()
	}

	parsedTime, err := time.Parse("2006-01-02T15:04:05.999999999Z", timestampStr)
	if err != nil {
		slog.Debug("Invalid $timestamp format, using current time",
			"timestamp", timestampStr, "error", err)

		return time.Now()
	}

	slog.Debug("Parsed custom timestamp", "timestamp", parsedTime.Format(time.RFC3339Nano))

	return parsedTime
}
