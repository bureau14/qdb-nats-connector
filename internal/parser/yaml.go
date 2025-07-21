// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package parser: YAML-based message transformation pipelines
// Types: YAMLParser, YAMLConfig, ParseState
// Ex: NewYAMLParser(opts).Parse(msg) → []WriterTable
package parser

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/nats-io/nats.go"
	"gopkg.in/yaml.v3"
)

// ParseState: Mutable state through pipeline
// Message: original NATS msg (read-only)
// Fields: accumulated typed values
// Per ADR-005: modified in-place for performance
type ParseState struct {
	Message   *nats.Msg              // Original message (read-only)
	Data      []byte                 // Current data representation
	Fields    map[string]interface{} // Accumulated typed values
	Errors    []error                // Non-fatal errors with context
	TableName string                 // From configuration

	// For performance: pre-allocated buffers - reused across messages, goroutine-safe
	tmpBuf        []byte      // Reusable byte buffer
	indexBuf      []time.Time // Goroutine-safe index buffer
	doubleVals    []float64   // [col_idx]float64 - goroutine-safe
	blobVals      [][]byte    // [col_idx][]byte - goroutine-safe
	int64Vals     []int64     // [col_idx]int64 - goroutine-safe
	stringVals    []string    // [col_idx]string - goroutine-safe
	timestampVals []time.Time // [col_idx]time.Time - goroutine-safe
}

// TransformationStep: Transform function modifying state
// Per ADR-005: pure functions, no I/O operations
type TransformationStep func(*ParseState) error

// stepRegistry maps names to step factories.
// Per ADR-005: pre-compiled steps for <5% overhead
var stepRegistry = map[string]func(map[string]interface{}) (TransformationStep, error){
	"decompress":        makeDecompressStep,
	"parse_json":        makeParseJSONStep,
	"extract_index":     makeExtractIndexStep,
	"extract_field":     makeExtractFieldStep,
	"compute_field":     makeComputeFieldStep,
	"safe_parse_number": makeSafeParseNumberStep,
}

// YAMLConfig: YAML parser configuration
// Per ADR-005: declarative transformation step pipeline
type YAMLConfig struct {
	Compression     string          `yaml:"compression"`
	Output          OutputSchema    `yaml:"output"`
	Transformations []TransformSpec `yaml:"transformations"`
}

// OutputSchema: QuasarDB table schema
type OutputSchema struct {
	TableName string         `yaml:"table_name"`
	Columns   []ColumnSchema `yaml:"columns"`
}

// ColumnSchema: column name+type
type ColumnSchema struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"` // timestamp, double, int64, blob, string
}

// TransformSpec: transformation step spec
// Supports both "step" and "block" for backward compatibility
type TransformSpec struct {
	Step   string                 `yaml:"step"`
	Block  string                 `yaml:"block"`
	Config map[string]interface{} `yaml:"config"`
}

// GetStepName returns the step name, checking both "step" and "block" fields
// for backward compatibility
func (t *TransformSpec) GetStepName() string {
	if t.Step != "" {
		return t.Step
	}

	return t.Block
}

// YAMLParser: Pipeline parser, goroutine-safe
// Per ADR-005: <5% overhead vs hardcoded
type YAMLParser struct {
	config      YAMLConfig
	pipeline    []TransformationStep
	errorAction string

	// Column mapping - pre-computed at startup
	columnTypes []qdb.TsColumnType
	columns     []qdb.WriterColumn
}

// Compile-time check that YAMLParser implements the Parser interface
var _ Parser = (*YAMLParser)(nil)

// NewYAMLParser creates YAML parser from config file.
// Args:
//
//	configPath: YAML config file path
//
// Returns:
//
//	*YAMLParser: compiled pipeline parser
//	error: config load/validation failure
//
// Example:
//
//	p := NewYAMLParser("sensor_parser.yaml") // → parser with transformation steps
//
// Per ADR-005: compiles YAML to optimized execution plan at startup
func NewYAMLParser(configPath string) (*YAMLParser, error) {
	config, err := loadYAMLConfig(configPath)
	if err != nil {
		return nil, err
	}

	// Create default options for direct config loading
	opts := ParserOptions{
		ErrorAction: "drop",
	}

	return NewYAMLParserFromConfig(config, opts)
}

// NewYAMLParserFromConfig creates parser from config struct.
// Args:
//
//	config: validated YAML configuration
//
// Returns:
//
//	*YAMLParser: compiled pipeline parser
//	error: validation or compilation failure
//
// Example:
//
//	p := NewYAMLParserFromConfig(cfg) // → parser ready for messages
//
// Per ADR-005: validates schema and compiles pipeline at initialization
func NewYAMLParserFromConfig(config YAMLConfig, opts ParserOptions) (*YAMLParser, error) {
	slog.Info("Initializing YAML parser", "transformations", len(config.Transformations))

	// Validate schema
	err := validateSchema(config.Output)
	if err != nil {
		return nil, err
	}

	// Compile pipeline
	pipeline, err := compilePipeline(config.Transformations)
	if err != nil {
		return nil, err
	}

	// Create column mapping
	columns, columnTypes, err := createColumnMapping(config.Output.Columns)
	if err != nil {
		return nil, err
	}

	// Validate column synchronization to prevent buffer overflow issues
	err = validateColumnSynchronization(config.Output.Columns, columns, columnTypes)
	if err != nil {
		return nil, err
	}

	parser := &YAMLParser{
		config:      config,
		pipeline:    pipeline,
		errorAction: opts.ErrorAction,
		columns:     columns,
		columnTypes: columnTypes,
	}

	// Store column information for ParseState buffer initialization
	parser.columnTypes = columnTypes
	parser.columns = columns

	// Remove shared state - now allocated per Parse() call for memory safety

	return parser, nil
}

// Parse transforms single NATS message to QuasarDB tables.
// In: msg *nats.Msg - NATS message
// Out: []qdb.WriterTable, error - tables or parse error
// Ex: Parse(msg) → [{sensors table}], nil
func (p *YAMLParser) Parse(msg *nats.Msg) ([]qdb.WriterTable, error) {
	if msg == nil {
		return nil, errors.NewParsingFailedError("yaml_parser", fmt.Errorf("nil message"))
	}
	if len(msg.Data) == 0 {
		return nil, errors.NewParsingFailedError("yaml_parser", fmt.Errorf("empty message data"))
	}

	// Process YAML message through transformation pipeline

	// Create new ParseState per call to prevent memory corruption
	state := p.newParseState()

	// Initialize state with message data and configured table name
	state.Message = msg
	state.Data = msg.Data
	state.TableName = p.config.Output.TableName

	// Execute pipeline - runs each transformation step in sequence.
	// Per ADR-005: direct function calls, no reflection
	for _, step := range p.pipeline {
		err := step(state)
		if err != nil {
			if p.errorAction == "fail" {
				return nil, errors.NewParsingFailedError("yaml_parser", err)
			}
			// For "drop" mode, log error and continue - allows partial
			// data extraction when some fields fail (ADR-005)
			slog.Warn("Pipeline step failed", "error", err, "subject", msg.Subject)
			state.Errors = append(state.Errors, err)
		}
	}

	// Check if parsing failed - in "fail" mode any error stops processing
	if len(state.Errors) > 0 && p.errorAction == "fail" {
		return nil, errors.NewParsingFailedError("yaml_parser", state.Errors[0])
	}

	// Create WriterTable - uses per-call allocated buffers for memory safety
	table, err := p.createWriterTable(state)
	if err != nil {
		return nil, err
	}

	return []qdb.WriterTable{table}, nil
}

// createWriterTable builds QDB table from state.
// In: state *ParseState - parsed fields
// Out: qdb.WriterTable - single-row table
// Ex: createWriterTable(state) → table
func (p *YAMLParser) createWriterTable(state *ParseState) (qdb.WriterTable, error) {
	// Set index - uses parsed index or current time as fallback
	var ts time.Time
	if parsedTs, ok := state.Fields["$timestamp"].(time.Time); ok {
		ts = parsedTs
	} else {
		ts = time.Now()
	}
	// Use goroutine-safe index buffer.
	if len(state.indexBuf) == 0 {
		slog.Error("CRITICAL: indexBuf is empty!")
		return qdb.WriterTable{}, errors.NewParsingFailedError("yaml_parser",
			fmt.Errorf("indexBuf is empty"))
	}
	state.indexBuf[0] = ts

	// Create table with pre-defined schema - columns were validated at startup
	table, err := qdb.NewWriterTable(state.TableName, p.columns)
	if err != nil {
		return qdb.WriterTable{}, errors.NewParsingFailedError("yaml_parser", err)
	}

	table.SetIndex(state.indexBuf)

	// Map fields to columns using pre-allocated buffers - thread-safe and efficient
	for i, col := range p.columns {
		value, exists := state.Fields[col.ColumnName]
		if !exists {
			continue
		}

		switch p.columnTypes[i] {
		case qdb.TsColumnDouble:
			if v, ok := value.(float64); ok {
				// Check buffer bounds before accessing
				if i >= len(state.doubleVals) {
					slog.Error("Buffer overflow detected in double column!",
						"column_index", i,
						"buffer_len", len(state.doubleVals),
						"column_name", col.ColumnName)

					return qdb.WriterTable{}, errors.NewParsingFailedError("yaml_parser",
						fmt.Errorf("buffer overflow: column index %d >= buffer length %d", i, len(state.doubleVals)))
				}
				// Use direct value storage for performance
				state.doubleVals[i] = v
				data := qdb.NewColumnDataDouble([]float64{state.doubleVals[i]})
				err := table.SetData(i, &data)
				if err != nil {
					return qdb.WriterTable{}, errors.NewParsingFailedError("yaml_parser", err)
				}
			}
		case qdb.TsColumnBlob:
			if v, ok := value.([]byte); ok {
				// Check buffer bounds before accessing
				if i >= len(state.blobVals) {
					slog.Error("Buffer overflow detected in blob column!",
						"column_index", i,
						"buffer_len", len(state.blobVals),
						"column_name", col.ColumnName)

					return qdb.WriterTable{}, errors.NewParsingFailedError("yaml_parser",
						fmt.Errorf("buffer overflow: column index %d >= buffer length %d", i, len(state.blobVals)))
				}
				// Use direct value storage for performance
				state.blobVals[i] = v
				data := qdb.NewColumnDataBlob([][]byte{state.blobVals[i]})
				err := table.SetData(i, &data)
				if err != nil {
					return qdb.WriterTable{}, errors.NewParsingFailedError("yaml_parser", err)
				}
			}
		case qdb.TsColumnInt64:
			if v, ok := value.(int64); ok {
				// Check buffer bounds before accessing
				if i >= len(state.int64Vals) {
					slog.Error("Buffer overflow detected in int64 column!",
						"column_index", i,
						"buffer_len", len(state.int64Vals),
						"column_name", col.ColumnName)

					return qdb.WriterTable{}, errors.NewParsingFailedError("yaml_parser",
						fmt.Errorf("buffer overflow: column index %d >= buffer length %d", i, len(state.int64Vals)))
				}
				// Use direct value storage for performance
				state.int64Vals[i] = v
				data := qdb.NewColumnDataInt64([]int64{state.int64Vals[i]})
				err := table.SetData(i, &data)
				if err != nil {
					return qdb.WriterTable{}, errors.NewParsingFailedError("yaml_parser", err)
				}
			}
		case qdb.TsColumnString:
			if v, ok := value.(string); ok {
				// Check buffer bounds before accessing
				if i >= len(state.stringVals) {
					slog.Error("Buffer overflow detected in string column!",
						"column_index", i,
						"buffer_len", len(state.stringVals),
						"column_name", col.ColumnName)

					return qdb.WriterTable{}, errors.NewParsingFailedError("yaml_parser",
						fmt.Errorf("buffer overflow: column index %d >= buffer length %d", i, len(state.stringVals)))
				}
				// Use direct value storage for performance
				state.stringVals[i] = v
				data := qdb.NewColumnDataString([]string{state.stringVals[i]})
				err := table.SetData(i, &data)
				if err != nil {
					return qdb.WriterTable{}, errors.NewParsingFailedError("yaml_parser", err)
				}
			}
		case qdb.TsColumnTimestamp:
			if v, ok := value.(time.Time); ok {
				// Check buffer bounds before accessing
				if i >= len(state.timestampVals) {
					slog.Error("Buffer overflow detected in timestamp column!",
						"column_index", i,
						"buffer_len", len(state.timestampVals),
						"column_name", col.ColumnName)

					return qdb.WriterTable{}, errors.NewParsingFailedError("yaml_parser",
						fmt.Errorf("buffer overflow: column index %d >= buffer length %d", i, len(state.timestampVals)))
				}
				// Use direct value storage for performance
				state.timestampVals[i] = v
				data := qdb.NewColumnDataTimestamp([]time.Time{state.timestampVals[i]})
				err := table.SetData(i, &data)
				if err != nil {
					return qdb.WriterTable{}, errors.NewParsingFailedError("yaml_parser", err)
				}
			}
		case qdb.TsColumnUninitialized:
			// Skip uninitialized columns - should not occur with validation
			continue
		case qdb.TsColumnSymbol:
			// Symbol columns not currently supported in YAML parser
			continue
		}
	}

	return table, nil
}

// newParseState creates a new ParseState instance for each Parse() call.
// This prevents memory corruption by ensuring each message gets its own buffers.
// In: none
// Out: *ParseState - new state with properly sized buffers
// Ex: newParseState() → fresh state
func (p *YAMLParser) newParseState() *ParseState {
	columnsLen := len(p.columns)

	state := &ParseState{
		Fields:        make(map[string]interface{}, 16),
		Errors:        make([]error, 0, 4),
		tmpBuf:        make([]byte, 0, 1024),
		indexBuf:      make([]time.Time, 1),
		doubleVals:    make([]float64, columnsLen),
		blobVals:      make([][]byte, columnsLen),
		int64Vals:     make([]int64, columnsLen),
		stringVals:    make([]string, columnsLen),
		timestampVals: make([]time.Time, columnsLen),
	}

	// Initialize blob buffers - other types are already allocated with make()
	for i, colType := range p.columnTypes {
		// Safety check
		if i >= len(state.blobVals) {
			slog.Error("CRITICAL: Column index exceeds blobVals buffer!",
				"column_index", i,
				"blobVals_len", len(state.blobVals))
			continue
		}

		switch colType {
		case qdb.TsColumnBlob:
			state.blobVals[i] = make([]byte, 0, 1024) // Pre-allocate with capacity
		case qdb.TsColumnDouble, qdb.TsColumnInt64, qdb.TsColumnString, qdb.TsColumnTimestamp:
			// These are already allocated with make() above
		case qdb.TsColumnUninitialized, qdb.TsColumnSymbol:
			// No pre-allocation needed for these types
		}
	}

	return state
}

// LoadYAMLConfig reads YAML parser config.
// In: configPath string - YAML file path
// Out: YAMLConfig - parsed config
// Ex: loadYAMLConfig("parser.yaml") → config
func loadYAMLConfig(configPath string) (YAMLConfig, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return YAMLConfig{}, errors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("failed to read config file: %v", err))
	}

	var config YAMLConfig
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return YAMLConfig{}, errors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("failed to parse YAML config: %v", err))
	}

	return config, nil
}

// compilePipeline compiles YAML specs to blocks.
// In: specs []TransformSpec - ordered transforms
// Out: []BuildingBlock - executable pipeline
// Ex: compilePipeline([{parse_json},{extract_field}]) → pipeline
// Per ADR-005: pre-compiles at startup for <5% overhead
func compilePipeline(specs []TransformSpec) ([]TransformationStep, error) {
	if len(specs) == 0 {
		return nil, errors.NewInvalidConfigError("yaml_parser", "empty transformation pipeline")
	}

	steps := make([]TransformationStep, len(specs))

	for i, spec := range specs {
		// Lookup factory in registry - validates step types at compile time
		// per ADR-005 to catch configuration errors early
		stepName := spec.GetStepName()
		factory, exists := stepRegistry[stepName]
		if !exists {
			return nil, errors.NewInvalidConfigError("yaml_parser",
				fmt.Sprintf("unknown transformation step: %s", stepName))
		}

		// Create step with config - factory validates parameters and returns
		// closure over configuration for optimal runtime performance
		step, err := factory(spec.Config)
		if err != nil {
			return nil, errors.NewInvalidConfigError("yaml_parser",
				fmt.Sprintf("failed to create step '%s': %v", stepName, err))
		}

		steps[i] = step
	}

	return steps, nil
}

// validateSchema checks QuasarDB compatibility.
// In: schema OutputSchema - table schema
// Out: error if invalid schema
// Ex: validateSchema(schema) → nil
func validateSchema(schema OutputSchema) error {
	if schema.TableName == "" {
		return errors.NewInvalidConfigError("yaml_parser", "table name is required")
	}

	if len(schema.Columns) == 0 {
		return errors.NewInvalidConfigError("yaml_parser", "at least one column is required")
	}

	// Check for duplicate column names - QuasarDB requires unique names
	seen := make(map[string]bool)
	for _, col := range schema.Columns {
		if col.Name == "" {
			return errors.NewInvalidConfigError("yaml_parser", "column name cannot be empty")
		}
		if seen[col.Name] {
			return errors.NewInvalidConfigError("yaml_parser",
				fmt.Sprintf("duplicate column name: %s", col.Name))
		}
		seen[col.Name] = true

		// Validate column type - must be supported QuasarDB type
		if !isValidColumnType(col.Type) {
			return errors.NewInvalidConfigError("yaml_parser",
				fmt.Sprintf("invalid column type: %s", col.Type))
		}
	}

	return nil
}

// createColumnMapping builds QDB column definitions.
// In: columns []ColumnSchema - schema columns
// Out: []WriterColumn, []TsColumnType - QDB types
// Ex: createColumnMapping(cols) → columns, types
func createColumnMapping(columns []ColumnSchema) ([]qdb.WriterColumn, []qdb.TsColumnType, error) {
	writerColumns := make([]qdb.WriterColumn, len(columns))
	columnTypes := make([]qdb.TsColumnType, len(columns))

	for i, col := range columns {
		writerColumns[i] = qdb.WriterColumn{
			ColumnName: col.Name,
			ColumnType: stringToColumnType(col.Type),
		}
		columnTypes[i] = stringToColumnType(col.Type)
	}

	return writerColumns, columnTypes, nil
}

// validateColumnSynchronization ensures p.columns and p.config.Output.Columns are properly synchronized
// to prevent buffer overflow issues that could cause segfaults.
// In: configColumns []ColumnSchema - original schema columns
//     writerColumns []WriterColumn - internal writer columns
//     columnTypes []TsColumnType - internal column types
// Out: error - validation failure or nil if synchronized
// Ex: validateColumnSynchronization(configCols, writerCols, types) → nil
func validateColumnSynchronization(configColumns []ColumnSchema, writerColumns []qdb.WriterColumn, columnTypes []qdb.TsColumnType) error {
	// Validate length consistency - critical for preventing buffer overflows
	if len(configColumns) != len(writerColumns) {
		return errors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("column count mismatch: schema has %d columns but internal mapping has %d columns",
				len(configColumns), len(writerColumns)))
	}

	if len(configColumns) != len(columnTypes) {
		return errors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("column count mismatch: schema has %d columns but internal types has %d types",
				len(configColumns), len(columnTypes)))
	}

	if len(writerColumns) != len(columnTypes) {
		return errors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("internal column count mismatch: writer has %d columns but types has %d entries",
				len(writerColumns), len(columnTypes)))
	}

	// Validate column names and types consistency - ensures proper indexing
	for i, configCol := range configColumns {
		// Check column name consistency
		if configCol.Name != writerColumns[i].ColumnName {
			return errors.NewInvalidConfigError("yaml_parser",
				fmt.Sprintf("column name mismatch at index %d: config has '%s' but internal mapping has '%s'",
					i, configCol.Name, writerColumns[i].ColumnName))
		}

		// Check column type consistency
		expectedType := stringToColumnType(configCol.Type)
		if expectedType != columnTypes[i] {
			return errors.NewInvalidConfigError("yaml_parser",
				fmt.Sprintf("column type mismatch at index %d for column '%s': config has type '%s' but internal has type %d",
					i, configCol.Name, configCol.Type, columnTypes[i]))
		}

		if expectedType != writerColumns[i].ColumnType {
			return errors.NewInvalidConfigError("yaml_parser",
				fmt.Sprintf("writer column type mismatch at index %d for column '%s': expected type %d but writer has type %d",
					i, configCol.Name, expectedType, writerColumns[i].ColumnType))
		}
	}

	return nil
}

// isValidColumnType validates QDB column type.
// In: colType string - type name
// Out: bool - valid for QuasarDB
// Ex: isValidColumnType("double") → true
func isValidColumnType(colType string) bool {
	switch colType {
	case "timestamp", "double", "int64", "blob", "string":
		return true
	default:
		return false
	}
}

// stringToColumnType maps string to QDB type.
// In: colType string - type name
// Out: TsColumnType - QDB enum value
// Ex: stringToColumnType("double") → TsColumnDouble
func stringToColumnType(colType string) qdb.TsColumnType {
	switch colType {
	case "timestamp":
		return qdb.TsColumnTimestamp
	case "double":
		return qdb.TsColumnDouble
	case "int64":
		return qdb.TsColumnInt64
	case "blob":
		return qdb.TsColumnBlob
	case "string":
		return qdb.TsColumnString
	default:
		return qdb.TsColumnBlob // Default fallback
	}
}

// makeDecompressStep creates decompressor step.
// In: config["algorithm"] - "gzip" supported
// Out: TransformationStep - decompresses state.Data
// Ex: makeDecompressStep({"algorithm": "gzip"}) → step
// Per ADR-005: handles compressed payloads efficiently
func makeDecompressStep(config map[string]interface{}) (TransformationStep, error) {
	algorithm, ok := config["algorithm"].(string)
	if !ok {
		return nil, errors.NewInvalidConfigError("yaml_parser", "decompress step requires 'algorithm' string")
	}

	switch algorithm {
	case "gzip":
		return makeGzipDecompressStep(), nil
	default:
		return nil, errors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("unsupported compression algorithm: %s (supported: gzip)", algorithm))
	}
}

// makeGzipDecompressStep creates gzip decompressor.
// In: none - no config needed
// Out: TransformationStep - modifies state.Data
// Ex: makeGzipDecompressStep() → decompressor
func makeGzipDecompressStep() TransformationStep {
	return func(state *ParseState) error {
		if state == nil {
			slog.Error("CRITICAL: state is nil in gzip decompress step")
			return errors.NewParsingFailedError("yaml_parser", fmt.Errorf("nil state in gzip decompress step"))
		}

		if state.Data == nil {
			slog.Error("CRITICAL: state.Data is nil in gzip decompress step")
			return errors.NewParsingFailedError("yaml_parser", fmt.Errorf("nil data in gzip decompress step"))
		}

		if len(state.Data) == 0 {
			return nil
		}

		reader, err := gzip.NewReader(bytes.NewReader(state.Data))
		if err != nil {
			slog.Error("Failed to create gzip reader", "error", err)
			return errors.NewParsingFailedError("yaml_parser", fmt.Errorf("failed to create gzip reader in decompress step: %w", err))
		}
		defer func() {
			closeErr := reader.Close()
			if closeErr != nil {
				slog.Error("Failed to close gzip reader", "error", closeErr)
				err = errors.NewParsingFailedError("yaml_parser", fmt.Errorf("failed to close gzip reader in decompress step: %w", closeErr))
			}
		}()

		decompressed, err := io.ReadAll(reader)
		if err != nil {
			slog.Error("Failed to decompress gzip data", "error", err)

			return errors.NewParsingFailedError("yaml_parser", fmt.Errorf("failed to decompress gzip data in decompress step: %w", err))
		}

		state.Data = decompressed

		return nil
	}
}

// makeParseJSONStep creates JSON parser step.
// In: config - unused, for interface consistency
// Out: TransformationStep - parses Data into Fields
// Ex: makeParseJSONStep({}) → parser
// Per ADR-005: common operation for JSON messages
func makeParseJSONStep(config map[string]interface{}) (TransformationStep, error) {
	return func(state *ParseState) error {
		if state == nil {
			return errors.NewParsingFailedError("yaml_parser", fmt.Errorf("nil state in parse_json step"))
		}
		if state.Data == nil {
			return errors.NewParsingFailedError("yaml_parser", fmt.Errorf("nil data in parse_json step"))
		}
		if len(state.Data) == 0 {
			return errors.NewParsingFailedError("yaml_parser", fmt.Errorf("empty data in parse_json step"))
		}

		var jsonData map[string]interface{}
		err := json.Unmarshal(state.Data, &jsonData)
		if err != nil {
			return errors.NewParsingFailedError("yaml_parser", fmt.Errorf("failed to parse JSON in parse_json step: %w", err))
		}

		// Merge parsed data into fields - accumulates values for later steps
		for key, value := range jsonData {
			state.Fields[key] = value
		}

		return nil
	}, nil
}

// makeExtractIndexStep extracts the timeseries index.
// In: config[source,format] - field names
// Out: TransformationStep - converts to time.Time
// Ex: makeExtractIndexStep({"source": "ts"}) → step
// Per ADR-005: handles various timestamp formats
func makeExtractIndexStep(config map[string]interface{}) (TransformationStep, error) {
	source, ok := config["source"].(string)
	if !ok {
		return nil, errors.NewInvalidConfigError("yaml_parser", "extract_index step requires 'source' string")
	}

	target := "$timestamp" // Always use $timestamp for the index

	format, ok := config["format"].(string)
	if !ok {
		format = "rfc3339" // Default format
	}

	return func(state *ParseState) error {
		value, exists := state.Fields[source]
		if !exists {
			return errors.NewParsingFailedError("yaml_parser", fmt.Errorf("source field '%s' not found in extract_index step", source))
		}

		var ts time.Time
		var err error

		switch v := value.(type) {
		case string:
			ts, err = parseTimestampString(v, format)
		case int64:
			ts = time.Unix(v, 0)
		case float64:
			ts = time.Unix(int64(v), 0)
		default:
			return errors.NewParsingFailedError("yaml_parser", fmt.Errorf("cannot extract index from type %T in extract_index step", value))
		}

		if err != nil {
			return errors.NewParsingFailedError("yaml_parser", fmt.Errorf("failed to parse index in extract_index step: %w", err))
		}

		state.Fields[target] = ts

		return nil
	}, nil
}

// makeExtractFieldStep extracts nested fields.
// In: config[source,target,type,on_error] - paths
// Out: TransformationStep - dot notation extraction
// Ex: makeExtractFieldStep({"source": "data.temp"}) → step
// Per ADR-005: supports JSON path navigation
func makeExtractFieldStep(config map[string]interface{}) (TransformationStep, error) {
	source, ok := config["source"].(string)
	if !ok {
		return nil, errors.NewInvalidConfigError("yaml_parser", "extract_field step requires 'source' string")
	}

	target, ok := config["target"].(string)
	if !ok {
		return nil, errors.NewInvalidConfigError("yaml_parser", "extract_field step requires 'target' string")
	}

	fieldType, ok := config["type"].(string)
	if !ok {
		fieldType = "auto" // Default to auto-detection
	}

	onError, ok := config["on_error"].(string)
	if !ok {
		onError = "fail" // Default to fail
	}

	return func(state *ParseState) error {
		value, err := extractFieldByPath(state.Fields, source)
		if err != nil {
			if onError == "skip" {
				return nil
			}

			return errors.NewParsingFailedError("yaml_parser", fmt.Errorf("failed to extract field '%s' in extract_field step: %w", source, err))
		}

		// Type conversion - handles string→number, etc per ADR-005
		convertedValue, err := convertFieldValue(value, fieldType)
		if err != nil {
			if onError == "skip" {
				return nil
			}

			return errors.NewParsingFailedError("yaml_parser", fmt.Errorf("failed to convert field '%s' to type '%s' in extract_field step: %w", source, fieldType, err))
		}

		state.Fields[target] = convertedValue

		return nil
	}, nil
}

// makeComputeFieldStep computes derived fields.
// In: config[operation,target,fields] - computation
// Out: TransformationStep - concat operation supported
// Ex: makeComputeFieldStep({"operation": "concat"}) → step
// Per ADR-005: string concatenation for computed fields
func makeComputeFieldStep(config map[string]interface{}) (TransformationStep, error) {
	operation, ok := config["operation"].(string)
	if !ok {
		return nil, errors.NewInvalidConfigError("yaml_parser", "compute_field step requires 'operation' string")
	}

	target, ok := config["target"].(string)
	if !ok {
		return nil, errors.NewInvalidConfigError("yaml_parser", "compute_field step requires 'target' string")
	}

	fieldsInterface, ok := config["fields"]
	if !ok {
		return nil, errors.NewInvalidConfigError("yaml_parser", "compute_field step requires 'fields' array")
	}

	fields, ok := fieldsInterface.([]interface{})
	if !ok {
		return nil, errors.NewInvalidConfigError("yaml_parser", "compute_field 'fields' must be an array")
	}

	switch operation {
	case "concat":
		return makeStringConcatStep(target, fields), nil
	default:
		return nil, errors.NewParsingFailedError("yaml_parser",
			fmt.Errorf("unsupported operation in compute_field step: %s", operation))
	}
}

// makeSafeParseNumberStep parses numbers gracefully.
// In: config[source,target,on_error] - field names
// Out: TransformationStep - null/skip/fail on error
// Ex: makeSafeParseNumberStep({"on_error": "null"}) → step
// Per ADR-005: graceful error handling pattern
func makeSafeParseNumberStep(config map[string]interface{}) (TransformationStep, error) {
	source, ok := config["source"].(string)
	if !ok {
		return nil, errors.NewInvalidConfigError("yaml_parser", "safe_parse_number step requires 'source' string")
	}

	target, ok := config["target"].(string)
	if !ok {
		return nil, errors.NewInvalidConfigError("yaml_parser", "safe_parse_number step requires 'target' string")
	}

	onError, ok := config["on_error"].(string)
	if !ok {
		onError = "null" // Default to null
	}

	return func(state *ParseState) error {
		value, exists := state.Fields[source]
		if !exists {
			if onError == "skip" {
				return nil
			}

			return errors.NewParsingFailedError("yaml_parser", fmt.Errorf("source field '%s' not found in safe_parse_number step", source))
		}

		var number float64
		var err error

		switch v := value.(type) {
		case string:
			number, err = strconv.ParseFloat(v, 64)
		case int64:
			number = float64(v)
		case int:
			number = float64(v)
		case float64:
			number = v
		case float32:
			number = float64(v)
		default:
			err = fmt.Errorf("cannot parse number from type %T in safe_parse_number step", value)
		}

		if err != nil {
			switch onError {
			case "null":
				state.Fields[target] = nil

				return nil
			case "skip":
				return nil
			case "fail":
				return errors.NewParsingFailedError("yaml_parser", fmt.Errorf("failed to parse number in safe_parse_number step: %w", err))
			default:
				return errors.NewParsingFailedError("yaml_parser", fmt.Errorf("failed to parse number in safe_parse_number step: %w", err))
			}
		}

		state.Fields[target] = number

		return nil
	}, nil
}

// makeStringConcatStep concatenates field values.
// In: target, fields[] - output field, inputs
// Out: TransformationStep - joins strings
// Ex: makeStringConcatStep("tag_id", [facility,":",tag]) → step
func makeStringConcatStep(target string, fields []interface{}) TransformationStep {
	return func(state *ParseState) error {
		var result strings.Builder

		for _, field := range fields {
			switch f := field.(type) {
			case string:
				// Check if it's a literal string (contains no field references)
				if strings.HasPrefix(f, "\"") && strings.HasSuffix(f, "\"") {
					// Literal string - strip quotes for direct value
					result.WriteString(f[1 : len(f)-1])
				} else if value, exists := state.Fields[f]; exists {
					// Field reference - lookup and format value
					result.WriteString(fmt.Sprintf("%v", value))
				} else {
					// Missing field - use literal as fallback per ADR-005
					result.WriteString(f)
				}
			default:
				result.WriteString(fmt.Sprintf("%v", field))
			}
		}

		state.Fields[target] = result.String()

		return nil
	}
}

// extractFieldByPath navigates nested maps by path.
// In: fields map, path "a.b.c" - dot notation
// Out: interface{} - value at path
// Ex: extractFieldByPath(data, "sensors.temp") → 23.5
func extractFieldByPath(fields map[string]interface{}, path string) (interface{}, error) {
	// Validate path format to prevent malformed paths
	if path == "" {
		return nil, fmt.Errorf("empty field path")
	}
	if strings.HasPrefix(path, ".") || strings.HasSuffix(path, ".") {
		return nil, fmt.Errorf("invalid field path format: leading/trailing dots not allowed")
	}
	if strings.Contains(path, "..") {
		return nil, fmt.Errorf("invalid field path format: consecutive dots not allowed")
	}

	parts := strings.Split(path, ".")
	current := fields

	for i, part := range parts {
		// Validate each part to prevent empty path segments
		if part == "" {
			return nil, fmt.Errorf("invalid field path format: empty path segment")
		}

		if i == len(parts)-1 {
			// Last part - return the value at final key
			value, exists := current[part]
			if !exists {
				return nil, fmt.Errorf("field '%s' not found", part)
			}

			return value, nil
		}

		// Navigate deeper - traverse nested maps
		next, exists := current[part]
		if !exists {
			return nil, fmt.Errorf("field '%s' not found in path", part)
		}

		nextMap, ok := next.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("field '%s' is not a map", part)
		}

		current = nextMap
	}

	return nil, fmt.Errorf("empty path")
}

// convertFieldValue casts to target type.
// In: value any, targetType - type name
// Out: typed value or conversion error
// Ex: convertFieldValue("42", "int64") → int64(42)
func convertFieldValue(value interface{}, targetType string) (interface{}, error) {
	if targetType == "auto" {
		return value, nil
	}

	switch targetType {
	case "string":
		return fmt.Sprintf("%v", value), nil
	case "int64":
		switch v := value.(type) {
		case int64:
			return v, nil
		case int:
			return int64(v), nil
		case float64:
			if v > math.MaxInt64 || v < math.MinInt64 {
				return 0, fmt.Errorf("float64 value %f out of int64 range", v)
			}

			return int64(v), nil
		case string:
			return strconv.ParseInt(v, 10, 64)
		default:
			return nil, fmt.Errorf("cannot convert %T to int64", value)
		}
	case "float64":
		switch v := value.(type) {
		case float64:
			return v, nil
		case int64:
			return float64(v), nil
		case int:
			return float64(v), nil
		case string:
			return strconv.ParseFloat(v, 64)
		default:
			return nil, fmt.Errorf("cannot convert %T to float64", value)
		}
	case "bool":
		switch v := value.(type) {
		case bool:
			return v, nil
		case string:
			return strconv.ParseBool(v)
		default:
			return nil, fmt.Errorf("cannot convert %T to bool", value)
		}
	case "bytes":
		switch v := value.(type) {
		case []byte:
			return v, nil
		case string:
			return []byte(v), nil
		default:
			return nil, fmt.Errorf("cannot convert %T to bytes", value)
		}
	default:
		return nil, fmt.Errorf("unsupported target type: %s", targetType)
	}
}

// parseTimestampString parses time formats.
// In: value, format - timestamp string
// Out: time.Time - parsed timestamp
// Ex: parseTimestampString("2024-01-01", "rfc3339") → time
// Per ADR-005: handles various timestamp formats
func parseTimestampString(value, format string) (time.Time, error) {
	switch format {
	case "rfc3339":
		return time.Parse(time.RFC3339, value)
	case "rfc3339nano":
		return time.Parse(time.RFC3339Nano, value)
	case "unix":
		unix, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return time.Time{}, err
		}

		return time.Unix(unix, 0), nil
	default:
		// Custom format - use Go time layout per ADR-005
		return time.Parse(format, value)
	}
}
