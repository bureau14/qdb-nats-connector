// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package parser: YAML-based message transformation pipelines
// Types: YAMLParser, YAMLConfig, ParseState
// Ex: NewYAMLParser(opts).Parse(msg) → []WriterTable
//
// CRITICAL MEMORY PINNING GUIDE FOR DEVELOPERS:
// ============================================
//
// This parser creates data that will be written to QuasarDB, which uses a zero-copy
// architecture with direct memory access from C++. ALL strings created in this parser
// MUST be compatible with Go's runtime.Pinner to avoid SEGMENTATION FAULTS.
//
// BACKGROUND:
// QuasarDB's Go API vendored in this project uses an optimization where Go strings
// are pinned in memory and their underlying byte arrays are accessed directly from
// C++ code. This is achieved through:
// - unsafe.StringData() to get the raw memory pointer
// - runtime.Pinner to prevent the Go garbage collector from moving the memory
// - Direct pointer passing to C++ for zero-copy operations
//
// THE PROBLEM:
// Some Go string creation methods produce strings that are NOT backed by contiguous
// memory regions. When QuasarDB tries to pin these strings, it causes a SEGFAULT
// because the memory layout is incompatible with the pinning mechanism.
//
// SAFE STRING CREATION METHODS:
// ✓ Direct concatenation using + operator
// ✓ fmt.Sprintf() and other fmt functions
// ✓ String literals
// ✓ strconv functions
// ✓ Simple type conversions
//
// UNSAFE STRING CREATION METHODS:
// ✗ strings.Builder - May use internal optimizations with non-contiguous memory
// ✗ Buffer pools that don't guarantee contiguous memory
// ✗ Any custom string building that doesn't ensure single memory region
//
// DEBUGGING SEGFAULTS:
// If you encounter segfaults:
// 1. Check the stack trace for qdb_batch_push_columns
// 2. Review ALL string creation in the transformation pipeline
// 3. Replace strings.Builder with direct concatenation
// 4. Test with large batches to trigger memory pressure
// 5. Use race detector: go test -race
//
// EXAMPLE FIX:
// WRONG:
//
//	var sb strings.Builder
//	sb.WriteString(prefix)
//	sb.WriteString(suffix)
//	result := sb.String()  // MAY SEGFAULT
//
// CORRECT:
//
//	result := prefix + suffix  // SAFE
package parser

import (
	"bytes"
	"compress/gzip"
	"crypto/sha1" //nolint:gosec // sharding scheme, not cryptographic use
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/internal/filter"
	"github.com/nats-io/nats.go"
	"gopkg.in/yaml.v3"
)

// ParseState: Mutable state through pipeline
// Message: original NATS msg (read-only)
// Fields: accumulated typed values
// Per ADR-005: modified in-place for performance
type ParseState struct {
	Message *nats.Msg              // Original message (read-only)
	Data    []byte                 // Current data representation
	Fields  map[string]interface{} // Accumulated typed values
	Errors  []error                // Non-fatal errors with context

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

// compiledStep pairs a compiled step function with its compile-time
// classification. Structural steps either decode the payload itself
// (decompress, parse_json, parse_protobuf, unpack) or reject the message's
// shape outright (extract_map_entry): when one fails, nothing downstream
// can succeed and the message is structurally unusable.
type compiledStep struct {
	fn         TransformationStep
	name       string
	structural bool
}

// isStructuralStep reports whether a failed step of this kind makes the
// whole message unusable (vs a partial field failure per ADR-005).
func isStructuralStep(name string) bool {
	return name == "decompress" || name == "parse_json" || name == "parse_protobuf" ||
		name == "extract_map_entry" || name == "unpack"
}

// stepRegistry maps names to step factories.
// Per ADR-005: pre-compiled steps for <5% overhead
var stepRegistry = map[string]func(map[string]interface{}) (TransformationStep, error){
	"decompress":        makeDecompressStep,
	"parse_json":        makeParseJSONStep,
	"parse_protobuf":    makeParseProtobufStep,
	"extract_index":     makeExtractIndexStep,
	"extract_timestamp": makeExtractTimestampStep,
	"extract_field":     makeExtractFieldStep,
	"extract_map_entry": makeExtractMapEntryStep,
	"extract_subject":   makeExtractSubjectStep,
	"extract_table":     makeExtractTableStep,
	"compute_field":     makeComputeFieldStep,
	"safe_parse_number": makeSafeParseNumberStep,
	"unpack":            makeUnpackStep,
}

// YAMLConfig: YAML parser configuration
// Per ADR-005: declarative transformation step pipeline
type YAMLConfig struct {
	Compression     string          `yaml:"compression"`
	Output          OutputSchema    `yaml:"output"`
	Transformations []TransformSpec `yaml:"transformations"`
	Filters         filter.Spec     `yaml:"filters"`
}

// OutputSchema: QuasarDB table schema
type OutputSchema struct {
	Columns []ColumnSchema `yaml:"columns"`
}

// ColumnSchema: column name+type
type ColumnSchema struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"` // timestamp, double, int64, blob, string, symbol
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
	config   YAMLConfig
	pipeline []compiledStep

	// hasIndexStep: config defines an extract_index step. When true, a
	// message that produced no $timestamp is structurally unusable; the
	// time.Now() index fallback only applies to configs without one.
	// explode is the other timestamp producer (explode.index) and is
	// mutually exclusive with extract_index, so at most one of
	// hasIndexStep / explode is ever set.
	hasIndexStep bool

	// explode: compiled terminal explode spec; nil for scalar configs.
	// When set, Parse materializes one N-row WriterTable per message via
	// parseExploded instead of the single-row createWriterTable path.
	explode *explodeSpec

	// Column mapping - pre-computed at startup
	columnTypes []qdb.TsColumnType
	columns     []qdb.WriterColumn

	// rowFilter drops/keeps rows pre-merge; nil means pass-through.
	rowFilter *filter.RowFilter
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

	return NewYAMLParserFromConfig(config)
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
// Per ADR-005: validates schema and compiles pipeline at initialization.
// Also builds the optional row filter from `config.Filters`; a nil filter
// means pass-through.
func NewYAMLParserFromConfig(config YAMLConfig) (*YAMLParser, error) {
	slog.Info("Initializing YAML parser", "transformations", len(config.Transformations))

	// Validate schema
	err := validateSchema(config.Output)
	if err != nil {
		return nil, err
	}

	// Split off the terminal explode spec (if any): it is not a pipeline
	// step, it changes row materialization (see explode.go).
	steps, explodeCfg, err := splitExplodeSpec(config.Transformations)
	if err != nil {
		return nil, err
	}

	// Compile pipeline
	pipeline, err := compilePipeline(steps)
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

	// Compile the terminal explode spec against pipeline + schema; nil for
	// scalar configs. Then enforce the broadcast rule: arrays cannot reach
	// output columns without an explode binding.
	explode, err := compileExplodeSpec(explodeCfg, steps, config.Output.Columns, columnTypes)
	if err != nil {
		return nil, err
	}

	err = validateArrayBindings(steps, explode, config.Output.Columns)
	if err != nil {
		return nil, err
	}

	// Build the optional row filter from the filters block + ordered columns.
	// Exploded (per-sample) columns are not filterable: Apply evaluates row 0
	// only. Returns nil (pass-through) when no filters are configured.
	rowFilter, err := filter.New(config.Filters, columns, explodedColumnNames(explode))
	if err != nil {
		return nil, err
	}

	parser := &YAMLParser{
		config:       config,
		pipeline:     pipeline,
		hasIndexStep: hasStep(steps, "extract_index"),
		explode:      explode,
		columns:      columns,
		columnTypes:  columnTypes,
		rowFilter:    rowFilter,
	}

	return parser, nil
}

// Parse transforms single NATS message to a classified ParseResult.
// In: msg *nats.Msg - NATS message
// Out: ParseResult, error - classified tables/outcome, or invariant error
// Ex: Parse(msg) → {Tables: [{sensors table}], Outcome: OutcomeOK}, nil
//
// The parser only classifies; drop/fail policy is applied by the caller.
func (p *YAMLParser) Parse(msg *nats.Msg) (ParseResult, error) {
	if msg == nil {
		return ParseResult{}, connectorErrors.NewParsingFailedError("yaml_parser", fmt.Errorf("nil message"))
	}
	// Empty payload is structurally unusable, not an invariant violation:
	// an error here would NACK-loop the message forever in drop mode.
	if len(msg.Data) == 0 {
		return ParseResult{
			Outcome: OutcomeUnusable,
			Errors:  []error{connectorErrors.NewParsingFailedError("yaml_parser", fmt.Errorf("empty message data"))},
		}, nil
	}

	// Create new ParseState per call to prevent memory corruption
	state := p.newParseState()
	state.Message = msg
	state.Data = msg.Data

	if !p.runPipeline(state) {
		return ParseResult{Outcome: OutcomeUnusable, Errors: state.Errors}, nil
	}

	// Structural floor: a row needs a real measurement timestamp (when the
	// config extracts one) and valid table routing; otherwise the message
	// is unusable and no row may be fabricated.
	tableName, floorErr := p.checkStructuralFloor(state)
	if floorErr != nil {
		state.Errors = append(state.Errors, floorErr)

		return ParseResult{Outcome: OutcomeUnusable, Errors: state.Errors}, nil
	}

	// Terminal explode: materialize one N-row table (see parseExploded);
	// the single-row builder below never runs for explode configs.
	if p.explode != nil {
		return p.parseExploded(state, tableName)
	}

	// Create WriterTable - uses per-call allocated buffers for memory safety
	table, err := p.createWriterTable(state, tableName)
	if err != nil {
		return ParseResult{}, err
	}

	outcome := OutcomeOK
	if len(state.Errors) > 0 {
		outcome = OutcomePartial
	}

	return ParseResult{Tables: []qdb.WriterTable{table}, Outcome: outcome, Errors: state.Errors}, nil
}

// runPipeline executes compiled steps in sequence, accumulating field-step
// errors in state.Errors (ADR-005 partial extraction). A failed structural
// step stops execution immediately - nothing downstream can succeed and the
// remaining steps would only add noise. Returns false when structurally
// unusable.
// Per ADR-005: direct function calls, no reflection
func (p *YAMLParser) runPipeline(state *ParseState) bool {
	for _, step := range p.pipeline {
		err := step.fn(state)
		if err == nil {
			continue
		}

		state.Errors = append(state.Errors, err)
		if step.structural {
			return false
		}
	}

	return true
}

// checkStructuralFloor validates the parsed state can anchor a real row.
// $timestamp is required iff an extract_index step is configured; explode
// configs anchor time via explode.index instead (validated in
// parseExploded) and only need the $table routing checked here.
// In: state *ParseState - post-pipeline state
// Out: string - validated table name, error - structural failure
// Ex: checkStructuralFloor(state) → "sensors\x00", nil
func (p *YAMLParser) checkStructuralFloor(state *ParseState) (string, error) {
	if p.hasIndexStep {
		if _, ok := state.Fields["$timestamp"].(time.Time); !ok {
			return "", connectorErrors.NewParsingFailedError("yaml_parser",
				fmt.Errorf("extract_index step configured but produced no $timestamp"))
		}
	}

	tableNameField, exists := state.Fields["$table"]
	if !exists {
		return "", connectorErrors.NewParsingFailedError("yaml_parser",
			fmt.Errorf("table name not set - extract_table step is required"))
	}

	tableName, ok := tableNameField.(string)
	if !ok {
		return "", connectorErrors.NewParsingFailedError("yaml_parser",
			fmt.Errorf("table name must be a string, got %T", tableNameField))
	}

	// Validate here as well as in extract_table: $table may have been set
	// directly by an extract_field step, bypassing extract_table's own
	// validation when that step failed.
	trimmed := strings.TrimSuffix(tableName, "\x00")

	err := validateTableName(trimmed)
	if err != nil {
		return "", err
	}

	// Memory-pinning contract: table names must carry a null terminator.
	// extract_table guarantees this; re-add it when $table was set directly.
	if !strings.HasSuffix(tableName, "\x00") {
		tableName = fmt.Sprintf("%s\x00", trimmed)
	}

	return tableName, nil
}

// createWriterTable builds QDB table from state.
// In: state *ParseState - parsed fields, tableName - validated by checkStructuralFloor
// Out: qdb.WriterTable - single-row table
// Ex: createWriterTable(state, "sensors\x00") → table
//
//nolint:dupl // per-column-type branches intentionally repeated for buffer-bound performance
func (p *YAMLParser) createWriterTable(state *ParseState, tableName string) (qdb.WriterTable, error) {
	// CRITICAL MEMORY PINNING REQUIREMENTS:
	// =====================================
	// ALL data written to qdb.WriterTable MUST be compatible with Go's runtime.Pinner.
	//
	// TECHNICAL EXPLANATION:
	// QuasarDB's vendored API uses zero-copy operations to avoid memory allocation overhead.
	// This is achieved through:
	// 1. unsafe.StringData() - Gets direct pointer to string's underlying byte array
	// 2. runtime.Pinner - Prevents GC from moving pinned memory during C++ API calls
	// 3. PinToC() method - Pins Go memory for safe access from C++ code
	//
	// STRING CREATION RULES:
	// [OK] SAFE: Direct concatenation using + operator (creates contiguous memory)
	// [OK] SAFE: fmt.Sprintf() and similar formatting functions
	// [OK] SAFE: String literals and simple conversions
	// [FAIL] UNSAFE: strings.Builder (may use internal optimizations with non-contiguous memory)
	// [FAIL] UNSAFE: Any string construction that doesn't guarantee single memory region
	//
	// FAILURE MODE:
	// Using incompatible strings causes SEGMENTATION FAULTS when QuasarDB attempts to pin
	// non-contiguous memory regions. The error typically occurs during batch.Push() operations
	// when PinToC() tries to access memory that doesn't exist or has been optimized away.
	//
	// DEBUGGING TIPS:
	// - Segfaults in qdb_batch_push_columns indicate string memory issues
	// - Check all string construction methods in the data pipeline
	// - Use direct concatenation (+) instead of strings.Builder for safety

	// SENTINEL-FILL INVARIANT:
	// =======================
	// Every output column MUST receive a SetData call, so the resulting single-row
	// table has column_len == index_len == 1 for every column. When the source field
	// is missing from state.Fields or its value's type assertion fails (e.g. because
	// safe_parse_number on_error="null" set state.Fields[target] = nil), the column
	// is filled with the QDB-defined per-type null sentinel:
	//   - Double: math.NaN() (matches QDB_IS_NULL_DOUBLE(pt) -> isnan(pt.value))
	//   - Int64: qdb.Int64Undefined() (matches QDB_IS_NULL_INT64, value 0x8000000000000000)
	//   - Timestamp: qdb.MinTimespec() (matches QDB_IS_NULL_TIMESTAMP)
	//   - String: "" (matches QDB_IS_NULL_STRING(pt) -> pt.content_length == 0)
	//   - Symbol: "" (same as String; symbols are strings client-side, TsValueString)
	//   - Blob: []byte{} (matches QDB_IS_NULL_BLOB(pt) -> pt.content_length == 0)
	// This invariant is the precondition for qdb.MergeSingleTableWriters producing
	// aligned (index_len, column_len) pairs across multi-row merges; without it the
	// merge appends a length-0 column slice to other tables' length-1 column slices,
	// producing a merged table whose column_len < rowCount and whose stored
	// (timestamp, value) pairs are misaligned.

	// Set index - uses parsed index, or current time as fallback. The
	// fallback is only reachable for configs without an extract_index step:
	// checkStructuralFloor classifies a configured-but-missing $timestamp
	// as unusable before this function runs. Explode configs never reach
	// this function at all (Parse dispatches to parseExploded, which owns
	// the time axis), so the fallback stays scalar-only.
	var ts time.Time
	if parsedTs, ok := state.Fields["$timestamp"].(time.Time); ok {
		ts = parsedTs
	} else {
		ts = time.Now()
	}
	// Use goroutine-safe index buffer.
	if len(state.indexBuf) == 0 {
		slog.Error("CRITICAL: indexBuf is empty!")

		return qdb.WriterTable{}, connectorErrors.NewParsingFailedError("yaml_parser",
			fmt.Errorf("indexBuf is empty"))
	}
	state.indexBuf[0] = ts

	// Table name already has null terminator from extract_table step
	// Create table with pre-defined schema - columns were validated at startup
	table, err := qdb.NewWriterTable(tableName, p.columns)
	if err != nil {
		return qdb.WriterTable{}, connectorErrors.NewParsingFailedError("yaml_parser", err)
	}

	table.SetIndex(state.indexBuf)

	// Map fields to columns using pre-allocated buffers. Every output column receives
	// a SetData call -- the sentinel-fill invariant documented above guarantees the
	// merged-table column lengths stay aligned with the merged index length.
	for i, col := range p.columns {
		// Column names have null terminators, but field names don't
		// Strip the null terminator for field lookup
		fieldName := col.ColumnName
		if fieldName != "" && fieldName[len(fieldName)-1] == 0 {
			fieldName = fieldName[:len(fieldName)-1]
		}
		// Lookup may return the zero interface value when the field was never set
		// (e.g. extract_field on_error="skip" suppressed it). The typed assertions
		// below treat zero-interface and wrong-type uniformly: both fall through to
		// the per-type null sentinel so the column always gets written.
		value := state.Fields[fieldName]

		switch p.columnTypes[i] {
		case qdb.TsColumnDouble:
			// Check buffer bounds before accessing
			if i >= len(state.doubleVals) {
				slog.Error("Buffer overflow detected in double column!",
					"column_index", i,
					"buffer_len", len(state.doubleVals),
					"column_name", fieldName)

				return qdb.WriterTable{}, connectorErrors.NewParsingFailedError("yaml_parser",
					fmt.Errorf("buffer overflow: column index %d >= buffer length %d", i, len(state.doubleVals)))
			}
			v, ok := value.(float64)
			if !ok {
				v = math.NaN()
			}
			// Use direct value storage for performance
			state.doubleVals[i] = v
			data := qdb.NewColumnDataDouble([]float64{state.doubleVals[i]})
			err := table.SetData(i, &data)
			if err != nil {
				return qdb.WriterTable{}, connectorErrors.NewParsingFailedError("yaml_parser", err)
			}
		case qdb.TsColumnBlob:
			// Check buffer bounds before accessing
			if i >= len(state.blobVals) {
				slog.Error("Buffer overflow detected in blob column!",
					"column_index", i,
					"buffer_len", len(state.blobVals),
					"column_name", fieldName)

				return qdb.WriterTable{}, connectorErrors.NewParsingFailedError("yaml_parser",
					fmt.Errorf("buffer overflow: column index %d >= buffer length %d", i, len(state.blobVals)))
			}
			v, ok := value.([]byte)
			if !ok {
				v = []byte{}
			}
			// Use direct value storage for performance
			state.blobVals[i] = v
			data := qdb.NewColumnDataBlob([][]byte{state.blobVals[i]})
			err := table.SetData(i, &data)
			if err != nil {
				return qdb.WriterTable{}, connectorErrors.NewParsingFailedError("yaml_parser", err)
			}
		case qdb.TsColumnInt64:
			// Check buffer bounds before accessing
			if i >= len(state.int64Vals) {
				slog.Error("Buffer overflow detected in int64 column!",
					"column_index", i,
					"buffer_len", len(state.int64Vals),
					"column_name", fieldName)

				return qdb.WriterTable{}, connectorErrors.NewParsingFailedError("yaml_parser",
					fmt.Errorf("buffer overflow: column index %d >= buffer length %d", i, len(state.int64Vals)))
			}
			v, ok := value.(int64)
			if !ok {
				v = qdb.Int64Undefined()
			}
			// Use direct value storage for performance
			state.int64Vals[i] = v
			data := qdb.NewColumnDataInt64([]int64{state.int64Vals[i]})
			err := table.SetData(i, &data)
			if err != nil {
				return qdb.WriterTable{}, connectorErrors.NewParsingFailedError("yaml_parser", err)
			}
		case qdb.TsColumnString:
			// MEMORY PINNING CRITICAL POINT:
			// This string will be pinned by QuasarDB using runtime.Pinner.
			// It MUST be backed by contiguous memory or SEGFAULT will occur.
			// Check buffer bounds before accessing
			if i >= len(state.stringVals) {
				slog.Error("Buffer overflow detected in string column!",
					"column_index", i,
					"buffer_len", len(state.stringVals),
					"column_name", fieldName)

				return qdb.WriterTable{}, connectorErrors.NewParsingFailedError("yaml_parser",
					fmt.Errorf("buffer overflow: column index %d >= buffer length %d", i, len(state.stringVals)))
			}
			v, ok := value.(string)
			if !ok {
				// Empty string is QDB's null sentinel for string columns
				// (QDB_IS_NULL_STRING(pt) -> pt.content_length == 0).
				// String literals are contiguous memory; pinning-safe.
				v = ""
			}
			// CRITICAL: Fresh string allocations prevent segfaults
			// The string value 'v' may come from shared memory (e.g., from YAML config
			// or from compute_field operations). We must create a fresh string that
			// will remain valid when pinned by QuasarDB.
			state.stringVals[i] = v
			data := qdb.NewColumnDataString([]string{state.stringVals[i]})
			err := table.SetData(i, &data)
			if err != nil {
				return qdb.WriterTable{}, connectorErrors.NewParsingFailedError("yaml_parser", err)
			}
		case qdb.TsColumnTimestamp:
			// Check buffer bounds before accessing
			if i >= len(state.timestampVals) {
				slog.Error("Buffer overflow detected in timestamp column!",
					"column_index", i,
					"buffer_len", len(state.timestampVals),
					"column_name", fieldName)

				return qdb.WriterTable{}, connectorErrors.NewParsingFailedError("yaml_parser",
					fmt.Errorf("buffer overflow: column index %d >= buffer length %d", i, len(state.timestampVals)))
			}
			v, ok := value.(time.Time)
			if !ok {
				v = qdb.MinTimespec()
			}
			// Use direct value storage for performance
			state.timestampVals[i] = v
			data := qdb.NewColumnDataTimestamp([]time.Time{state.timestampVals[i]})
			err := table.SetData(i, &data)
			if err != nil {
				return qdb.WriterTable{}, connectorErrors.NewParsingFailedError("yaml_parser", err)
			}
		case qdb.TsColumnSymbol:
			// MEMORY PINNING CRITICAL POINT:
			// Symbols are strings client-side (TsColumnSymbol.AsValueType() ==
			// TsValueString); this string will be pinned by QuasarDB using
			// runtime.Pinner and MUST be backed by contiguous memory.
			// Check buffer bounds before accessing
			if i >= len(state.stringVals) {
				slog.Error("Buffer overflow detected in symbol column!",
					"column_index", i,
					"buffer_len", len(state.stringVals),
					"column_name", fieldName)

				return qdb.WriterTable{}, connectorErrors.NewParsingFailedError("yaml_parser",
					fmt.Errorf("buffer overflow: column index %d >= buffer length %d", i, len(state.stringVals)))
			}
			v, ok := value.(string)
			if !ok {
				// Empty string is QDB's null sentinel for symbol columns,
				// same as string columns (symbols are strings client-side).
				v = ""
			}
			state.stringVals[i] = v
			data := qdb.NewColumnDataString([]string{state.stringVals[i]})
			err := table.SetData(i, &data)
			if err != nil {
				return qdb.WriterTable{}, connectorErrors.NewParsingFailedError("yaml_parser", err)
			}
		case qdb.TsColumnUninitialized:
			// Schema validation rejects uninitialized columns before reaching here.
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
		case qdb.TsColumnDouble, qdb.TsColumnInt64, qdb.TsColumnString, qdb.TsColumnSymbol, qdb.TsColumnTimestamp:
			// These are already allocated with make() above
			// (symbols share stringVals; each column owns its index-i slot)
		case qdb.TsColumnUninitialized:
			// No pre-allocation needed for this type
		}
	}

	return state
}

// resolveDescriptorPaths rewrites relative parse_protobuf schema paths
// (descriptor_file and proto_file) to be baseDir-relative, making a config
// + schema bundle relocatable (both files side by side, wherever mounted).
// Absolute paths pass through untouched. Only file-loaded configs get this
// treatment; programmatic NewYAMLParserFromConfig callers keep cwd/absolute
// semantics.
// In: config *YAMLConfig - freshly unmarshalled config (mutated in place)
//
//	baseDir string - directory of the YAML config file
//
// Ex: resolveDescriptorPaths(&cfg, "/etc/conn") → descriptor_file "/etc/conn/x.desc"
func resolveDescriptorPaths(config *YAMLConfig, baseDir string) {
	for i := range config.Transformations {
		spec := &config.Transformations[i]
		if spec.GetStepName() != "parse_protobuf" || spec.Config == nil {
			continue
		}

		for _, key := range []string{"descriptor_file", "proto_file"} {
			path, ok := spec.Config[key].(string)
			if !ok || path == "" || filepath.IsAbs(path) {
				continue
			}

			spec.Config[key] = filepath.Join(baseDir, path)
		}
	}
}

// LoadYAMLConfig reads YAML parser config. Relative descriptor_file values
// resolve against the config file's directory (resolveDescriptorPaths).
// In: configPath string - YAML file path
// Out: YAMLConfig - parsed config
// Ex: loadYAMLConfig("parser.yaml") → config
func loadYAMLConfig(configPath string) (YAMLConfig, error) {
	data, err := os.ReadFile(configPath) //nolint:gosec // configPath is a trusted operator-provided config file path
	if err != nil {
		return YAMLConfig{}, connectorErrors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("failed to read config file: %v", err))
	}

	var config YAMLConfig
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return YAMLConfig{}, connectorErrors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("failed to parse YAML config: %v", err))
	}

	resolveDescriptorPaths(&config, filepath.Dir(configPath))

	return config, nil
}

// compilePipeline compiles YAML specs to blocks.
// In: specs []TransformSpec - ordered transforms
// Out: []BuildingBlock - executable pipeline
// Ex: compilePipeline([{parse_json},{extract_field}]) → pipeline
// Per ADR-005: pre-compiles at startup for <5% overhead
func compilePipeline(specs []TransformSpec) ([]compiledStep, error) {
	if len(specs) == 0 {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser", "empty transformation pipeline")
	}

	if !hasStep(specs, "extract_table") {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser",
			"transformation pipeline must include an extract_table step")
	}

	steps := make([]compiledStep, len(specs))

	for i, spec := range specs {
		// Lookup factory in registry - validates step types at compile time
		// per ADR-005 to catch configuration errors early
		stepName := spec.GetStepName()
		factory, exists := stepRegistry[stepName]
		if !exists {
			return nil, connectorErrors.NewInvalidConfigError("yaml_parser",
				fmt.Sprintf("unknown transformation step: %s", stepName))
		}

		// Create step with config - factory validates parameters and returns
		// closure over configuration for optimal runtime performance
		step, err := factory(spec.Config)
		if err != nil {
			return nil, connectorErrors.NewInvalidConfigError("yaml_parser",
				fmt.Sprintf("failed to create step '%s': %v", stepName, err))
		}

		steps[i] = compiledStep{
			fn:         step,
			name:       stepName,
			structural: isStructuralStep(stepName),
		}
	}

	return steps, nil
}

// hasStep reports whether specs contain a step of the given name.
// Ex: hasStep(specs, "extract_index") → true
func hasStep(specs []TransformSpec, name string) bool {
	for _, spec := range specs {
		if spec.GetStepName() == name {
			return true
		}
	}

	return false
}

// validateTableName enforces the minimal table name contract.
// QuasarDB accepts liberal names (GP shards are "skf/<hex>"); the
// connector must not be more restrictive. Callers pass the name with
// the \x00 terminator already trimmed.
// In: tableName string - table name without \x00 terminator
// Out: error if empty or first character is not alphanumeric
// Ex: validateTableName("skf/b831") -> nil
func validateTableName(tableName string) error {
	if tableName == "" {
		return connectorErrors.NewParsingFailedError("yaml_parser",
			fmt.Errorf("table name cannot be empty"))
	}

	c := tableName[0]
	isAlnum := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
	if !isAlnum {
		return connectorErrors.NewParsingFailedError("yaml_parser",
			fmt.Errorf("table name must start with an alphanumeric character: %s", tableName))
	}

	return nil
}

// validateSchema checks QuasarDB compatibility.
// In: schema OutputSchema - table schema
// Out: error if invalid schema
// Ex: validateSchema(schema) → nil
func validateSchema(schema OutputSchema) error {
	if len(schema.Columns) == 0 {
		return connectorErrors.NewInvalidConfigError("yaml_parser", "at least one column is required")
	}

	// Check for duplicate column names - QuasarDB requires unique names
	seen := make(map[string]bool)
	for _, col := range schema.Columns {
		if col.Name == "" {
			return connectorErrors.NewInvalidConfigError("yaml_parser", "column name cannot be empty")
		}
		if seen[col.Name] {
			return connectorErrors.NewInvalidConfigError("yaml_parser",
				fmt.Sprintf("duplicate column name: %s", col.Name))
		}
		seen[col.Name] = true

		// Validate column type - must be supported QuasarDB type
		if !isValidColumnType(col.Type) {
			return connectorErrors.NewInvalidConfigError("yaml_parser",
				fmt.Sprintf("invalid column type: %s", col.Type))
		}
	}

	return nil
}

// writerDataType maps a schema column type to the client-side data type used
// by the batch writer. The batch push column data_type describes how data is
// stored CLIENT-side (qdb_exp_batch_push_column_t docs): symbols travel as
// strings and the server resolves the symtable from the table schema, so
// TsColumnSymbol maps to TsColumnString; every other type maps to itself.
// In: colType qdb.TsColumnType - schema column type
// Out: qdb.TsColumnType - client-side writer data type
// Ex: writerDataType(qdb.TsColumnSymbol) → qdb.TsColumnString
func writerDataType(colType qdb.TsColumnType) qdb.TsColumnType {
	if colType == qdb.TsColumnSymbol {
		return qdb.TsColumnString
	}

	return colType
}

// createColumnMapping builds QDB column definitions.
// The WriterColumn carries the client-side data type (symbol -> string, see
// writerDataType); columnTypes keeps the schema type for per-column dispatch.
// In: columns []ColumnSchema - schema columns
// Out: []WriterColumn, []TsColumnType - QDB types
// Ex: createColumnMapping(cols) → columns, types
func createColumnMapping(columns []ColumnSchema) ([]qdb.WriterColumn, []qdb.TsColumnType, error) {
	writerColumns := make([]qdb.WriterColumn, len(columns))
	columnTypes := make([]qdb.TsColumnType, len(columns))

	for i, col := range columns {
		// CRITICAL: Pre-append null terminator to column names to prevent pinStringBytes
		// from modifying them later. This prevents memory corruption when the same
		// column definitions are reused across multiple batches.
		writerColumns[i] = qdb.WriterColumn{
			ColumnName: col.Name + "\x00",
			ColumnType: writerDataType(stringToColumnType(col.Type)),
		}
		columnTypes[i] = stringToColumnType(col.Type)
	}

	return writerColumns, columnTypes, nil
}

// validateColumnSynchronization ensures p.columns and p.config.Output.Columns are properly synchronized
// to prevent buffer overflow issues that could cause segfaults.
// In: configColumns []ColumnSchema - original schema columns
//
//	writerColumns []WriterColumn - internal writer columns
//	columnTypes []TsColumnType - internal column types
//
// Out: error - validation failure or nil if synchronized
// Ex: validateColumnSynchronization(configCols, writerCols, types) → nil
func validateColumnSynchronization(configColumns []ColumnSchema, writerColumns []qdb.WriterColumn, columnTypes []qdb.TsColumnType) error {
	// Validate length consistency - critical for preventing buffer overflows
	if len(configColumns) != len(writerColumns) {
		return connectorErrors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("column count mismatch: schema has %d columns but internal mapping has %d columns",
				len(configColumns), len(writerColumns)))
	}

	if len(configColumns) != len(columnTypes) {
		return connectorErrors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("column count mismatch: schema has %d columns but internal types has %d types",
				len(configColumns), len(columnTypes)))
	}

	if len(writerColumns) != len(columnTypes) {
		return connectorErrors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("internal column count mismatch: writer has %d columns but types has %d entries",
				len(writerColumns), len(columnTypes)))
	}

	// Validate column names and types consistency - ensures proper indexing
	for i, configCol := range configColumns {
		// Check column name consistency (accounting for null terminator)
		expectedName := configCol.Name + "\x00"
		if expectedName != writerColumns[i].ColumnName {
			return connectorErrors.NewInvalidConfigError("yaml_parser",
				fmt.Sprintf("column name mismatch at index %d: config has '%s' but internal mapping has '%s'",
					i, configCol.Name, writerColumns[i].ColumnName))
		}

		// Check column type consistency
		expectedType := stringToColumnType(configCol.Type)
		if expectedType != columnTypes[i] {
			return connectorErrors.NewInvalidConfigError("yaml_parser",
				fmt.Sprintf("column type mismatch at index %d for column '%s': config has type '%s' but internal has type %d",
					i, configCol.Name, configCol.Type, columnTypes[i]))
		}

		// Writer columns carry the client-side data type (symbol -> string).
		if writerDataType(expectedType) != writerColumns[i].ColumnType {
			return connectorErrors.NewInvalidConfigError("yaml_parser",
				fmt.Sprintf("writer column type mismatch at index %d for column '%s': expected type %d but writer has type %d",
					i, configCol.Name, writerDataType(expectedType), writerColumns[i].ColumnType))
		}
	}

	return nil
}

// isValidColumnType validates QDB column type.
// In: colType string - type name
// Out: bool - valid for QuasarDB
// Ex: isValidColumnType("symbol") → true
func isValidColumnType(colType string) bool {
	switch colType {
	case "timestamp", "double", "int64", "blob", "string", "symbol":
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
	case "symbol":
		return qdb.TsColumnSymbol
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
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser", "decompress step requires 'algorithm' string")
	}

	switch algorithm {
	case "gzip":
		return makeGzipDecompressStep(), nil
	default:
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser",
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

			return connectorErrors.NewParsingFailedError("yaml_parser", fmt.Errorf("nil state in gzip decompress step"))
		}

		if state.Data == nil {
			slog.Error("CRITICAL: state.Data is nil in gzip decompress step")

			return connectorErrors.NewParsingFailedError("yaml_parser", fmt.Errorf("nil data in gzip decompress step"))
		}

		if len(state.Data) == 0 {
			return nil
		}

		reader, err := gzip.NewReader(bytes.NewReader(state.Data))
		if err != nil {
			slog.Error("Failed to create gzip reader", "error", err)

			return connectorErrors.NewParsingFailedError("yaml_parser", fmt.Errorf("failed to create gzip reader in decompress step: %w", err))
		}
		defer func() {
			closeErr := reader.Close()
			if closeErr != nil {
				slog.Error("Failed to close gzip reader", "error", closeErr)
				err = connectorErrors.NewParsingFailedError("yaml_parser", fmt.Errorf("failed to close gzip reader in decompress step: %w", closeErr))
			}
		}()

		decompressed, err := io.ReadAll(reader)
		if err != nil {
			slog.Error("Failed to decompress gzip data", "error", err)

			return connectorErrors.NewParsingFailedError("yaml_parser", fmt.Errorf("failed to decompress gzip data in decompress step: %w", err))
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
			return connectorErrors.NewParsingFailedError("yaml_parser", fmt.Errorf("nil state in parse_json step"))
		}
		if state.Data == nil {
			return connectorErrors.NewParsingFailedError("yaml_parser", fmt.Errorf("nil data in parse_json step"))
		}
		if len(state.Data) == 0 {
			return connectorErrors.NewParsingFailedError("yaml_parser", fmt.Errorf("empty data in parse_json step"))
		}

		var jsonData map[string]interface{}
		err := json.Unmarshal(state.Data, &jsonData)
		if err != nil {
			return connectorErrors.NewParsingFailedError("yaml_parser", fmt.Errorf("failed to parse JSON in parse_json step: %w", err))
		}

		// Merge parsed data into fields - accumulates values for later steps
		for key, value := range jsonData {
			state.Fields[key] = value
		}

		return nil
	}, nil
}

// makeTimestampExtraction builds the shared lookup+convert+store closure used
// by extract_index and extract_timestamp, so index and column timestamp
// parsing are identical by construction.
// In: stepName - error context; source - field to read; target - field to write
//
//	format - see convertToTimestamp
//
// Out: TransformationStep - writes time.Time into state.Fields[target]
// Ex: makeTimestampExtraction("extract_index", "ts", "$timestamp", "unix") → step
func makeTimestampExtraction(stepName, source, target, format string) TransformationStep {
	return func(state *ParseState) error {
		value, exists := state.Fields[source]
		if !exists {
			return connectorErrors.NewParsingFailedError("yaml_parser", fmt.Errorf("source field '%s' not found in %s step", source, stepName))
		}

		ts, err := convertToTimestamp(value, format)
		if err != nil {
			return connectorErrors.NewParsingFailedError("yaml_parser", fmt.Errorf("failed to parse timestamp in %s step: %w", stepName, err))
		}

		state.Fields[target] = ts

		return nil
	}
}

// makeExtractIndexStep extracts the timeseries index.
// In: config[source,format] - source field, format (default "rfc3339", see convertToTimestamp)
// Out: TransformationStep - writes time.Time into state.Fields["$timestamp"]
// Ex: makeExtractIndexStep({"source": "ts", "format": "unix_ms"}) → step
// Per ADR-005: handles various timestamp formats
func makeExtractIndexStep(config map[string]interface{}) (TransformationStep, error) {
	source, ok := config["source"].(string)
	if !ok {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser", "extract_index step requires 'source' string")
	}

	format, ok := config["format"].(string)
	if !ok {
		format = "rfc3339" // Default format
	}

	// Target is always $timestamp: the reserved index field.
	return makeTimestampExtraction("extract_index", source, "$timestamp", format), nil
}

// makeExtractTimestampStep creates a timestamp value-column extraction step.
// Identical to extract_index except the parsed time.Time lands in a regular
// output column (a "timestamp"-typed column in the output schema) instead of
// the reserved $timestamp index field.
// In: config[source,target,format] - source field, target column, format (default "rfc3339")
// Out: TransformationStep - writes time.Time into state.Fields[target]
// Ex: makeExtractTimestampStep({"source": "updated_at", "target": "ingested_at"}) → step
func makeExtractTimestampStep(config map[string]interface{}) (TransformationStep, error) {
	source, ok := config["source"].(string)
	if !ok {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser", "extract_timestamp step requires 'source' string")
	}

	target, ok := config["target"].(string)
	if !ok {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser", "extract_timestamp step requires 'target' string")
	}

	// $-prefixed fields ($timestamp, $table) are pipeline-internal; routing the
	// index through this step would bypass the extract_index structural floor.
	if strings.HasPrefix(target, "$") {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser", "extract_timestamp 'target' must not start with '$'; use extract_index for the index")
	}

	format, ok := config["format"].(string)
	if !ok {
		format = "rfc3339" // Default format
	}

	return makeTimestampExtraction("extract_timestamp", source, target, format), nil
}

// makeExtractTableStep creates table name extraction step. Always
// configure exactly one of value/source explicitly: computed table names
// concat into a plain field, then point source at it (compute_field
// rejects $-prefixed targets, so the legacy "$table" default source is
// only reachable by a message that literally carries a "$table" key).
// In: config[value,source] - static value or source field
// Out: TransformationStep - sets state.Fields["$table"]
// Ex: makeExtractTableStep({"source": "table_name"}) → step
func makeExtractTableStep(config map[string]interface{}) (TransformationStep, error) {
	// Two modes:
	// 1. value: "static_table_name" - for static tables
	// 2. source: "field_name" - for dynamic tables (default: "$table")

	value, hasValue := config["value"].(string)
	source, hasSource := config["source"].(string)

	if !hasSource && !hasValue {
		source = "$table" // Default source
		hasSource = true
	}

	if hasValue && hasSource {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser",
			"extract_table cannot have both 'value' and 'source'")
	}

	return func(state *ParseState) error {
		var tableName string

		if hasValue {
			// CRITICAL: Create defensive copy with null terminator to prevent segfault.
			// QuasarDB's pinStringBytes expects null-terminated strings for table names.
			// We MUST create a new string for each message to prevent memory corruption
			// when the same table name is used across multiple batches.
			//
			// Using fmt.Sprintf with explicit null terminator ensures:
			// 1. A fresh string allocation for each message
			// 2. The string already has the null terminator QuasarDB expects
			// 3. pinStringBytes won't modify the string (it checks for existing \0)
			tableName = fmt.Sprintf("%s\x00", value)
		} else {
			field, exists := state.Fields[source]
			if !exists {
				return connectorErrors.NewParsingFailedError("yaml_parser",
					fmt.Errorf("source field '%s' not found in extract_table", source))
			}
			// Strictly typed: the routed value must already be a string
			if fieldStr, ok := field.(string); ok {
				// CRITICAL: Create defensive copy with null terminator.
				// Same reasoning as above for dynamic routing.
				tableName = fmt.Sprintf("%s\x00", fieldStr)
			} else {
				return connectorErrors.NewParsingFailedError("yaml_parser",
					fmt.Errorf("table name field '%s' must be a string, got %T", source, field))
			}
		}

		// Minimal table-name contract (non-empty, alphanumeric first char;
		// see validateTableName). The \x00 terminator added above is
		// excluded from validation.
		if tableName != "" && tableName[len(tableName)-1] == 0 {
			err := validateTableName(tableName[:len(tableName)-1])
			if err != nil {
				return err
			}
		} else {
			// This shouldn't happen with our code above, but be defensive
			return connectorErrors.NewParsingFailedError("yaml_parser",
				fmt.Errorf("internal error: table name missing null terminator"))
		}

		// Store in special field that createWriterTable will read
		state.Fields["$table"] = tableName

		return nil
	}, nil
}

// validateFieldPath checks dot-path format: non-empty, no leading/trailing
// or consecutive dots. Called by compileFieldPath at config compile time.
// In: path "a.b.c" - dot notation
// Out: error - nil when well-formed
// Ex: validateFieldPath("a..b") → error
func validateFieldPath(path string) error {
	if path == "" {
		return fmt.Errorf("empty field path")
	}
	if strings.HasPrefix(path, ".") || strings.HasSuffix(path, ".") {
		return fmt.Errorf("invalid field path format: leading/trailing dots not allowed")
	}
	if strings.Contains(path, "..") {
		return fmt.Errorf("invalid field path format: consecutive dots not allowed")
	}

	return nil
}

// compileFieldPath validates a dot-path at config time and returns a
// per-message lookup closure. Segments are split once, here; the closure
// only walks nested maps. A missing key or a non-map intermediate resolves
// to (nil, false). Known accepted wart: a flat field whose literal name
// contains a dot is unaddressable.
// In: path "a.b.c" - dot notation
// Out: lookup closure over state.Fields, or a path-format error
// Ex: compileFieldPath("sensors.temp") → lookup; lookup(fields) → (23.5, true)
func compileFieldPath(path string) (func(fields map[string]interface{}) (interface{}, bool), error) {
	err := validateFieldPath(path)
	if err != nil {
		return nil, err
	}

	segments := strings.Split(path, ".")

	return func(fields map[string]interface{}) (interface{}, bool) {
		current := fields

		for _, segment := range segments[:len(segments)-1] {
			next, ok := current[segment].(map[string]interface{})
			if !ok {
				return nil, false
			}

			current = next
		}

		value, ok := current[segments[len(segments)-1]]

		return value, ok
	}, nil
}

// makeExtractFieldStep extracts nested fields.
// In: config[source,target,type,on_error] - source is a compiled dot-path
// Out: TransformationStep - dot notation extraction
// Ex: makeExtractFieldStep({"source": "data.temp"}) → step
// Per ADR-005: supports JSON path navigation
func makeExtractFieldStep(config map[string]interface{}) (TransformationStep, error) {
	source, ok := config["source"].(string)
	if !ok {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser", "extract_field step requires 'source' string")
	}

	lookupSource, err := compileFieldPath(source)
	if err != nil {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("extract_field 'source' is not a valid field path: %v", err))
	}

	target, ok := config["target"].(string)
	if !ok {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser", "extract_field step requires 'target' string")
	}

	// type is deliberately required: parse_json emits float64 for ALL
	// numbers while parse_protobuf emits int64 for integer kinds, and
	// createWriterTable sentinel-fills type-assert mismatches silently. An
	// implicit "auto" default turns that divergence into silent corruption;
	// an explicit type: "auto" keeps intentional pass-through expressible.
	fieldType, ok := config["type"].(string)
	if !ok || fieldType == "" {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser",
			"extract_field step requires an explicit 'type' (use \"auto\" for intentional pass-through)")
	}

	onError, ok := config["on_error"].(string)
	if !ok {
		onError = "fail" // Default to fail
	}

	return func(state *ParseState) error {
		value, exists := lookupSource(state.Fields)
		if !exists {
			if onError == "skip" {
				return nil
			}

			return connectorErrors.NewParsingFailedError("yaml_parser", fmt.Errorf("source field '%s' not found in extract_field step", source))
		}

		// Type conversion - handles string→number, etc per ADR-005
		convertedValue, err := convertFieldValue(value, fieldType)
		if err != nil {
			if onError == "skip" {
				return nil
			}

			return connectorErrors.NewParsingFailedError("yaml_parser", fmt.Errorf("failed to convert field '%s' to type '%s' in extract_field step: %w", source, fieldType, err))
		}

		state.Fields[target] = convertedValue

		return nil
	}, nil
}

// makeComputeFieldStep computes derived fields.
// In: config[operation,target,...] - operation "concat" (fields array),
//
//	"split" (source, separator, index),
//	"hash" (source, algorithm),
//	"slice" (source, start, optional end),
//	"str" (source)
//
// Out: TransformationStep - derived string field
// Ex: makeComputeFieldStep({"operation": "concat"}) → step
// Per ADR-005: string concatenation for computed fields
func makeComputeFieldStep(config map[string]interface{}) (TransformationStep, error) {
	operation, ok := config["operation"].(string)
	if !ok {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser", "compute_field step requires 'operation' string")
	}

	target, ok := config["target"].(string)
	if !ok {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser", "compute_field step requires 'target' string")
	}

	// $-prefixed fields ($timestamp, $table) are pipeline-internal and
	// reserved for extract_* steps; compose table routing as concat into a
	// plain field, then point extract_table 'source' at it.
	if strings.HasPrefix(target, "$") {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser",
			"compute_field 'target' must not start with '$'; write a plain field and point extract_table 'source' at it")
	}

	switch operation {
	case "concat":
		cfg, err := parseConcatConfig(config, target)
		if err != nil {
			return nil, err
		}

		return makeStringConcatStep(cfg), nil
	case "split":
		cfg, err := parseSplitConfig(config, target)
		if err != nil {
			return nil, err
		}

		return makeSplitFieldStep(cfg), nil
	case "hash":
		cfg, err := parseHashConfig(config, target)
		if err != nil {
			return nil, err
		}

		return makeHashFieldStep(cfg), nil
	case "slice":
		cfg, err := parseSliceConfig(config, target)
		if err != nil {
			return nil, err
		}

		return makeSliceFieldStep(cfg), nil
	case "str":
		cfg, err := parseStrConfig(config, target)
		if err != nil {
			return nil, err
		}

		return makeStrFieldStep(cfg), nil
	default:
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("unsupported operation in compute_field step: %s", operation))
	}
}

// makeSafeParseNumberStep parses numbers gracefully.
// In: config[source,target,on_error] - source is a compiled dot-path
// Out: TransformationStep - null/skip/fail on error
// Ex: makeSafeParseNumberStep({"on_error": "null"}) → step
// Per ADR-005: graceful error handling pattern
func makeSafeParseNumberStep(config map[string]interface{}) (TransformationStep, error) {
	source, ok := config["source"].(string)
	if !ok {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser", "safe_parse_number step requires 'source' string")
	}

	lookupSource, err := compileFieldPath(source)
	if err != nil {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("safe_parse_number 'source' is not a valid field path: %v", err))
	}

	target, ok := config["target"].(string)
	if !ok {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser", "safe_parse_number step requires 'target' string")
	}

	onError, ok := config["on_error"].(string)
	if !ok {
		onError = "null" // Default to null
	}

	return func(state *ParseState) error {
		value, exists := lookupSource(state.Fields)
		if !exists {
			if onError == "skip" {
				return nil
			}

			return connectorErrors.NewParsingFailedError("yaml_parser", fmt.Errorf("source field '%s' not found in safe_parse_number step", source))
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
				return connectorErrors.NewParsingFailedError("yaml_parser", fmt.Errorf("failed to parse number in safe_parse_number step: %w", err))
			default:
				return connectorErrors.NewParsingFailedError("yaml_parser", fmt.Errorf("failed to parse number in safe_parse_number step: %w", err))
			}
		}

		state.Fields[target] = number

		return nil
	}, nil
}

// compileSourceOption reads and compiles the required 'source' dot-path
// option of a compute_field operation.
// In: config map - raw step config, operation - name for error messages
// Out: (original path, compiled lookup closure) or config error
// Ex: compileSourceOption({"source": "a.b"}, "hash") → ("a.b", lookup, nil)
func compileSourceOption(config map[string]interface{}, operation string) (source string, lookup func(fields map[string]interface{}) (interface{}, bool), err error) {
	source, ok := config["source"].(string)
	if !ok || source == "" {
		return "", nil, connectorErrors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("compute_field '%s' requires a 'source' string option", operation))
	}

	lookupSource, err := compileFieldPath(source)
	if err != nil {
		return "", nil, connectorErrors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("compute_field '%s' source is not a valid field path: %v", operation, err))
	}

	return source, lookupSource, nil
}

// requireStringSource resolves a compiled source lookup and requires a
// string value. A missing or wrong-typed value is a per-message parsing
// error (non-structural → OutcomePartial), never a silent coercion.
// In: lookup - compiled dot-path closure, source - original path for errors,
//
//	operation - compute_field operation name, fields - message fields
//
// Out: string value or ParsingFailed error
// Ex: requireStringSource(lookup, "stream_id", "hash", fields) → "skf-cxrn:..."
func requireStringSource(lookup func(fields map[string]interface{}) (interface{}, bool), source, operation string, fields map[string]interface{}) (string, error) {
	value, exists := lookup(fields)
	if !exists {
		return "", connectorErrors.NewParsingFailedError("yaml_parser",
			fmt.Errorf("field '%s' not found in compute_field %s step", source, operation))
	}

	s, ok := value.(string)
	if !ok {
		return "", connectorErrors.NewParsingFailedError("yaml_parser",
			fmt.Errorf("compute_field %s source '%s' has type %T, want string", operation, source, value))
	}

	return s, nil
}

// concatPart: one compiled entry of a concat fields[] array. A nil lookup
// means the entry is a literal; otherwise it is a compiled dot-path.
type concatPart struct {
	literal string
	source  string // original dot-path, for error messages
	lookup  func(fields map[string]interface{}) (interface{}, bool)
}

// concatFieldConfig: compile-time configuration of a compute_field concat.
type concatFieldConfig struct {
	target string
	parts  []concatPart
}

// parseConcatConfig validates compute_field concat options and classifies
// each fields[] entry at config time: a `"quoted"` entry is a literal
// (quotes stripped), anything else is a compiled dot-path whose resolved
// value must be a string per message.
// In: config["fields"] - non-empty array of string entries
//
//	target string - already-validated output field name
//
// Out: concatFieldConfig - validated config
// Ex: parseConcatConfig({"fields": ['"skf/"', "shard"]}, "table_name") → cfg
func parseConcatConfig(config map[string]interface{}, target string) (concatFieldConfig, error) {
	fields, ok := config["fields"].([]interface{})
	if !ok || len(fields) == 0 {
		return concatFieldConfig{}, connectorErrors.NewInvalidConfigError("yaml_parser",
			"compute_field 'concat' requires a non-empty 'fields' array")
	}

	parts := make([]concatPart, 0, len(fields))

	for _, field := range fields {
		f, ok := field.(string)
		if !ok {
			return concatFieldConfig{}, connectorErrors.NewInvalidConfigError("yaml_parser",
				fmt.Sprintf("compute_field 'concat' fields entries must be strings, got %T", field))
		}

		if len(f) >= 2 && strings.HasPrefix(f, "\"") && strings.HasSuffix(f, "\"") {
			parts = append(parts, concatPart{literal: f[1 : len(f)-1]})

			continue
		}

		lookup, err := compileFieldPath(f)
		if err != nil {
			return concatFieldConfig{}, connectorErrors.NewInvalidConfigError("yaml_parser",
				fmt.Sprintf("compute_field 'concat' fields entry is not a valid field path: %v", err))
		}

		parts = append(parts, concatPart{source: f, lookup: lookup})
	}

	return concatFieldConfig{target: target, parts: parts}, nil
}

// makeStringConcatStep concatenates literal and field-valued parts. Field
// parts must resolve to strings; a missing or non-string value is a
// per-message parsing error (non-structural → OutcomePartial), never a
// silent coercion or fallback.
// In: cfg concatFieldConfig - validated config
// Out: TransformationStep - writes the joined string to cfg.target
// Ex: makeStringConcatStep({parts: ["skf/", shard], target: "table_name"}) → step
//
// MEMORY PINNING WARNING:
// ======================
// This function uses direct string concatenation (+= operator) which is SAFE for QuasarDB.
//
// DO NOT REFACTOR TO USE strings.Builder!
// strings.Builder may use internal optimizations that create non-contiguous memory regions,
// which will cause SEGMENTATION FAULTS when QuasarDB tries to pin the memory for C++ access.
//
// The current implementation guarantees contiguous memory allocation required by:
// - QuasarDB's PinToC() method
// - unsafe.StringData() pointer access
// - runtime.Pinner memory stabilization
//
// Any string created here will be written to QuasarDB tables and MUST remain pinnable.
func makeStringConcatStep(cfg concatFieldConfig) TransformationStep {
	return func(state *ParseState) error {
		var result string // Direct concatenation - QuasarDB memory-safe

		for _, part := range cfg.parts {
			if part.lookup == nil {
				result += part.literal

				continue
			}

			s, err := requireStringSource(part.lookup, part.source, "concat", state.Fields)
			if err != nil {
				return err
			}

			result += s
		}

		state.Fields[cfg.target] = result

		return nil
	}
}

// splitFieldConfig: compile-time configuration of a compute_field split.
type splitFieldConfig struct {
	source       string // original dot-path, for error messages
	lookupSource func(fields map[string]interface{}) (interface{}, bool)
	separator    string
	target       string
	index        int // negative = from the end
}

// parseSplitConfig validates compute_field split options.
// In: config["source"] - input dot-path, compiled here
//
//	config["separator"] - non-empty split separator
//	config["index"] - required int, negative = from the end
//	target string - already-validated output field name
//
// Out: splitFieldConfig - validated config
// Ex: parseSplitConfig({"source": "stream_id", "separator": ":", "index": -1}, "revision_raw") → cfg
func parseSplitConfig(config map[string]interface{}, target string) (splitFieldConfig, error) {
	source, lookupSource, err := compileSourceOption(config, "split")
	if err != nil {
		return splitFieldConfig{}, err
	}

	separator, ok := config["separator"].(string)
	if !ok || separator == "" {
		return splitFieldConfig{}, connectorErrors.NewInvalidConfigError("yaml_parser",
			"compute_field 'split' requires a non-empty 'separator' string option")
	}

	index, present, err := parseIntOption(config, "index")
	if err != nil || !present {
		return splitFieldConfig{}, connectorErrors.NewInvalidConfigError("yaml_parser",
			"compute_field 'split' requires an integer 'index' option")
	}

	return splitFieldConfig{source: source, lookupSource: lookupSource, separator: separator, target: target, index: index}, nil
}

// makeSplitFieldStep creates a compute_field split step: strings.Split the
// source value and select one part. Pure Go split semantics: a separator
// absent from the value yields a single part, so index 0 and -1 both
// resolve to the whole string. The output is a string; int64 coercion
// happens downstream via extract_field/safe_parse_number. Substrings are
// contiguous and pinning-safe (see makeStringConcatStep contract above).
//
// Non-structural: missing/non-string source and out-of-range index follow
// the normal per-step error flow → OutcomePartial.
//
// In: cfg splitFieldConfig - validated config
// Out: TransformationStep - writes the selected part to cfg.target
// Ex: makeSplitFieldStep({source: "stream_id", separator: ":", index: -1, target: "revision_raw"}) → step
func makeSplitFieldStep(cfg splitFieldConfig) TransformationStep {
	return func(state *ParseState) error {
		s, err := requireStringSource(cfg.lookupSource, cfg.source, "split", state.Fields)
		if err != nil {
			return err
		}

		parts := strings.Split(s, cfg.separator)

		idx := cfg.index
		if idx < 0 {
			idx += len(parts)
		}

		if idx < 0 || idx >= len(parts) {
			return connectorErrors.NewParsingFailedError("yaml_parser",
				fmt.Errorf("split index %d out of range (%d parts) in compute_field step", cfg.index, len(parts)))
		}

		state.Fields[cfg.target] = parts[idx]

		return nil
	}
}

// parseIntOption reads an optional integer config value. YAML integer
// literals arrive as int (yaml.v3); Go-literal test configs may pass int64.
// In: config map - raw step config, key - option name
// Out: (value, present, error) - error on a present non-integer
// Ex: parseIntOption({"segment": -2}, "segment") → (-2, true, nil)
func parseIntOption(config map[string]interface{}, key string) (value int, present bool, err error) {
	raw, present := config[key]
	if !present {
		return 0, false, nil
	}

	switch v := raw.(type) {
	case int:
		return v, true, nil
	case int64:
		return int(v), true, nil
	default:
		return 0, false, fmt.Errorf("'%s' must be an integer, got %T", key, raw)
	}
}

// hashFieldConfig: compile-time configuration of a compute_field hash.
type hashFieldConfig struct {
	source       string // original dot-path, for error messages
	lookupSource func(fields map[string]interface{}) (interface{}, bool)
	target       string
	digestHex    func(data []byte) string // algorithm resolved at config time
}

// parseHashConfig validates compute_field hash options and resolves the
// algorithm to a digest closure at config time.
// In: config["source"] - input dot-path, compiled here
//
//	config["algorithm"] - "sha1" or "sha256"
//	target string - already-validated output field name
//
// Out: hashFieldConfig - validated config
// Ex: parseHashConfig({"source": "stream_id", "algorithm": "sha1"}, "stream_id_hashed") → cfg
func parseHashConfig(config map[string]interface{}, target string) (hashFieldConfig, error) {
	source, lookupSource, err := compileSourceOption(config, "hash")
	if err != nil {
		return hashFieldConfig{}, err
	}

	algorithm, ok := config["algorithm"].(string)
	if !ok {
		return hashFieldConfig{}, connectorErrors.NewInvalidConfigError("yaml_parser",
			"compute_field 'hash' requires an 'algorithm' string option")
	}

	var digestHex func(data []byte) string

	switch algorithm {
	case "sha1":
		digestHex = func(data []byte) string {
			d := sha1.Sum(data) //nolint:gosec // sharding scheme, not cryptographic use

			return hex.EncodeToString(d[:])
		}
	case "sha256":
		digestHex = func(data []byte) string {
			d := sha256.Sum256(data)

			return hex.EncodeToString(d[:])
		}
	default:
		return hashFieldConfig{}, connectorErrors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("unsupported algorithm in compute_field hash step: %s", algorithm))
	}

	return hashFieldConfig{source: source, lookupSource: lookupSource, target: target, digestHex: digestHex}, nil
}

// makeHashFieldStep creates a compute_field hash step: digest the source
// string and write the full lowercase hex digest. Truncation is not this
// operation's job; compose with slice. hex.EncodeToString returns a fresh
// contiguous allocation, pinning-safe (see makeStringConcatStep contract
// above).
//
// Non-structural: missing/non-string source follows the normal per-step
// error flow → OutcomePartial.
//
// In: cfg hashFieldConfig - validated config
// Out: TransformationStep - writes the hex digest to cfg.target
// Ex: makeHashFieldStep({source: "stream_id", target: "stream_id_hashed"}) → step
func makeHashFieldStep(cfg hashFieldConfig) TransformationStep {
	return func(state *ParseState) error {
		s, err := requireStringSource(cfg.lookupSource, cfg.source, "hash", state.Fields)
		if err != nil {
			return err
		}

		state.Fields[cfg.target] = cfg.digestHex([]byte(s))

		return nil
	}
}

// sliceFieldConfig: compile-time configuration of a compute_field slice.
type sliceFieldConfig struct {
	source       string // original dot-path, for error messages
	lookupSource func(fields map[string]interface{}) (interface{}, bool)
	target       string
	start        int
	end          int
	hasEnd       bool // omitted end = end of string
}

// parseSliceConfig validates compute_field slice options.
// In: config["source"] - input dot-path, compiled here
//
//	config["start"] - required int, negative = from the end
//	config["end"] - optional int, negative = from the end
//	target string - already-validated output field name
//
// Out: sliceFieldConfig - validated config
// Ex: parseSliceConfig({"source": "stream_id_hashed", "start": 0, "end": 4}, "shard") → cfg
func parseSliceConfig(config map[string]interface{}, target string) (sliceFieldConfig, error) {
	source, lookupSource, err := compileSourceOption(config, "slice")
	if err != nil {
		return sliceFieldConfig{}, err
	}

	start, present, err := parseIntOption(config, "start")
	if err != nil || !present {
		return sliceFieldConfig{}, connectorErrors.NewInvalidConfigError("yaml_parser",
			"compute_field 'slice' requires an integer 'start' option")
	}

	end, hasEnd, err := parseIntOption(config, "end")
	if err != nil {
		return sliceFieldConfig{}, connectorErrors.NewInvalidConfigError("yaml_parser",
			"compute_field 'slice' 'end' option must be an integer")
	}

	return sliceFieldConfig{source: source, lookupSource: lookupSource, target: target, start: start, end: end, hasEnd: hasEnd}, nil
}

// clampSliceIndex resolves a Python slice index against a string of length
// n: negative counts from the end, out-of-range clamps to [0, n].
// In: idx - raw index, n - string length in bytes
// Out: resolved index in [0, n]
// Ex: clampSliceIndex(-1, 3) → 2; clampSliceIndex(10, 3) → 3
func clampSliceIndex(idx, n int) int {
	if idx < 0 {
		idx += n
	}

	return min(max(idx, 0), n)
}

// makeSliceFieldStep creates a compute_field slice step with Python slice
// semantics on the source string: negative indices count from the end,
// out-of-range indices clamp ("abc"[0:10] → "abc", never an error), and
// inverted ranges yield "". Indexing is byte-based (like split) - fine for
// hex digests and ASCII identifiers. The output is a Go substring of the
// source: contiguous and pinning-safe (see makeStringConcatStep contract
// above).
//
// Non-structural: missing/non-string source follows the normal per-step
// error flow → OutcomePartial.
//
// In: cfg sliceFieldConfig - validated config
// Out: TransformationStep - writes the substring to cfg.target
// Ex: makeSliceFieldStep({source: "stream_id_hashed", start: 0, end: 4, target: "shard"}) → step
func makeSliceFieldStep(cfg sliceFieldConfig) TransformationStep {
	return func(state *ParseState) error {
		s, err := requireStringSource(cfg.lookupSource, cfg.source, "slice", state.Fields)
		if err != nil {
			return err
		}

		lo := clampSliceIndex(cfg.start, len(s))

		hi := len(s)
		if cfg.hasEnd {
			hi = clampSliceIndex(cfg.end, len(s))
		}

		if lo >= hi {
			state.Fields[cfg.target] = ""

			return nil
		}

		state.Fields[cfg.target] = s[lo:hi]

		return nil
	}
}

// strFieldConfig: compile-time configuration of a compute_field str.
type strFieldConfig struct {
	source       string // original dot-path, for error messages
	lookupSource func(fields map[string]interface{}) (interface{}, bool)
	target       string
}

// parseStrConfig validates compute_field str options.
// In: config["source"] - input dot-path, compiled here
//
//	target string - already-validated output field name
//
// Out: strFieldConfig - validated config
// Ex: parseStrConfig({"source": "floor"}, "floor_str") → cfg
func parseStrConfig(config map[string]interface{}, target string) (strFieldConfig, error) {
	source, lookupSource, err := compileSourceOption(config, "str")
	if err != nil {
		return strFieldConfig{}, err
	}

	return strFieldConfig{source: source, lookupSource: lookupSource, target: target}, nil
}

// makeStrFieldStep creates a compute_field str step: strictly-typed
// conversion to string via strconv - the single sanctioned escape hatch
// that keeps every other operation strict. Accepts exactly int64, float64,
// bool, and string; anything else is a per-message parsing error. strconv
// results are fresh contiguous allocations, pinning-safe (see
// makeStringConcatStep contract above).
//
// Non-structural: missing/unsupported source follows the normal per-step
// error flow → OutcomePartial.
//
// In: cfg strFieldConfig - validated config
// Out: TransformationStep - writes the string form to cfg.target
// Ex: makeStrFieldStep({source: "floor", target: "floor_str"}) → step
func makeStrFieldStep(cfg strFieldConfig) TransformationStep {
	return func(state *ParseState) error {
		value, exists := cfg.lookupSource(state.Fields)
		if !exists {
			return connectorErrors.NewParsingFailedError("yaml_parser",
				fmt.Errorf("field '%s' not found in compute_field str step", cfg.source))
		}

		switch v := value.(type) {
		case string:
			state.Fields[cfg.target] = v
		case int64:
			state.Fields[cfg.target] = strconv.FormatInt(v, 10)
		case float64:
			state.Fields[cfg.target] = strconv.FormatFloat(v, 'g', -1, 64)
		case bool:
			state.Fields[cfg.target] = strconv.FormatBool(v)
		default:
			return connectorErrors.NewParsingFailedError("yaml_parser",
				fmt.Errorf("compute_field str source '%s' has unsupported type %T", cfg.source, value))
		}

		return nil
	}
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
		// SAFE: fmt.Sprintf creates contiguous memory strings compatible with runtime.Pinner
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

// isEpochFormat reports whether format is a unix epoch keyword.
// In: format string - format name
// Out: bool - true for unix, unix_ms, unix_us, unix_ns
// Ex: isEpochFormat("unix_ms") → true
func isEpochFormat(format string) bool {
	switch format {
	case "unix", "unix_ms", "unix_us", "unix_ns":
		return true
	default:
		return false
	}
}

// epochIntToTime converts an integer epoch in the format's unit.
// In: v int64 - epoch count; format - epoch keyword (see isEpochFormat)
// Out: time.Time - converted timestamp
// Ex: epochIntToTime(1704110400000, "unix_ms") → 2024-01-01T12:00:00Z
func epochIntToTime(v int64, format string) time.Time {
	switch format {
	case "unix_ms":
		return time.UnixMilli(v)
	case "unix_us":
		return time.UnixMicro(v)
	case "unix_ns":
		return time.Unix(0, v)
	default: // "unix" - epoch seconds
		return time.Unix(v, 0)
	}
}

// epochFloatToTime converts a fractional epoch, preserving the sub-unit
// fraction as nanoseconds. Computed in int64 nanoseconds, so the representable
// range is that of time.Unix(0, ns) (years 1678-2262).
// In: v float64 - epoch count with fraction; format - epoch keyword (see isEpochFormat)
// Out: time.Time - converted timestamp
// Ex: epochFloatToTime(1704110400.5, "unix") → 2024-01-01T12:00:00.5Z
func epochFloatToTime(v float64, format string) time.Time {
	var unitNanos float64
	switch format {
	case "unix_ms":
		unitNanos = 1e6
	case "unix_us":
		unitNanos = 1e3
	case "unix_ns":
		unitNanos = 1
	default: // "unix" - epoch seconds
		unitNanos = 1e9
	}

	whole, frac := math.Modf(v)

	return time.Unix(0, int64(whole)*int64(unitNanos)+int64(math.Round(frac*unitNanos)))
}

// parseTimestampString parses a timestamp string per format.
// Epoch keywords accept integer and fractional numeric strings; any other
// format is a date format: rfc3339, rfc3339nano, or a custom Go time layout.
// In: value, format - timestamp string and format name
// Out: time.Time - parsed timestamp
// Ex: parseTimestampString("1704110400.5", "unix") → 2024-01-01T12:00:00.5Z
// Per ADR-005: handles various timestamp formats
func parseTimestampString(value, format string) (time.Time, error) {
	if isEpochFormat(format) {
		i, intErr := strconv.ParseInt(value, 10, 64)
		if intErr == nil {
			return epochIntToTime(i, format), nil
		}

		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return time.Time{}, err
		}

		return epochFloatToTime(f, format), nil
	}

	switch format {
	case "rfc3339":
		return time.Parse(time.RFC3339, value)
	case "rfc3339nano":
		return time.Parse(time.RFC3339Nano, value)
	default:
		// Custom format - use Go time layout per ADR-005
		return time.Parse(format, value)
	}
}

// convertToTimestamp converts a pipeline field value to time.Time per format.
// Epoch keywords (unix, unix_ms, unix_us, unix_ns) apply to numeric input and
// numeric strings; date formats (rfc3339, rfc3339nano, custom Go layout) apply
// to strings only. Numeric input with a date format is an error, never an
// implicit epoch: silently guessing the unit would corrupt data by orders of
// magnitude (a millisecond epoch read as seconds lands in year 55978).
// In: value - string|int64|float64|time.Time from state.Fields
//
//	format - "rfc3339"|"rfc3339nano"|"unix"|"unix_ms"|"unix_us"|"unix_ns"|Go layout
//
// Out: time.Time, error - converted timestamp
// Ex: convertToTimestamp(1704110400.5, "unix") → 2024-01-01T12:00:00.5Z
func convertToTimestamp(value interface{}, format string) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case string:
		return parseTimestampString(v, format)
	case int64:
		if !isEpochFormat(format) {
			return time.Time{}, fmt.Errorf("numeric timestamp requires an epoch format (unix, unix_ms, unix_us, unix_ns), got '%s'", format)
		}

		return epochIntToTime(v, format), nil
	case float64:
		if !isEpochFormat(format) {
			return time.Time{}, fmt.Errorf("numeric timestamp requires an epoch format (unix, unix_ms, unix_us, unix_ns), got '%s'", format)
		}

		return epochFloatToTime(v, format), nil
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to timestamp", value)
	}
}
