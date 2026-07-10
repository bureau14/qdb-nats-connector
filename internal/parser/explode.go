// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package parser: terminal 1->N row materialization (explode step)
// Types: explodeSpec, intervalMode
// Ex: splitExplodeSpec(specs) → scalar pipeline + compiled explode config
//
// explode is the single cardinality-changing primitive (ADR-012): the
// transformation language stays 100% scalar, arrays are just typed field
// values, and one N-row WriterTable materializes per message. It is NOT a
// TransformationStep and never enters the pipeline: it must be the last
// spec, is compiled into an explodeSpec on the parser, and Parse dispatches
// on it at row materialization.
package parser

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
)

// intervalMode selects how the explode time-axis interval is sourced.
// There is deliberately NO default (unlike extract_table's $table source):
// a fabricated time axis is the worst failure class in a tsdb.
type intervalMode int

const (
	intervalModeValue    intervalMode = iota // static Go duration from config
	intervalModeSource                       // per-message field + mandatory unit
	intervalModeByLength                     // array length -> duration map
)

// explodeSpec is the compiled terminal explode configuration.
type explodeSpec struct {
	sourcePath   string // array field dot-path, for error messages
	lookupSource func(fields map[string]interface{}) (interface{}, bool)
	startPath    string // time.Time field dot-path (e.g. via extract_timestamp)
	lookupStart  func(fields map[string]interface{}) (interface{}, bool)

	mode             intervalMode
	intervalValue    time.Duration // mode Value; validated > 0 at load
	intervalPath     string        // mode Source
	intervalLookup   func(fields map[string]interface{}) (interface{}, bool)
	intervalUnit     time.Duration         // mode Source: ns|us|ms|s multiplier
	intervalByLength map[int]time.Duration // mode ByLength; values > 0 at load

	targetCol   int // output column index bound per-element
	targetName  string
	targetType  qdb.TsColumnType // TsColumnDouble or TsColumnInt64
	ordinalCol  int              // 0-based int64 ordinal column; -1 when not configured
	ordinalName string           // "" when not configured
}

// explodedColumnNames returns the per-sample output column names (target +
// ordinal); nil-safe for scalar configs. Filters must reject specs
// referencing these: RowFilter.Apply evaluates row 0 only.
func explodedColumnNames(e *explodeSpec) []string {
	if e == nil {
		return nil
	}

	names := []string{e.targetName}
	if e.ordinalName != "" {
		names = append(names, e.ordinalName)
	}

	return names
}

// splitExplodeSpec removes the terminal explode spec from the
// transformation list, so compilePipeline never sees it. Explode must be
// the last step (compile-time enforced: no step can observe plural state)
// and at most one may appear.
// In: specs []TransformSpec - raw transformation list
// Out: scalar steps, explode config (nil when absent)
// Ex: splitExplodeSpec([...,{explode}]) → [...], explodeCfg, nil
func splitExplodeSpec(specs []TransformSpec) ([]TransformSpec, map[string]interface{}, error) {
	count := 0
	for _, spec := range specs {
		if spec.GetStepName() == "explode" {
			count++
		}
	}

	if count == 0 {
		return specs, nil, nil
	}

	if count > 1 {
		return nil, nil, connectorErrors.NewInvalidConfigError("yaml_parser",
			"at most one explode step is allowed per config")
	}

	last := len(specs) - 1
	if specs[last].GetStepName() != "explode" {
		return nil, nil, connectorErrors.NewInvalidConfigError("yaml_parser",
			"explode must be the last transformation step")
	}

	return specs[:last], specs[last].Config, nil
}

// columnIndex returns the schema index of the named output column, -1 when
// absent.
func columnIndex(schema []ColumnSchema, name string) int {
	for i, col := range schema {
		if col.Name == name {
			return i
		}
	}

	return -1
}

// explodeIntervalUnits maps the mandatory unit key of a sourced interval to
// its nanosecond multiplier. A unitless source is a compile-time error:
// wrong-unit x 8192 samples = years-off but plausible-looking timestamps.
var explodeIntervalUnits = map[string]time.Duration{
	"ns": time.Nanosecond,
	"us": time.Microsecond,
	"ms": time.Millisecond,
	"s":  time.Second,
}

// explodeDurationValue parses a config duration literal. Durations are
// inherently integer-nanosecond quantized (time.Duration): exact 3 kHz is
// not representable and must be approximated, e.g. "333333ns" (ADR-012).
func explodeDurationValue(context string, raw interface{}) (time.Duration, error) {
	s, ok := raw.(string)
	if !ok {
		return 0, fmt.Errorf("%s must be a positive Go duration string, got %T", context, raw)
	}

	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive Go duration, got %q", context, s)
	}

	return d, nil
}

// byLengthKey coerces a YAML by_length map key to a positive int array
// length. yaml.v3 delivers integer keys as int (whole-map decode uses
// map[string]interface{} only when ALL keys are strings, else native types),
// quoted keys as string; both forms are accepted.
func byLengthKey(k interface{}) (int, error) {
	switch v := k.(type) {
	case int:
		if v > 0 {
			return v, nil
		}
	case int64:
		if v > 0 {
			return int(v), nil
		}
	case string:
		n, err := strconv.Atoi(v)
		if err == nil && n > 0 {
			return n, nil
		}
	}

	return 0, fmt.Errorf("explode index.interval by_length keys must be positive integers, got %v", k)
}

// parseByLength builds the array-length -> interval map. Accepts both YAML
// map shapes (see byLengthKey).
func parseByLength(raw interface{}) (map[int]time.Duration, error) {
	entries := map[interface{}]interface{}{}

	switch m := raw.(type) {
	case map[interface{}]interface{}:
		entries = m
	case map[string]interface{}:
		for k, v := range m {
			entries[k] = v
		}
	}

	if len(entries) == 0 {
		return nil, fmt.Errorf("explode index.interval by_length must be a non-empty map of array length to duration")
	}

	byLength := make(map[int]time.Duration, len(entries))

	for k, v := range entries {
		n, err := byLengthKey(k)
		if err != nil {
			return nil, err
		}

		d, err := explodeDurationValue(fmt.Sprintf("explode index.interval by_length[%d]", n), v)
		if err != nil {
			return nil, err
		}

		byLength[n] = d
	}

	return byLength, nil
}

// compileExplodeInterval validates the index.interval block: exactly one of
// 'value' (static duration), 'source' (per-message field, mandatory unit),
// or 'by_length' (array length -> duration).
func compileExplodeInterval(raw interface{}, spec *explodeSpec) error {
	m, ok := raw.(map[string]interface{})
	if !ok {
		return fmt.Errorf("explode index.interval must be a map")
	}

	rawValue, hasValue := m["value"]
	rawSource, hasSource := m["source"]
	rawBy, hasBy := m["by_length"]
	rawUnit, hasUnit := m["unit"]

	count := 0
	for _, present := range []bool{hasValue, hasSource, hasBy} {
		if present {
			count++
		}
	}
	if count != 1 {
		return fmt.Errorf("explode index.interval requires exactly one of 'value', 'source', or 'by_length'")
	}

	if hasUnit && !hasSource {
		return fmt.Errorf("explode index.interval 'unit' is only valid with 'source'")
	}

	switch {
	case hasValue:
		d, err := explodeDurationValue("explode index.interval 'value'", rawValue)
		if err != nil {
			return err
		}
		spec.mode, spec.intervalValue = intervalModeValue, d
	case hasSource:
		return compileExplodeIntervalSource(rawSource, rawUnit, hasUnit, spec)
	case hasBy:
		byLength, err := parseByLength(rawBy)
		if err != nil {
			return err
		}
		spec.mode, spec.intervalByLength = intervalModeByLength, byLength
	}

	return nil
}

// compileExplodeIntervalSource validates the sourced-interval variant:
// dot-path field plus mandatory unit.
func compileExplodeIntervalSource(rawSource, rawUnit interface{}, hasUnit bool, spec *explodeSpec) error {
	source, ok := rawSource.(string)
	if !ok || source == "" {
		return fmt.Errorf("explode index.interval 'source' must be a non-empty string")
	}

	lookup, err := compileFieldPath(source)
	if err != nil {
		return fmt.Errorf("explode index.interval 'source' is not a valid field path: %v", err)
	}

	if !hasUnit {
		return fmt.Errorf("explode index.interval 'source' requires 'unit' (ns|us|ms|s)")
	}

	unitName, _ := rawUnit.(string)
	unit, ok := explodeIntervalUnits[unitName]
	if !ok {
		return fmt.Errorf("explode index.interval unit must be one of ns, us, ms, s; got %v", rawUnit)
	}

	spec.mode = intervalModeSource
	spec.intervalPath, spec.intervalLookup, spec.intervalUnit = source, lookup, unit

	return nil
}

// compileExplodeIndex validates the index block: start (dot-path to a
// time.Time field, the capture-window START anchor -- t[0] == start
// exactly) and interval (see compileExplodeInterval).
func compileExplodeIndex(cfg map[string]interface{}, spec *explodeSpec) error {
	rawIndex, ok := cfg["index"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("explode step requires an 'index' map")
	}

	rawStart, ok := rawIndex["start"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("explode index.start requires 'source' (dot-path to a time.Time field)")
	}

	startPath, ok := rawStart["source"].(string)
	if !ok || startPath == "" {
		return fmt.Errorf("explode index.start requires 'source' (dot-path to a time.Time field)")
	}

	lookupStart, err := compileFieldPath(startPath)
	if err != nil {
		return fmt.Errorf("explode index.start 'source' is not a valid field path: %v", err)
	}
	spec.startPath, spec.lookupStart = startPath, lookupStart

	rawInterval, ok := rawIndex["interval"]
	if !ok {
		return fmt.Errorf("explode index.interval requires exactly one of 'value', 'source', or 'by_length'")
	}

	return compileExplodeInterval(rawInterval, spec)
}

// compileExplodeColumns binds target (per-element values) and the optional
// ordinal (0-based sample index) to output columns and validates their
// types.
func compileExplodeColumns(cfg map[string]interface{}, schema []ColumnSchema, columnTypes []qdb.TsColumnType, spec *explodeSpec) error {
	target, ok := cfg["target"].(string)
	if !ok || target == "" {
		return fmt.Errorf("explode step requires a 'target' string")
	}
	if strings.HasPrefix(target, "$") {
		return fmt.Errorf("explode 'target' must not start with '$' (reserved for extract_* steps)")
	}

	idx := columnIndex(schema, target)
	if idx < 0 {
		return fmt.Errorf("explode 'target' must reference an output column, got %q", target)
	}
	if columnTypes[idx] != qdb.TsColumnDouble && columnTypes[idx] != qdb.TsColumnInt64 {
		return fmt.Errorf("explode 'target' column %q must be a double or int64 column, got %s",
			target, schema[idx].Type)
	}
	spec.targetCol, spec.targetName, spec.targetType = idx, target, columnTypes[idx]

	spec.ordinalCol = -1
	rawOrdinal, present := cfg["ordinal"]
	if !present {
		return nil
	}

	ordinal, ok := rawOrdinal.(string)
	if !ok || ordinal == "" || strings.HasPrefix(ordinal, "$") {
		return fmt.Errorf("explode 'ordinal' must be a non-'$' output column name, got %v", rawOrdinal)
	}
	if ordinal == target {
		return fmt.Errorf("explode 'ordinal' and 'target' must be different columns")
	}

	idx = columnIndex(schema, ordinal)
	if idx < 0 {
		return fmt.Errorf("explode 'ordinal' must reference an output column, got %q", ordinal)
	}
	if columnTypes[idx] != qdb.TsColumnInt64 {
		return fmt.Errorf("explode 'ordinal' column %q must be an int64 column, got %s",
			ordinal, schema[idx].Type)
	}
	spec.ordinalCol, spec.ordinalName = idx, ordinal

	return nil
}

// unpackOutputColumnType returns the qdb column type an unpack step config
// produces: double for float element types or whenever scale is set, int64
// otherwise. Raw config reads are safe: compilePipeline already validated
// the step.
func unpackOutputColumnType(cfg map[string]interface{}) qdb.TsColumnType {
	typeName, _ := cfg["type"].(string)
	_, hasScale := cfg["scale"]

	if hasScale || typeName == "float32" || typeName == "float64" {
		return qdb.TsColumnDouble
	}

	return qdb.TsColumnInt64
}

// validateExplodeUnpackTrace statically checks element-vs-column type
// compatibility when the exploded array is produced by an unpack step in
// the same config. The authoritative check remains the per-message type
// assertion; this only surfaces the error at config load where possible.
func validateExplodeUnpackTrace(steps []TransformSpec, spec *explodeSpec) error {
	for _, step := range steps {
		if step.GetStepName() != "unpack" {
			continue
		}

		target, _ := step.Config["target"].(string)
		if target != spec.sourcePath {
			continue
		}

		produced := unpackOutputColumnType(step.Config)
		if produced != spec.targetType {
			return connectorErrors.NewInvalidConfigError("yaml_parser", fmt.Sprintf(
				"explode 'target' column %q is %s but unpack %q produces %s",
				spec.targetName, columnTypeName(spec.targetType), target, columnTypeName(produced)))
		}
	}

	return nil
}

// columnTypeName renders a qdb column type for error messages.
func columnTypeName(t qdb.TsColumnType) string {
	if t == qdb.TsColumnDouble {
		return "double"
	}

	return "int64"
}

// validateArrayBindings enforces the broadcast rule (ADR-012): an
// array-typed field reaching output without an explode binding is a
// config-load error. Enforced where statically knowable: every unpack
// target that names an output column must be the explode source.
func validateArrayBindings(steps []TransformSpec, explode *explodeSpec, schema []ColumnSchema) error {
	for _, step := range steps {
		if step.GetStepName() != "unpack" {
			continue
		}

		target, _ := step.Config["target"].(string)
		if columnIndex(schema, target) < 0 {
			continue
		}

		if explode == nil || explode.sourcePath != target {
			return connectorErrors.NewInvalidConfigError("yaml_parser", fmt.Sprintf(
				"unpack target %q is an output column but no explode binds it; array values cannot be written directly",
				target))
		}
	}

	return nil
}

// compileExplodeSpec validates and compiles the explode config against the
// scalar pipeline and output schema. Returns nil when cfg is nil (scalar
// config).
// In: cfg - the explode step config (from splitExplodeSpec)
//
//	steps - the scalar pipeline specs (explode already removed)
//	schema - output columns; columnTypes - matching qdb types
//
// Out: *explodeSpec - compiled spec, or nil
// Ex: compileExplodeSpec(cfg, steps, schema, types) → spec
func compileExplodeSpec(cfg map[string]interface{}, steps []TransformSpec, schema []ColumnSchema, columnTypes []qdb.TsColumnType) (*explodeSpec, error) {
	if cfg == nil {
		return nil, nil //nolint:nilnil // nil spec IS the scalar-config result, not an error
	}

	if hasStep(steps, "extract_index") {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser",
			"explode and extract_index are mutually exclusive: explode owns the $timestamp index")
	}

	spec := &explodeSpec{}

	source, ok := cfg["source"].(string)
	if !ok || source == "" {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser",
			"explode step requires a 'source' string")
	}

	lookup, err := compileFieldPath(source)
	if err != nil {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("explode 'source' is not a valid field path: %v", err))
	}
	spec.sourcePath, spec.lookupSource = source, lookup

	err = compileExplodeColumns(cfg, schema, columnTypes, spec)
	if err != nil {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser", err.Error())
	}

	err = compileExplodeIndex(cfg, spec)
	if err != nil {
		return nil, connectorErrors.NewInvalidConfigError("yaml_parser", err.Error())
	}

	err = validateExplodeUnpackTrace(steps, spec)
	if err != nil {
		return nil, err
	}

	return spec, nil
}
