// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package parser: fixed-width binary array reinterpretation (unpack step)
// Types: unpackSpec, unpackElemType
// Ex: makeUnpackStep(config) → step decoding packed int16 bytes to []int64
//
// Vocabulary (ADR-012): parse_* steps handle self-describing formats
// (schema in the data: JSON, protobuf); unpack handles formats where the
// schema lives in the config because raw bytes cannot carry it.
package parser

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
)

// unpackElemType describes one fixed-width element type: its byte width,
// whether it is a float, and how to read one element. readInt is set for
// integer types (the []int64 output path); widen is set where the default
// float64-widening via readInt would be wrong (uint64, floats).
type unpackElemType struct {
	width   int
	isFloat bool
	readInt func(binary.ByteOrder, []byte) int64
	widen   func(binary.ByteOrder, []byte) float64
}

// unpackElemTypes maps the config 'type' value to its decoder. Integer
// outputs are []int64: uint64 values above MaxInt64 wrap (two's
// complement), matching the protobuf decoder's convention (see
// protoSingularValue in proto.go). The float64 widening used by the scale
// path preserves the UNSIGNED value for uint64 instead: the declared
// element type, not the output container, defines the element's value.
var unpackElemTypes = map[string]unpackElemType{
	"int8":    {width: 1, readInt: readInt8},
	"uint8":   {width: 1, readInt: readUint8},
	"int16":   {width: 2, readInt: readInt16},
	"uint16":  {width: 2, readInt: readUint16},
	"int32":   {width: 4, readInt: readInt32},
	"uint32":  {width: 4, readInt: readUint32},
	"int64":   {width: 8, readInt: readInt64},
	"uint64":  {width: 8, readInt: readInt64, widen: widenUint64},
	"float32": {width: 4, isFloat: true, widen: widenFloat32},
	"float64": {width: 8, isFloat: true, widen: widenFloat64},
}

// unpackTypeNames lists the supported 'type' values for error messages.
const unpackTypeNames = "int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64"

// The int8/int16/int32/int64 readers reinterpret the unsigned wire value as
// its two's-complement signed counterpart -- that reinterpretation IS the
// decode, so the gosec overflow warnings are the intended behavior.
func readInt8(_ binary.ByteOrder, b []byte) int64 {
	return int64(int8(b[0])) //nolint:gosec // two's-complement decode
}

func readUint8(_ binary.ByteOrder, b []byte) int64 { return int64(b[0]) }

func readInt16(o binary.ByteOrder, b []byte) int64 {
	return int64(int16(o.Uint16(b))) //nolint:gosec // two's-complement decode
}

func readUint16(o binary.ByteOrder, b []byte) int64 { return int64(o.Uint16(b)) }

func readInt32(o binary.ByteOrder, b []byte) int64 {
	return int64(int32(o.Uint32(b))) //nolint:gosec // two's-complement decode
}

func readUint32(o binary.ByteOrder, b []byte) int64 { return int64(o.Uint32(b)) }

func readInt64(o binary.ByteOrder, b []byte) int64 {
	return int64(o.Uint64(b)) //nolint:gosec // two's-complement decode; uint64 wrap documented above
}

func widenUint64(o binary.ByteOrder, b []byte) float64 { return float64(o.Uint64(b)) }
func widenFloat32(o binary.ByteOrder, b []byte) float64 {
	return float64(math.Float32frombits(o.Uint32(b)))
}

func widenFloat64(o binary.ByteOrder, b []byte) float64 {
	return math.Float64frombits(o.Uint64(b))
}

// widenOf returns the float64-widening reader for an element type; integer
// types without an explicit widen derive it from readInt (exact: every
// integer type except uint64 fits int64, and uint64 sets widen itself).
func widenOf(elem unpackElemType) func(binary.ByteOrder, []byte) float64 {
	if elem.widen != nil {
		return elem.widen
	}

	readInt := elem.readInt

	return func(o binary.ByteOrder, b []byte) float64 { return float64(readInt(o, b)) }
}

// unpackSpec: compile-time configuration of an unpack step.
type unpackSpec struct {
	source       string // original dot-path, for error messages
	lookupSource func(fields map[string]interface{}) (interface{}, bool)
	typeName     string
	elem         unpackElemType
	widen        func(binary.ByteOrder, []byte) float64
	order        binary.ByteOrder
	target       string
	hasScale     bool
	scaleSource  string // "" when literal
	scaleLookup  func(fields map[string]interface{}) (interface{}, bool)
	scaleValue   float64 // literal mode; finite and non-zero, validated at load
}

// validateScale rejects NaN, Inf, and 0.0 scales. A NaN scale would write N
// doubles that ARE QuasarDB's double-null sentinel (QDB_IS_NULL_DOUBLE ->
// isnan) -- silent total data loss no other layer can catch. 0.0 is
// protobuf's absent-field default, the likeliest real corruption; captured
// SKF waveforms contain zero legitimate 0.0 scales (ADR-012).
func validateScale(s float64) error {
	if math.IsNaN(s) || math.IsInf(s, 0) || s == 0.0 {
		return fmt.Errorf("scale must be finite and non-zero, got %v", s)
	}

	return nil
}

// unpackScaleLiteral coerces a YAML scale literal (int or float64) and
// validates it.
// In: raw interface{} - config scale["value"]
// Out: float64 - validated scale
// Ex: unpackScaleLiteral(0.5) → 0.5
func unpackScaleLiteral(raw interface{}) (float64, error) {
	var s float64

	switch v := raw.(type) {
	case float64:
		s = v
	case int:
		s = float64(v)
	default:
		return 0, fmt.Errorf("unpack scale 'value' must be a number, got %T", raw)
	}

	err := validateScale(s)
	if err != nil {
		return 0, fmt.Errorf("unpack scale 'value': %v", err)
	}

	return s, nil
}

// parseUnpackScale validates the optional scale block: a map with exactly
// one of 'value' (numeric literal) or 'source' (dot-path to a float64).
// In: config map - raw step config; spec - config being built
// Out: error - InvalidConfig on malformed scale
// Ex: parseUnpackScale({"scale": {"source": "attr.scale"}}, &spec) → nil
func parseUnpackScale(config map[string]interface{}, spec *unpackSpec) error {
	raw, present := config["scale"]
	if !present {
		return nil
	}

	m, ok := raw.(map[string]interface{})
	rawValue, hasValue := m["value"]
	rawSource, hasSource := m["source"]

	if !ok || hasValue == hasSource {
		return connectorErrors.NewInvalidConfigError("yaml_parser",
			"unpack 'scale' must be a map with exactly one of 'value' or 'source'")
	}

	if hasValue {
		s, err := unpackScaleLiteral(rawValue)
		if err != nil {
			return connectorErrors.NewInvalidConfigError("yaml_parser", err.Error())
		}

		spec.scaleValue = s
	} else {
		source, ok := rawSource.(string)
		if !ok || source == "" {
			return connectorErrors.NewInvalidConfigError("yaml_parser",
				"unpack scale 'source' must be a non-empty string")
		}

		lookup, err := compileFieldPath(source)
		if err != nil {
			return connectorErrors.NewInvalidConfigError("yaml_parser",
				fmt.Sprintf("unpack scale 'source' is not a valid field path: %v", err))
		}

		spec.scaleSource = source
		spec.scaleLookup = lookup
	}

	spec.hasScale = true

	return nil
}

// parseUnpackEndianness resolves the optional endianness key. Omitted means
// the host machine's byte order (binary.NativeEndian): convenient for
// same-host producer/consumer pairs. Cross-machine wire formats should
// declare it explicitly -- the byte order is out-of-band knowledge the
// bytes cannot carry.
// In: config map - raw step config
// Out: binary.ByteOrder - resolved byte order
// Ex: parseUnpackEndianness({"endianness": "little"}) → binary.LittleEndian
func parseUnpackEndianness(config map[string]interface{}) (binary.ByteOrder, error) {
	raw, present := config["endianness"]
	if !present {
		return binary.NativeEndian, nil
	}

	s, _ := raw.(string)
	switch s {
	case "little":
		return binary.LittleEndian, nil
	case "big":
		return binary.BigEndian, nil
	}

	return nil, connectorErrors.NewInvalidConfigError("yaml_parser",
		fmt.Sprintf("unpack 'endianness' must be \"little\" or \"big\", got %v", raw))
}

// parseUnpackConfig validates unpack step config.
// In: config["source"] - dot-path to a []byte field
//
//	config["type"] - element type (see unpackTypeNames)
//	config["endianness"] - optional "little"|"big" (default: host order)
//	config["target"] - output field name (flat, no '$' prefix)
//	config["scale"] - optional map, see parseUnpackScale
//
// Out: unpackSpec - validated config
// Ex: parseUnpackConfig({"source": "attr.samples", "type": "int16", ...}) → spec
func parseUnpackConfig(config map[string]interface{}) (unpackSpec, error) {
	spec := unpackSpec{}

	for key, dst := range map[string]*string{
		"source": &spec.source,
		"type":   &spec.typeName,
		"target": &spec.target,
	} {
		value, ok := config[key].(string)
		if !ok || value == "" {
			return unpackSpec{}, connectorErrors.NewInvalidConfigError("yaml_parser",
				fmt.Sprintf("unpack step requires a '%s' string option", key))
		}

		*dst = value
	}

	if strings.HasPrefix(spec.target, "$") {
		return unpackSpec{}, connectorErrors.NewInvalidConfigError("yaml_parser",
			"unpack 'target' must not start with '$' (reserved for extract_* steps)")
	}

	elem, ok := unpackElemTypes[spec.typeName]
	if !ok {
		return unpackSpec{}, connectorErrors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("unpack 'type' must be one of %s; got %q", unpackTypeNames, spec.typeName))
	}
	spec.elem = elem
	spec.widen = widenOf(elem)

	lookup, err := compileFieldPath(spec.source)
	if err != nil {
		return unpackSpec{}, connectorErrors.NewInvalidConfigError("yaml_parser",
			fmt.Sprintf("unpack 'source' is not a valid field path: %v", err))
	}
	spec.lookupSource = lookup

	spec.order, err = parseUnpackEndianness(config)
	if err != nil {
		return unpackSpec{}, err
	}

	err = parseUnpackScale(config, &spec)
	if err != nil {
		return unpackSpec{}, err
	}

	return spec, nil
}

// unpackInts decodes packed integers to a fresh []int64 (contiguous
// allocation, pinning-safe).
func unpackInts(raw []byte, order binary.ByteOrder, read func(binary.ByteOrder, []byte) int64, width int) []int64 {
	out := make([]int64, len(raw)/width)
	for i := range out {
		out[i] = read(order, raw[i*width:])
	}

	return out
}

// unpackFloats decodes packed floats to a fresh []float64.
func unpackFloats(raw []byte, order binary.ByteOrder, widen func(binary.ByteOrder, []byte) float64, width int) []float64 {
	out := make([]float64, len(raw)/width)
	for i := range out {
		out[i] = widen(order, raw[i*width:])
	}

	return out
}

// unpackScaled decodes and calibrates in one pass. Arithmetic contract
// (golden diffs against reference decoders rely on the exact order): widen
// the element to float64 FIRST, then multiply by the float64 scale.
func unpackScaled(raw []byte, order binary.ByteOrder, widen func(binary.ByteOrder, []byte) float64, width int, scale float64) []float64 {
	out := make([]float64, len(raw)/width)
	for i := range out {
		out[i] = widen(order, raw[i*width:]) * scale
	}

	return out
}

// resolveScale returns the per-message scale: the validated literal, or the
// looked-up source field re-validated per message (a NaN/Inf/0.0 sourced
// scale is a structural failure -- see validateScale).
func (u *unpackSpec) resolveScale(fields map[string]interface{}) (float64, error) {
	if u.scaleLookup == nil {
		return u.scaleValue, nil
	}

	value, ok := u.scaleLookup(fields)
	if !ok {
		return 0, connectorErrors.NewParsingFailedError("yaml_parser",
			fmt.Errorf("scale field '%s' not found in unpack step", u.scaleSource))
	}

	s, ok := value.(float64)
	if !ok {
		return 0, connectorErrors.NewParsingFailedError("yaml_parser",
			fmt.Errorf("unpack scale '%s' has type %T, want float64", u.scaleSource, value))
	}

	err := validateScale(s)
	if err != nil {
		return 0, connectorErrors.NewParsingFailedError("yaml_parser",
			fmt.Errorf("unpack scale '%s': %v", u.scaleSource, err))
	}

	return s, nil
}

// run executes the unpack step: reinterpret the []byte source as a typed
// numeric array. Output type: []int64 for integer types without scale;
// []float64 for float types or whenever scale is set. Empty input yields an
// empty array (explode's zero-row path), not an error.
func (u *unpackSpec) run(state *ParseState) error {
	value, ok := u.lookupSource(state.Fields)
	if !ok {
		return connectorErrors.NewParsingFailedError("yaml_parser",
			fmt.Errorf("source field '%s' not found in unpack step", u.source))
	}

	raw, ok := value.([]byte)
	if !ok {
		return connectorErrors.NewParsingFailedError("yaml_parser",
			fmt.Errorf("unpack source '%s' has type %T, want []byte", u.source, value))
	}

	if len(raw)%u.elem.width != 0 {
		return connectorErrors.NewParsingFailedError("yaml_parser",
			fmt.Errorf("unpack source '%s' byte length %d is not a multiple of %s width %d",
				u.source, len(raw), u.typeName, u.elem.width))
	}

	if u.hasScale {
		scale, err := u.resolveScale(state.Fields)
		if err != nil {
			return err
		}

		state.Fields[u.target] = unpackScaled(raw, u.order, u.widen, u.elem.width, scale)

		return nil
	}

	if u.elem.isFloat {
		state.Fields[u.target] = unpackFloats(raw, u.order, u.widen, u.elem.width)
	} else {
		state.Fields[u.target] = unpackInts(raw, u.order, u.elem.readInt, u.elem.width)
	}

	return nil
}

// makeUnpackStep creates a step reinterpreting a []byte field as a typed
// numeric array (fixed-width binary "unpack", the cross-language term of
// art: Python struct.unpack, Erlang bit syntax).
//
// unpack is STRUCTURAL: decode is all-or-nothing (missing/mistyped source,
// truncated byte length, non-finite or zero scale) → OutcomeUnusable, zero
// tables. A partially-decoded sample array has no meaningful sentinel form.
//
// In: config - see parseUnpackConfig
// Out: TransformationStep - writes the typed array into state.Fields
// Ex: makeUnpackStep({"source": "attr.samples", "type": "int16", "endianness": "little", "scale": map[...], "target": "samples"}) → step
func makeUnpackStep(config map[string]interface{}) (TransformationStep, error) {
	spec, err := parseUnpackConfig(config)
	if err != nil {
		return nil, err
	}

	return spec.run, nil
}
