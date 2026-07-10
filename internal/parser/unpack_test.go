// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package parser: unpack step tests
// Ex: TestUnpackGoldenVectors → known bytes decode to known arrays
package parser

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// unpackTestTypeNames: every supported unpack element type.
var unpackTestTypeNames = []string{
	"int8", "int16", "int32", "int64",
	"uint8", "uint16", "uint32", "uint64",
	"float32", "float64",
}

// runUnpackStep compiles an unpack step and runs it over the given fields.
// Returns the step error; decoded output lands in fields[target].
func runUnpackStep(t *testing.T, config, fields map[string]interface{}) error {
	t.Helper()

	step, err := makeUnpackStep(config)
	require.NoError(t, err)

	return step(&ParseState{Fields: fields})
}

// unpackConfig builds a minimal unpack step config.
func unpackConfig(typeName, endianness string) map[string]interface{} {
	config := map[string]interface{}{
		"source": "raw",
		"type":   typeName,
		"target": "out",
	}
	if endianness != "" {
		config["endianness"] = endianness
	}

	return config
}

// referenceDecode decodes raw via encoding/binary.Read into the named
// element type and widens every element to float64 -- the stdlib reference
// the unpack implementation must match. uint64 widens as its UNSIGNED value.
func referenceDecode(t *testing.T, raw []byte, typeName string, order binary.ByteOrder) []float64 {
	t.Helper()

	width := unpackElemTypes[typeName].width
	n := len(raw) / width
	out := make([]float64, n)
	r := bytes.NewReader(raw)

	decode := func(dst interface{}, widen func(i int) float64) {
		require.NoError(t, binary.Read(r, order, dst))
		for i := range n {
			out[i] = widen(i)
		}
	}

	switch typeName {
	case "int8":
		v := make([]int8, n)
		decode(&v, func(i int) float64 { return float64(v[i]) })
	case "uint8":
		v := make([]uint8, n)
		decode(&v, func(i int) float64 { return float64(v[i]) })
	case "int16":
		v := make([]int16, n)
		decode(&v, func(i int) float64 { return float64(v[i]) })
	case "uint16":
		v := make([]uint16, n)
		decode(&v, func(i int) float64 { return float64(v[i]) })
	case "int32":
		v := make([]int32, n)
		decode(&v, func(i int) float64 { return float64(v[i]) })
	case "uint32":
		v := make([]uint32, n)
		decode(&v, func(i int) float64 { return float64(v[i]) })
	case "int64":
		v := make([]int64, n)
		decode(&v, func(i int) float64 { return float64(v[i]) })
	case "uint64":
		v := make([]uint64, n)
		decode(&v, func(i int) float64 { return float64(v[i]) })
	case "float32":
		v := make([]float32, n)
		decode(&v, func(i int) float64 { return float64(v[i]) })
	case "float64":
		v := make([]float64, n)
		decode(&v, func(i int) float64 { return v[i] })
	}

	return out
}

// TestUnpackConfigErrors validates compile-time config rejection.
func TestUnpackConfigErrors(t *testing.T) {
	cases := []struct {
		name   string
		config map[string]interface{}
		want   string
	}{
		{
			"missing source",
			map[string]interface{}{"type": "int16", "target": "out"},
			"requires a 'source'",
		},
		{
			"missing type",
			map[string]interface{}{"source": "raw", "target": "out"},
			"requires a 'type'",
		},
		{
			"missing target",
			map[string]interface{}{"source": "raw", "type": "int16"},
			"requires a 'target'",
		},
		{
			"unknown type",
			map[string]interface{}{"source": "raw", "type": "int24", "target": "out"},
			"'type' must be one of",
		},
		{
			"dollar target",
			map[string]interface{}{"source": "raw", "type": "int16", "target": "$out"},
			"must not start with '$'",
		},
		{
			"bad source path",
			map[string]interface{}{"source": "a..b", "type": "int16", "target": "out"},
			"not a valid field path",
		},
		{"bad endianness", map[string]interface{}{
			"source": "raw", "type": "int16", "target": "out",
			"endianness": "middle",
		}, "'endianness' must be"},
		{"non-string endianness", map[string]interface{}{
			"source": "raw", "type": "int16", "target": "out",
			"endianness": 3,
		}, "'endianness' must be"},
		{"scale not a map", map[string]interface{}{
			"source": "raw", "type": "int16", "target": "out",
			"scale": "x",
		}, "exactly one of 'value' or 'source'"},
		{"scale empty", map[string]interface{}{
			"source": "raw", "type": "int16", "target": "out",
			"scale": map[string]interface{}{},
		}, "exactly one of 'value' or 'source'"},
		{
			"scale both",
			map[string]interface{}{
				"source": "raw", "type": "int16", "target": "out",
				"scale": map[string]interface{}{"value": 1.0, "source": "s"},
			},
			"exactly one of 'value' or 'source'",
		},
		{"scale value non-numeric", map[string]interface{}{
			"source": "raw", "type": "int16", "target": "out",
			"scale": map[string]interface{}{"value": "abc"},
		}, "must be a number"},
		{"scale value zero", map[string]interface{}{
			"source": "raw", "type": "int16", "target": "out",
			"scale": map[string]interface{}{"value": 0.0},
		}, "finite and non-zero"},
		{"scale value nan", map[string]interface{}{
			"source": "raw", "type": "int16", "target": "out",
			"scale": map[string]interface{}{"value": math.NaN()},
		}, "finite and non-zero"},
		{"scale value inf", map[string]interface{}{
			"source": "raw", "type": "int16", "target": "out",
			"scale": map[string]interface{}{"value": math.Inf(1)},
		}, "finite and non-zero"},
		{"scale source empty", map[string]interface{}{
			"source": "raw", "type": "int16", "target": "out",
			"scale": map[string]interface{}{"source": ""},
		}, "must be a non-empty string"},
		{"scale source bad path", map[string]interface{}{
			"source": "raw", "type": "int16", "target": "out",
			"scale": map[string]interface{}{"source": "a..b"},
		}, "not a valid field path"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			step, err := makeUnpackStep(tc.config)
			assert.Nil(t, step)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}

	t.Run("endianness omitted defaults to host order", func(t *testing.T) {
		step, err := makeUnpackStep(unpackConfig("int16", ""))
		require.NoError(t, err)
		assert.NotNil(t, step)
	})

	t.Run("unpack is structural", func(t *testing.T) {
		assert.True(t, isStructuralStep("unpack"))
	})
}

// TestUnpackGoldenVectors validates known bytes decode to known arrays,
// covering both endiannesses, integer edge values, uint64 wrap, and the
// widen-then-multiply scale contract (Float64bits-exact).
func TestUnpackGoldenVectors(t *testing.T) {
	intCases := []struct {
		name       string
		typeName   string
		endianness string
		raw        []byte
		want       []int64
	}{
		{"int8 edges", "int8", "little", []byte{0x80, 0x7F, 0xFF, 0x00}, []int64{-128, 127, -1, 0}},
		{"uint8 edges", "uint8", "little", []byte{0xFF, 0x00, 0x80}, []int64{255, 0, 128}},
		{
			"int16 little", "int16", "little",
			[]byte{0x01, 0x00, 0xFF, 0xFF, 0x00, 0x80, 0xFF, 0x7F},
			[]int64{1, -1, -32768, 32767},
		},
		{
			"int16 big", "int16", "big",
			[]byte{0x00, 0x01, 0xFF, 0xFF, 0x80, 0x00, 0x7F, 0xFF},
			[]int64{1, -1, -32768, 32767},
		},
		{"uint16 max", "uint16", "little", []byte{0xFF, 0xFF}, []int64{65535}},
		{"int32 min", "int32", "little", []byte{0x00, 0x00, 0x00, 0x80}, []int64{math.MinInt32}},
		{"int32 big max", "int32", "big", []byte{0x7F, 0xFF, 0xFF, 0xFF}, []int64{math.MaxInt32}},
		{"uint32 max", "uint32", "little", []byte{0xFF, 0xFF, 0xFF, 0xFF}, []int64{math.MaxUint32}},
		{
			"int64 min", "int64", "little",
			[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80},
			[]int64{math.MinInt64},
		},
		{
			"uint64 wraps above MaxInt64", "uint64", "little",
			[]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			[]int64{-1},
		},
	}

	for _, tc := range intCases {
		t.Run(tc.name, func(t *testing.T) {
			fields := map[string]interface{}{"raw": tc.raw}
			require.NoError(t, runUnpackStep(t, unpackConfig(tc.typeName, tc.endianness), fields))
			assert.Equal(t, tc.want, fields["out"])
		})
	}

	t.Run("float32 little", func(t *testing.T) {
		raw := binary.LittleEndian.AppendUint32(nil, math.Float32bits(1.5))
		raw = binary.LittleEndian.AppendUint32(raw, math.Float32bits(-0.25))
		fields := map[string]interface{}{"raw": raw}
		require.NoError(t, runUnpackStep(t, unpackConfig("float32", "little"), fields))
		assert.Equal(t, []float64{1.5, -0.25}, fields["out"])
	})

	t.Run("float64 big", func(t *testing.T) {
		raw := binary.BigEndian.AppendUint64(nil, math.Float64bits(-2.25))
		fields := map[string]interface{}{"raw": raw}
		require.NoError(t, runUnpackStep(t, unpackConfig("float64", "big"), fields))
		assert.Equal(t, []float64{-2.25}, fields["out"])
	})

	t.Run("scaled int16 literal", func(t *testing.T) {
		config := unpackConfig("int16", "little")
		config["scale"] = map[string]interface{}{"value": 0.5}
		fields := map[string]interface{}{"raw": []byte{0x02, 0x00, 0xFF, 0xFF, 0x00, 0x80}}
		require.NoError(t, runUnpackStep(t, config, fields))
		assert.Equal(t, []float64{1.0, -0.5, -16384.0}, fields["out"])
	})

	t.Run("scaled int16 from source", func(t *testing.T) {
		config := unpackConfig("int16", "little")
		config["scale"] = map[string]interface{}{"source": "attr.scale"}
		scale := float64(float32(9.28e-4)) // f32 wire value widened, per proto.go
		fields := map[string]interface{}{
			"raw":  []byte{0x01, 0x00, 0xFF, 0x7F},
			"attr": map[string]interface{}{"scale": scale},
		}
		require.NoError(t, runUnpackStep(t, config, fields))
		want := []float64{1.0 * scale, 32767.0 * scale}
		got, ok := fields["out"].([]float64)
		require.True(t, ok)
		require.Len(t, got, 2)
		for i := range want {
			assert.Equal(t, math.Float64bits(want[i]), math.Float64bits(got[i]), "index %d", i)
		}
	})

	t.Run("scaled uint64 widens unsigned", func(t *testing.T) {
		config := unpackConfig("uint64", "little")
		config["scale"] = map[string]interface{}{"value": 1.0}
		fields := map[string]interface{}{
			"raw": []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
		}
		require.NoError(t, runUnpackStep(t, config, fields))
		assert.Equal(t, []float64{float64(uint64(math.MaxUint64))}, fields["out"])
	})

	t.Run("empty bytes yield empty arrays", func(t *testing.T) {
		fields := map[string]interface{}{"raw": []byte{}}
		require.NoError(t, runUnpackStep(t, unpackConfig("int32", "little"), fields))
		assert.Equal(t, []int64{}, fields["out"])

		config := unpackConfig("int32", "little")
		config["scale"] = map[string]interface{}{"value": 2.0}
		require.NoError(t, runUnpackStep(t, config, fields))
		assert.Equal(t, []float64{}, fields["out"])
	})
}

// TestUnpackTruncatedLengthUnusable: byte lengths that are not a multiple
// of the element width are structural failures for every multi-byte width.
func TestUnpackTruncatedLengthUnusable(t *testing.T) {
	for _, typeName := range []string{"int16", "int32", "int64", "float32", "float64"} {
		t.Run(typeName, func(t *testing.T) {
			width := unpackElemTypes[typeName].width
			fields := map[string]interface{}{"raw": make([]byte, width+1)}
			err := runUnpackStep(t, unpackConfig(typeName, "little"), fields)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not a multiple")
			assert.NotContains(t, fields, "out")
		})
	}
}

// TestUnpackScaleStructuralFailures: sourced scales that are missing,
// mistyped, NaN, Inf, or 0.0 are per-message structural failures.
func TestUnpackScaleStructuralFailures(t *testing.T) {
	config := unpackConfig("int16", "little")
	config["scale"] = map[string]interface{}{"source": "scale"}

	cases := []struct {
		name   string
		fields map[string]interface{}
		want   string
	}{
		{"missing", map[string]interface{}{}, "scale field 'scale' not found"},
		{"mistyped", map[string]interface{}{"scale": "0.5"}, "want float64"},
		{"nan", map[string]interface{}{"scale": math.NaN()}, "finite and non-zero"},
		{"pos inf", map[string]interface{}{"scale": math.Inf(1)}, "finite and non-zero"},
		{"neg inf", map[string]interface{}{"scale": math.Inf(-1)}, "finite and non-zero"},
		{"zero", map[string]interface{}{"scale": 0.0}, "finite and non-zero"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.fields["raw"] = []byte{0x01, 0x00}
			err := runUnpackStep(t, config, tc.fields)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
			assert.NotContains(t, tc.fields, "out")
		})
	}
}

// TestUnpackMissingOrMistypedSourceUnusable: absent or non-[]byte sources
// are structural failures.
func TestUnpackMissingOrMistypedSourceUnusable(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		err := runUnpackStep(t, unpackConfig("int16", "little"), map[string]interface{}{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "source field 'raw' not found")
	})

	t.Run("mistyped", func(t *testing.T) {
		fields := map[string]interface{}{"raw": "not bytes"}
		err := runUnpackStep(t, unpackConfig("int16", "little"), fields)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "want []byte")
	})
}

// TestUnpackRoundTripProperty: generative coverage over the full type x
// endianness matrix. Random byte payloads decoded by unpack must match the
// encoding/binary.Read reference exactly (Float64bits comparison so NaN
// payload bit patterns count too), with and without scale, and the omitted
// endianness must behave as the host byte order.
func TestUnpackRoundTripProperty(t *testing.T) {
	orders := map[string]binary.ByteOrder{
		"little": binary.LittleEndian,
		"big":    binary.BigEndian,
		"":       binary.NativeEndian, // omitted = host order
	}

	rapid.Check(t, func(rt *rapid.T) {
		typeName := rapid.SampledFrom(unpackTestTypeNames).Draw(rt, "type")
		endianness := rapid.SampledFrom([]string{"little", "big", ""}).Draw(rt, "endianness")
		n := rapid.IntRange(0, 1024).Draw(rt, "n")

		width := unpackElemTypes[typeName].width
		raw := rapid.SliceOfN(rapid.Byte(), n*width, n*width).Draw(rt, "raw")
		want := referenceDecode(t, raw, typeName, orders[endianness])

		// Unscaled: integer types must yield the reference truncated to
		// int64 (uint64 wraps); float types the reference exactly.
		fields := map[string]interface{}{"raw": raw}
		require.NoError(t, runUnpackStep(t, unpackConfig(typeName, endianness), fields))

		if unpackElemTypes[typeName].isFloat {
			got, ok := fields["out"].([]float64)
			require.True(rt, ok)
			require.Len(rt, got, n)
			for i := range got {
				assert.Equal(rt, math.Float64bits(want[i]), math.Float64bits(got[i]), "index %d", i)
			}
		} else {
			got, ok := fields["out"].([]int64)
			require.True(rt, ok)
			require.Len(rt, got, n)
			wantInts := referenceDecodeInts(t, raw, typeName, orders[endianness])
			assert.Equal(rt, wantInts, got)
		}

		// Scaled: widen-then-multiply against the same reference.
		scale := rapid.OneOf(
			rapid.Float64Range(1e-9, 1e9),
			rapid.Float64Range(-1e9, -1e-9),
		).Draw(rt, "scale")

		config := unpackConfig(typeName, endianness)
		config["scale"] = map[string]interface{}{"source": "s"}
		fields = map[string]interface{}{"raw": raw, "s": scale}
		require.NoError(t, runUnpackStep(t, config, fields))

		got, ok := fields["out"].([]float64)
		require.True(rt, ok)
		require.Len(rt, got, n)
		for i := range got {
			assert.Equal(rt, math.Float64bits(want[i]*scale), math.Float64bits(got[i]), "index %d", i)
		}
	})
}

// referenceDecodeInts: binary.Read reference for the []int64 output path
// (integer types only; uint64 wraps via two's complement).
func referenceDecodeInts(t *testing.T, raw []byte, typeName string, order binary.ByteOrder) []int64 {
	t.Helper()

	width := unpackElemTypes[typeName].width
	n := len(raw) / width
	out := make([]int64, n)
	r := bytes.NewReader(raw)

	decode := func(dst interface{}, conv func(i int) int64) {
		require.NoError(t, binary.Read(r, order, dst))
		for i := range n {
			out[i] = conv(i)
		}
	}

	switch typeName {
	case "int8":
		v := make([]int8, n)
		decode(&v, func(i int) int64 { return int64(v[i]) })
	case "uint8":
		v := make([]uint8, n)
		decode(&v, func(i int) int64 { return int64(v[i]) })
	case "int16":
		v := make([]int16, n)
		decode(&v, func(i int) int64 { return int64(v[i]) })
	case "uint16":
		v := make([]uint16, n)
		decode(&v, func(i int) int64 { return int64(v[i]) })
	case "int32":
		v := make([]int32, n)
		decode(&v, func(i int) int64 { return int64(v[i]) })
	case "uint32":
		v := make([]uint32, n)
		decode(&v, func(i int) int64 { return int64(v[i]) })
	case "int64":
		v := make([]int64, n)
		decode(&v, func(i int) int64 { return v[i] })
	case "uint64":
		v := make([]uint64, n)
		decode(&v, func(i int) int64 { return int64(v[i]) }) //nolint:gosec // wrap is the documented contract
	default:
		t.Fatalf("referenceDecodeInts: not an integer type: %s", typeName)
	}

	return out
}
