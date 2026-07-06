// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package parser: parse_protobuf step tests (synthetic test.v1 schema only)
// Types: -
// Ex: go test -run TestParseProtobuf ./internal/parser
package parser

import (
	"errors"
	"math"
	"os"
	"testing"
	"time"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/internal/util"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"pgregory.net/rapid"
)

const (
	testEnvelopeDesc = "testdata/envelope.desc"
	testEnvelopeType = "test.v1.Envelope"
	testInnerType    = "test.v1.Inner"
)

// newEnvelopeMessage returns an empty dynamic test.v1.Envelope message.
func newEnvelopeMessage(t *testing.T) *dynamicpb.Message {
	t.Helper()

	dec, err := newProtoDecoder(protoStepConfig{
		descriptorFile: testEnvelopeDesc,
		messageType:    testEnvelopeType,
	})
	require.NoError(t, err)

	return dynamicpb.NewMessage(dec.desc)
}

// marshalEnvelope builds a test payload via dynamicpb round-trip: set
// populates the message through protoreflect, then it is marshalled.
func marshalEnvelope(t *testing.T, set func(m protoreflect.Message)) []byte {
	t.Helper()

	m := newEnvelopeMessage(t)
	set(m)

	data, err := proto.Marshal(m)
	require.NoError(t, err)

	return data
}

// marshalInner builds a serialized test.v1.Inner payload - the synthetic
// stand-in for a nested attribute message carried as map value bytes.
func marshalInner(t *testing.T, reading float64, unit string) []byte {
	t.Helper()

	dec, err := newProtoDecoder(protoStepConfig{
		descriptorFile: testEnvelopeDesc,
		messageType:    testInnerType,
	})
	require.NoError(t, err)

	m := dynamicpb.NewMessage(dec.desc)
	fields := m.Descriptor().Fields()
	m.Set(fields.ByName("reading"), protoreflect.ValueOfFloat64(reading))
	m.Set(fields.ByName("unit"), protoreflect.ValueOfString(unit))

	data, err := proto.Marshal(m)
	require.NoError(t, err)

	return data
}

// requireParsingFailedError asserts err is a yaml_parser ParsingFailed error.
func requireParsingFailedError(t *testing.T, err error) {
	t.Helper()

	require.Error(t, err)

	var connErr *connectorErrors.ConnectorError
	require.True(t, errors.As(err, &connErr))
	assert.Equal(t, "yaml_parser", connErr.Component)
	assert.Equal(t, connectorErrors.ErrCodeParsingFailed, connErr.Code)
}

// setStringField sets a string field by name.
func setStringField(m protoreflect.Message, name, value string) {
	m.Set(m.Descriptor().Fields().ByName(protoreflect.Name(name)), protoreflect.ValueOfString(value))
}

// setTimestampField populates a google.protobuf.Timestamp field by name.
func setTimestampField(m protoreflect.Message, name string, seconds int64, nanos int32) {
	fd := m.Descriptor().Fields().ByName(protoreflect.Name(name))
	ts := m.Mutable(fd).Message()
	tsFields := ts.Descriptor().Fields()
	ts.Set(tsFields.ByNumber(1), protoreflect.ValueOfInt64(seconds))
	ts.Set(tsFields.ByNumber(2), protoreflect.ValueOfInt32(nanos))
}

// runEnvelopeStep runs the parse_protobuf step on payload and returns the
// decoded fields.
func runEnvelopeStep(t *testing.T, payload []byte) map[string]interface{} {
	t.Helper()

	step, err := makeParseProtobufStep(map[string]interface{}{
		"descriptor_file": testEnvelopeDesc,
		"message_type":    testEnvelopeType,
	})
	require.NoError(t, err)

	state := &ParseState{Data: payload, Fields: map[string]interface{}{}}
	require.NoError(t, step(state))

	return state.Fields
}

// envelopePipelineConfig returns a full pipeline over the synthetic schema:
// parse_protobuf + extract_index + extract_field + extract_table.
func envelopePipelineConfig() YAMLConfig {
	return YAMLConfig{
		Output: OutputSchema{
			Columns: []ColumnSchema{
				{Name: "timestamp", Type: "timestamp"},
				{Name: "name", Type: "string"},
				{Name: "ratio", Type: "double"},
			},
		},
		Transformations: []TransformSpec{
			{Step: "parse_protobuf", Config: map[string]interface{}{
				"descriptor_file": testEnvelopeDesc,
				"message_type":    testEnvelopeType,
			}},
			{Step: "extract_index", Config: map[string]interface{}{
				"source": "created_at",
				"format": "rfc3339nano",
			}},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "name",
				"target": "name",
				"type":   "string",
			}},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "ratio",
				"target": "ratio",
				"type":   "float64",
			}},
			{Step: "extract_table", Config: map[string]interface{}{"value": "events"}},
		},
	}
}

// TestParseProtobufFactoryErrors validates fail-fast config handling
func TestParseProtobufFactoryErrors(t *testing.T) {
	garbageDesc, err := os.CreateTemp(t.TempDir(), "garbage-*.desc")
	require.NoError(t, err)
	_, err = garbageDesc.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF})
	require.NoError(t, err)
	require.NoError(t, garbageDesc.Close())

	cases := []struct {
		name   string
		config map[string]interface{}
	}{
		{"missing descriptor_file", map[string]interface{}{
			"message_type": testEnvelopeType,
		}},
		{"missing message_type", map[string]interface{}{
			"descriptor_file": testEnvelopeDesc,
		}},
		{"nonexistent descriptor file", map[string]interface{}{
			"descriptor_file": "testdata/nonexistent.desc",
			"message_type":    testEnvelopeType,
		}},
		{"garbage descriptor file", map[string]interface{}{
			"descriptor_file": garbageDesc.Name(),
			"message_type":    testEnvelopeType,
		}},
		{"unknown message type", map[string]interface{}{
			"descriptor_file": testEnvelopeDesc,
			"message_type":    "test.v1.Nonexistent",
		}},
		{"enum is not a message type", map[string]interface{}{
			"descriptor_file": testEnvelopeDesc,
			"message_type":    "test.v1.Mode",
		}},
		{"source empty", map[string]interface{}{
			"descriptor_file": testEnvelopeDesc,
			"message_type":    testEnvelopeType,
			"source":          "",
		}},
		{"source not a string", map[string]interface{}{
			"descriptor_file": testEnvelopeDesc,
			"message_type":    testEnvelopeType,
			"source":          7,
		}},
		{"source malformed path", map[string]interface{}{
			"descriptor_file": testEnvelopeDesc,
			"message_type":    testEnvelopeType,
			"source":          "a..b",
		}},
		{"target empty", map[string]interface{}{
			"descriptor_file": testEnvelopeDesc,
			"message_type":    testEnvelopeType,
			"target":          "",
		}},
		{"target not a string", map[string]interface{}{
			"descriptor_file": testEnvelopeDesc,
			"message_type":    testEnvelopeType,
			"target":          7,
		}},
		{"target contains dot", map[string]interface{}{
			"descriptor_file": testEnvelopeDesc,
			"message_type":    testEnvelopeType,
			"target":          "a.b",
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			step, err := makeParseProtobufStep(tc.config)
			assert.Nil(t, step)
			require.Error(t, err)

			var connErr *connectorErrors.ConnectorError
			require.True(t, errors.As(err, &connErr))
			assert.Equal(t, "yaml_parser", connErr.Component)
			assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
		})
	}
}

// TestParseProtobufSourceField validates decoding from a state.Fields entry
// instead of the raw payload (the nested re-decode pattern)
func TestParseProtobufSourceField(t *testing.T) {
	step, err := makeParseProtobufStep(map[string]interface{}{
		"descriptor_file": testEnvelopeDesc,
		"message_type":    testInnerType,
		"source":          "attribute_raw",
	})
	require.NoError(t, err)

	inner := marshalInner(t, 42.5, "mm")

	t.Run("bytes source decodes", func(t *testing.T) {
		state := &ParseState{Fields: map[string]interface{}{"attribute_raw": inner}}
		require.NoError(t, step(state))
		assert.Equal(t, 42.5, state.Fields["reading"])
		assert.Equal(t, "mm", state.Fields["unit"])
	})

	t.Run("string source decodes", func(t *testing.T) {
		state := &ParseState{Fields: map[string]interface{}{"attribute_raw": string(inner)}}
		require.NoError(t, step(state))
		assert.Equal(t, 42.5, state.Fields["reading"])
	})

	t.Run("missing source field errors", func(t *testing.T) {
		state := &ParseState{Fields: map[string]interface{}{}}
		requireParsingFailedError(t, step(state))
	})

	t.Run("wrong source type errors", func(t *testing.T) {
		state := &ParseState{Fields: map[string]interface{}{"attribute_raw": int64(7)}}
		requireParsingFailedError(t, step(state))
	})

	t.Run("empty source bytes error", func(t *testing.T) {
		state := &ParseState{Fields: map[string]interface{}{"attribute_raw": []byte{}}}
		requireParsingFailedError(t, step(state))
	})
}

// TestParseProtobufTarget validates nesting decoded fields under a single
// top-level key instead of merging at the root
func TestParseProtobufTarget(t *testing.T) {
	step, err := makeParseProtobufStep(map[string]interface{}{
		"descriptor_file": testEnvelopeDesc,
		"message_type":    testInnerType,
		"source":          "attribute_raw",
		"target":          "attr",
	})
	require.NoError(t, err)

	state := &ParseState{Fields: map[string]interface{}{
		"attribute_raw": marshalInner(t, 1.5, "deg"),
		"attr":          "pre-existing", // target overwrites, consistent with root merge
	}}
	require.NoError(t, step(state))

	nested, ok := state.Fields["attr"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, 1.5, nested["reading"])
	assert.Equal(t, "deg", nested["unit"])

	// nested values stay dot-path addressable for extract_field
	reading, err := extractFieldByPath(state.Fields, "attr.reading")
	require.NoError(t, err)
	assert.Equal(t, 1.5, reading)

	// decoded fields must not leak to the root when target is set
	_, present := state.Fields["reading"]
	assert.False(t, present)
}

// TestParseProtobufNestedWrongInnerType pins the M1 finding that makes the
// nested re-decode unsuitable as a message-shape filter: bytes of the WRONG
// message type decode without error to zero populated fields, because
// mismatched wire types park as unknown fields (Envelope field 1 is a
// string, wire type 2; Inner field 1 is a double, wire type 1).
func TestParseProtobufNestedWrongInnerType(t *testing.T) {
	wrongInner := marshalEnvelope(t, func(m protoreflect.Message) {
		setStringField(m, "name", "not-an-inner")
	})

	step, err := makeParseProtobufStep(map[string]interface{}{
		"descriptor_file": testEnvelopeDesc,
		"message_type":    testInnerType,
		"source":          "attribute_raw",
		"target":          "attr",
	})
	require.NoError(t, err)

	state := &ParseState{Fields: map[string]interface{}{"attribute_raw": wrongInner}}
	require.NoError(t, step(state))

	nested, ok := state.Fields["attr"].(map[string]interface{})
	require.True(t, ok)
	assert.Empty(t, nested)
}

// TestParseProtobufScalarMapping validates the normalized Go type for every
// mapped field kind
func TestParseProtobufScalarMapping(t *testing.T) {
	payload := marshalEnvelope(t, func(m protoreflect.Message) {
		fields := m.Descriptor().Fields()
		setStringField(m, "name", "sensor-a")
		m.Set(fields.ByName("count"), protoreflect.ValueOfInt64(42))
		m.Set(fields.ByName("ratio"), protoreflect.ValueOfFloat64(0.5))
		m.Set(fields.ByName("enabled"), protoreflect.ValueOfBool(true))
		m.Set(fields.ByName("raw"), protoreflect.ValueOfBytes([]byte{0x01, 0x02}))
		m.Set(fields.ByName("mode"), protoreflect.ValueOfEnum(1))

		tags := m.Mutable(fields.ByName("tags")).List()
		tags.Append(protoreflect.ValueOfString("a"))
		tags.Append(protoreflect.ValueOfString("b"))

		nums := m.Mutable(fields.ByName("nums")).List()
		nums.Append(protoreflect.ValueOfInt64(1))
		nums.Append(protoreflect.ValueOfInt64(2))

		inner := m.Mutable(fields.ByName("inner")).Message()
		innerFields := inner.Descriptor().Fields()
		inner.Set(innerFields.ByName("reading"), protoreflect.ValueOfFloat64(1.5))
		inner.Set(innerFields.ByName("unit"), protoreflect.ValueOfString("mm"))

		blobs := m.Mutable(fields.ByName("blobs")).Map()
		blobs.Set(protoreflect.ValueOfInt32(3).MapKey(), protoreflect.ValueOfBytes([]byte{0xAA}))
	})

	fields := runEnvelopeStep(t, payload)

	assert.Equal(t, "sensor-a", fields["name"])
	assert.Equal(t, int64(42), fields["count"])
	assert.InDelta(t, 0.5, fields["ratio"], 0)
	assert.Equal(t, true, fields["enabled"])
	assert.Equal(t, []byte{0x01, 0x02}, fields["raw"])
	assert.Equal(t, int64(1), fields["mode"])
	assert.Equal(t, []interface{}{"a", "b"}, fields["tags"])
	assert.Equal(t, []interface{}{int64(1), int64(2)}, fields["nums"])
	assert.Equal(t, map[string]interface{}{"reading": 1.5, "unit": "mm"}, fields["inner"])
	assert.Equal(t, map[string]interface{}{"3": []byte{0xAA}}, fields["blobs"])

	// Nested messages are dot-path addressable by extract_field.
	unit, err := extractFieldByPath(fields, "inner.unit")
	require.NoError(t, err)
	assert.Equal(t, "mm", unit)
}

// TestParseProtobufIntegerNormalization pins the int64/float64 widening of
// uint32, uint64, sint32, and float, including the documented uint64 wrap
func TestParseProtobufIntegerNormalization(t *testing.T) {
	payload := marshalEnvelope(t, func(m protoreflect.Message) {
		fields := m.Descriptor().Fields()
		m.Set(fields.ByName("small"), protoreflect.ValueOfUint32(7))
		m.Set(fields.ByName("big"), protoreflect.ValueOfUint64(uint64(math.MaxInt64)+1))
		m.Set(fields.ByName("weight"), protoreflect.ValueOfFloat32(1.5))
		m.Set(fields.ByName("delta"), protoreflect.ValueOfInt32(-5))
	})

	fields := runEnvelopeStep(t, payload)

	assert.Equal(t, int64(7), fields["small"])
	assert.Equal(t, int64(math.MinInt64), fields["big"])
	assert.InDelta(t, float64(1.5), fields["weight"], 0)
	assert.Equal(t, int64(-5), fields["delta"])
}

// TestParseProtobufTimestamps validates fixed 9-digit nanosecond rendering
// of well-known Timestamp fields
func TestParseProtobufTimestamps(t *testing.T) {
	seconds := time.Date(2026, 7, 2, 11, 22, 33, 0, time.UTC).Unix()

	cases := []struct {
		name  string
		nanos int32
		want  string
	}{
		{"zero nanos keep full width", 0, "2026-07-02T11:22:33.000000000Z"},
		{"max nanos", 999999999, "2026-07-02T11:22:33.999999999Z"},
		{"single nano", 1, "2026-07-02T11:22:33.000000001Z"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := marshalEnvelope(t, func(m protoreflect.Message) {
				setTimestampField(m, "created_at", seconds, tc.nanos)
			})

			fields := runEnvelopeStep(t, payload)
			assert.Equal(t, tc.want, fields["created_at"])

			// Unset timestamp fields are absent, not zero-rendered.
			assert.NotContains(t, fields, "updated_at")
		})
	}
}

// TestParseProtobufPopulatedOnly validates populated-only Range semantics:
// unset proto3 scalars never appear in state.Fields
func TestParseProtobufPopulatedOnly(t *testing.T) {
	payload := marshalEnvelope(t, func(m protoreflect.Message) {
		setStringField(m, "name", "only-me")
	})

	fields := runEnvelopeStep(t, payload)

	assert.Equal(t, map[string]interface{}{"name": "only-me"}, fields)
}

// TestParseProtobufUnknownFields validates that wire fields absent from the
// descriptor are silently ignored (protobuf forward compatibility)
func TestParseProtobufUnknownFields(t *testing.T) {
	payload := marshalEnvelope(t, func(m protoreflect.Message) {
		setStringField(m, "name", "known")
	})

	// Append field 999, wire type varint: tag 999<<3 = 7992 → varint
	// 0xB8 0x3E, followed by value 1.
	withUnknown := append(append([]byte{}, payload...), 0xB8, 0x3E, 0x01)

	fields := runEnvelopeStep(t, withUnknown)

	assert.Equal(t, map[string]interface{}{"name": "known"}, fields)
}

// TestParseProtobufWireTypeMismatch documents protobuf wire semantics: a
// known field with the wrong wire type does NOT fail decode - it is parked
// as an unknown field and simply absent from the output. Only malformed
// wire data (truncation, invalid tags) errors and drops structurally
func TestParseProtobufWireTypeMismatch(t *testing.T) {
	t.Run("mismatched field treated as unknown", func(t *testing.T) {
		// Field 1 (name) is a string (wire type 2); encode it as varint
		// (wire type 0) instead: tag 0x08, value 0x05.
		fields := runEnvelopeStep(t, []byte{0x08, 0x05})

		assert.NotContains(t, fields, "name")
		assert.Empty(t, fields)
	})

	t.Run("truncated length-delimited field errors", func(t *testing.T) {
		step, err := makeParseProtobufStep(map[string]interface{}{
			"descriptor_file": testEnvelopeDesc,
			"message_type":    testEnvelopeType,
		})
		require.NoError(t, err)

		// Field 1 declares 5 payload bytes but only 1 follows.
		state := &ParseState{Data: []byte{0x0A, 0x05, 0x61}, Fields: map[string]interface{}{}}
		err = step(state)
		require.Error(t, err)

		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, connectorErrors.ErrCodeParsingFailed, connErr.Code)
	})

	t.Run("malformed payload drops structurally through pipeline", func(t *testing.T) {
		parser, err := NewYAMLParserFromConfig(envelopePipelineConfig())
		require.NoError(t, err)

		res, err := parser.Parse(&nats.Msg{Subject: util.RandomTopicName(), Data: []byte{0x0A, 0x05, 0x61}})
		requireUnusable(t, res, err)
	})
}

// TestParseProtobufPipeline is the M1 exit criterion: parse_protobuf +
// existing steps ingest a flat synthetic message end-to-end
func TestParseProtobufPipeline(t *testing.T) {
	parser, err := NewYAMLParserFromConfig(envelopePipelineConfig())
	require.NoError(t, err)

	payload := marshalEnvelope(t, func(m protoreflect.Message) {
		setStringField(m, "name", "sensor-a")
		m.Set(m.Descriptor().Fields().ByName("ratio"), protoreflect.ValueOfFloat64(0.5))
		setTimestampField(m, "created_at", time.Date(2026, 7, 2, 11, 22, 33, 0, time.UTC).Unix(), 500000000)
	})

	res, err := parser.Parse(&nats.Msg{Subject: util.RandomTopicName(), Data: payload})
	require.NoError(t, err)
	assert.Equal(t, OutcomeOK, res.Outcome)
	require.Len(t, res.Tables, 1)

	table := res.Tables[0]
	assert.Equal(t, "events", stripNullTerminator(table.GetName()))
	assert.Equal(t, 1, table.RowCount())
}

// TestParseProtobufGarbageUnusable validates that undecodable payloads drop
// structurally through the full pipeline
func TestParseProtobufGarbageUnusable(t *testing.T) {
	parser, err := NewYAMLParserFromConfig(envelopePipelineConfig())
	require.NoError(t, err)

	res, err := parser.Parse(&nats.Msg{
		Subject: util.RandomTopicName(),
		Data:    []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
	})
	requireUnusable(t, res, err)
}

// TestParseProtobufProperties validates decode robustness and round-trip
// correctness with property-based testing
func TestParseProtobufProperties(t *testing.T) {
	parser, err := NewYAMLParserFromConfig(envelopePipelineConfig())
	require.NoError(t, err)

	t.Run("property: arbitrary bytes never panic or error the parser", func(t *testing.T) {
		payloads := rapid.SliceOfN(rapid.Byte(), 0, 256)

		rapid.Check(t, func(rt *rapid.T) {
			msg := &nats.Msg{
				Subject: util.RandomTopicName(),
				Data:    payloads.Draw(rt, "payload"),
			}

			res, err := parser.Parse(msg)
			require.NoError(rt, err)

			if res.Outcome == OutcomeOK {
				require.Len(rt, res.Tables, 1)
			}
		})
	})

	t.Run("property: truncated or mutated valid payloads never panic", func(t *testing.T) {
		payload := marshalEnvelope(t, func(m protoreflect.Message) {
			setStringField(m, "name", "sensor-a")
			m.Set(m.Descriptor().Fields().ByName("ratio"), protoreflect.ValueOfFloat64(0.5))
			setTimestampField(m, "created_at", 1751455353, 123456789)
		})

		step, err := makeParseProtobufStep(map[string]interface{}{
			"descriptor_file": testEnvelopeDesc,
			"message_type":    testEnvelopeType,
		})
		require.NoError(t, err)

		rapid.Check(t, func(rt *rapid.T) {
			mutated := append([]byte{}, payload...)
			if rapid.Bool().Draw(rt, "truncate") {
				mutated = mutated[:rapid.IntRange(0, len(mutated)).Draw(rt, "cut")]
			} else {
				pos := rapid.IntRange(0, len(mutated)-1).Draw(rt, "pos")
				mutated[pos] ^= rapid.ByteRange(1, 255).Draw(rt, "flip")
			}

			state := &ParseState{Data: mutated, Fields: map[string]interface{}{}}
			_ = step(state) // must return or error, never panic
		})
	})

	t.Run("property: drawn values round-trip through marshal and decode", func(t *testing.T) {
		rapid.Check(t, func(rt *rapid.T) {
			name := rapid.String().Draw(rt, "name")
			count := rapid.Int64().Draw(rt, "count")
			ratio := rapid.Float64Range(-1e12, 1e12).Draw(rt, "ratio")
			seconds := rapid.Int64Range(0, 1e10).Draw(rt, "seconds")
			nanos := rapid.Int32Range(0, 999999999).Draw(rt, "nanos")

			payload := marshalEnvelope(t, func(m protoreflect.Message) {
				fields := m.Descriptor().Fields()
				setStringField(m, "name", name)
				m.Set(fields.ByName("count"), protoreflect.ValueOfInt64(count))
				m.Set(fields.ByName("ratio"), protoreflect.ValueOfFloat64(ratio))
				setTimestampField(m, "created_at", seconds, nanos)
			})

			fields := runEnvelopeStep(t, payload)

			if name != "" {
				require.Equal(rt, name, fields["name"])
			}
			if count != 0 {
				require.Equal(rt, count, fields["count"])
			}
			if ratio != 0 {
				require.InDelta(rt, ratio, fields["ratio"], 0)
			}

			rendered, ok := fields["created_at"].(string)
			require.True(rt, ok)

			parsed, err := time.Parse(time.RFC3339Nano, rendered)
			require.NoError(rt, err)
			require.Equal(rt, seconds, parsed.Unix())
			require.Equal(rt, int(nanos), parsed.Nanosecond())
		})
	})
}
