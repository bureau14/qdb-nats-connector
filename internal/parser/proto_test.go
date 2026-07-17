// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package parser: parse_protobuf step tests (synthetic test.v1 schema only)
// Types: -
// Ex: go test -run TestParseProtobuf ./internal/parser
package parser

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/internal/parser/prototest"
	"github.com/bureau14/qdb-nats-connector/internal/util"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
	"pgregory.net/rapid"
)

// Schema paths are relative to internal/parser (the test working dir);
// the shared marshalling helpers live in prototest (same embedded schema).
const (
	testEnvelopeDesc  = "prototest/testdata/envelope.desc"
	testEnvelopeProto = "prototest/testdata/envelope.proto"
	testEnvelopeType  = prototest.EnvelopeType
	testInnerType     = prototest.InnerType
)

// requireParsingFailedError asserts err is a yaml_parser ParsingFailed error.
func requireParsingFailedError(t *testing.T, err error) {
	t.Helper()

	require.Error(t, err)

	var connErr *connectorErrors.ConnectorError
	require.True(t, errors.As(err, &connErr))
	assert.Equal(t, "yaml_parser", connErr.Component)
	assert.Equal(t, connectorErrors.ErrCodeParsingFailed, connErr.Code)
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

	badSyntaxProto := filepath.Join(t.TempDir(), "bad.proto")
	require.NoError(t, os.WriteFile(badSyntaxProto, []byte(`syntax = "proto3";
message {`), 0o600))

	badImportProto := filepath.Join(t.TempDir(), "badimport.proto")
	require.NoError(t, os.WriteFile(badImportProto, []byte(`syntax = "proto3";
import "nonexistent/missing.proto";
message M { string a = 1; }
`), 0o600))

	cases := []struct {
		name   string
		config map[string]interface{}
	}{
		{"neither descriptor_file nor proto_file", map[string]interface{}{
			"message_type": testEnvelopeType,
		}},
		{"both descriptor_file and proto_file", map[string]interface{}{
			"descriptor_file": testEnvelopeDesc,
			"proto_file":      testEnvelopeProto,
			"message_type":    testEnvelopeType,
		}},
		{"missing message_type", map[string]interface{}{
			"descriptor_file": testEnvelopeDesc,
		}},
		{"nonexistent descriptor file", map[string]interface{}{
			"descriptor_file": "testdata/nonexistent.desc",
			"message_type":    testEnvelopeType,
		}},
		{"proto_file empty", map[string]interface{}{
			"proto_file":   "",
			"message_type": testEnvelopeType,
		}},
		{"proto_file not a string", map[string]interface{}{
			"proto_file":   7,
			"message_type": testEnvelopeType,
		}},
		{"nonexistent proto file", map[string]interface{}{
			"proto_file":   "testdata/nonexistent.proto",
			"message_type": testEnvelopeType,
		}},
		{"proto file syntax error", map[string]interface{}{
			"proto_file":   badSyntaxProto,
			"message_type": testEnvelopeType,
		}},
		{"proto file unresolvable import", map[string]interface{}{
			"proto_file":   badImportProto,
			"message_type": "M",
		}},
		{"unknown message type via proto_file", map[string]interface{}{
			"proto_file":   testEnvelopeProto,
			"message_type": "test.v1.Nonexistent",
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

// TestParseProtobufProtoFileEquivalence pins the two schema front-ends to
// identical decode output: the same payload decoded via the protoc-compiled
// descriptor and via in-process .proto compilation yields equal fields.
// envelope.proto imports google/protobuf/timestamp.proto, so this also
// exercises protocompile's standard imports (the in-process replacement for
// protoc --include_imports).
func TestParseProtobufProtoFileEquivalence(t *testing.T) {
	payload := prototest.MarshalEnvelope(t, func(m protoreflect.Message) {
		fields := m.Descriptor().Fields()
		prototest.SetStringField(m, "name", "sensor-a")
		m.Set(fields.ByName("count"), protoreflect.ValueOfInt64(42))
		m.Set(fields.ByName("ratio"), protoreflect.ValueOfFloat64(0.5))
		prototest.SetTimestampField(m, "created_at", 1700000000, 123456789)
		prototest.SetBlobsEntry(m, 3, []byte{0xAA})
	})

	decode := func(schemaKey, schemaPath string) map[string]interface{} {
		step, err := makeParseProtobufStep(map[string]interface{}{
			schemaKey:      schemaPath,
			"message_type": testEnvelopeType,
		})
		require.NoError(t, err)

		state := &ParseState{Data: payload, Fields: map[string]interface{}{}}
		require.NoError(t, step(state))

		return state.Fields
	}

	fromDesc := decode("descriptor_file", testEnvelopeDesc)
	fromProto := decode("proto_file", testEnvelopeProto)

	assert.Equal(t, fromDesc, fromProto)
	assert.Equal(t, "sensor-a", fromProto["name"])
	assert.Equal(t, "2023-11-14T22:13:20.123456789Z", fromProto["created_at"])
}

// writeSchemaPipelineYAML writes a minimal parse_protobuf pipeline config
// referencing schemaPath under schemaKey (descriptor_file or proto_file),
// returning the config path.
// The schema path is SINGLE-quoted: absolute Windows temp paths contain
// backslashes, and in a double-quoted YAML scalar `\U...` is a unicode
// escape ("did not find expected hexdecimal number").
func writeSchemaPipelineYAML(t *testing.T, dir, schemaKey, schemaPath string) string {
	t.Helper()

	content := `
output:
  columns:
    - name: "name"
      type: "string"
transformations:
  - step: "parse_protobuf"
    config:
      ` + schemaKey + `: '` + schemaPath + `'
      message_type: "` + testEnvelopeType + `"
  - step: "extract_table"
    config:
      value: "t"
  - step: "extract_field"
    config:
      source: "name"
      target: "name"
      type: "string"
`
	path := filepath.Join(dir, "pipeline.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	return path
}

// TestDescriptorFileResolution pins the path semantics of descriptor_file:
// file-loaded configs resolve relative paths against the config directory
// (yaml + desc ship as a relocatable bundle); absolute paths pass through.
func TestDescriptorFileResolution(t *testing.T) {
	t.Run("relative resolves against config dir", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "bundle")
		require.NoError(t, os.MkdirAll(dir, 0o750))
		prototest.WriteDescriptor(t, dir)

		configPath := writeSchemaPipelineYAML(t, dir, "descriptor_file", "envelope.desc")

		parser, err := NewYAMLParser(configPath)
		require.NoError(t, err)
		require.NotNil(t, parser)
	})

	t.Run("absolute passes through", func(t *testing.T) {
		descDir := t.TempDir()
		descPath := prototest.WriteDescriptor(t, descDir)

		configPath := writeSchemaPipelineYAML(t, t.TempDir(), "descriptor_file", descPath)

		parser, err := NewYAMLParser(configPath)
		require.NoError(t, err)
		require.NotNil(t, parser)
	})

	t.Run("relative missing next to config fails compile", func(t *testing.T) {
		configPath := writeSchemaPipelineYAML(t, t.TempDir(), "descriptor_file", "envelope.desc")

		parser, err := NewYAMLParser(configPath)
		require.Error(t, err)
		assert.Nil(t, parser)

		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
	})

	t.Run("programmatic config stays cwd-relative", func(t *testing.T) {
		// testEnvelopeDesc is relative to internal/parser (the test cwd);
		// NewYAMLParserFromConfig must not rewrite it.
		_, err := NewYAMLParserFromConfig(envelopePipelineConfig())
		require.NoError(t, err)
	})
}

// TestProtoFileResolution pins the same path semantics for proto_file:
// file-loaded configs resolve relative paths against the config directory
// (yaml + proto ship as a relocatable bundle); absolute paths pass through.
func TestProtoFileResolution(t *testing.T) {
	t.Run("relative resolves against config dir", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "bundle")
		require.NoError(t, os.MkdirAll(dir, 0o750))
		prototest.WriteProtoSource(t, dir)

		configPath := writeSchemaPipelineYAML(t, dir, "proto_file", "envelope.proto")

		parser, err := NewYAMLParser(configPath)
		require.NoError(t, err)
		require.NotNil(t, parser)
	})

	t.Run("absolute passes through", func(t *testing.T) {
		protoDir := t.TempDir()
		protoPath := prototest.WriteProtoSource(t, protoDir)

		configPath := writeSchemaPipelineYAML(t, t.TempDir(), "proto_file", protoPath)

		parser, err := NewYAMLParser(configPath)
		require.NoError(t, err)
		require.NotNil(t, parser)
	})

	t.Run("relative missing next to config fails compile", func(t *testing.T) {
		configPath := writeSchemaPipelineYAML(t, t.TempDir(), "proto_file", "envelope.proto")

		parser, err := NewYAMLParser(configPath)
		require.Error(t, err)
		assert.Nil(t, parser)

		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
	})

	t.Run("programmatic config stays cwd-relative", func(t *testing.T) {
		// testEnvelopeProto is relative to internal/parser (the test cwd);
		// NewYAMLParserFromConfig must not rewrite it.
		config := envelopePipelineConfig()
		config.Transformations[0].Config = map[string]interface{}{
			"proto_file":   testEnvelopeProto,
			"message_type": testEnvelopeType,
		}

		_, err := NewYAMLParserFromConfig(config)
		require.NoError(t, err)
	})
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

	inner := prototest.MarshalInner(t, 42.5, "mm")

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
		"attribute_raw": prototest.MarshalInner(t, 1.5, "deg"),
		"attr":          "pre-existing", // target overwrites, consistent with root merge
	}}
	require.NoError(t, step(state))

	nested, ok := state.Fields["attr"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, 1.5, nested["reading"])
	assert.Equal(t, "deg", nested["unit"])

	// nested values stay dot-path addressable for extract_field
	lookupReading, err := compileFieldPath("attr.reading")
	require.NoError(t, err)
	reading, found := lookupReading(state.Fields)
	require.True(t, found)
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
	wrongInner := prototest.MarshalEnvelope(t, func(m protoreflect.Message) {
		prototest.SetStringField(m, "name", "not-an-inner")
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
	payload := prototest.MarshalEnvelope(t, func(m protoreflect.Message) {
		fields := m.Descriptor().Fields()
		prototest.SetStringField(m, "name", "sensor-a")
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
	lookupUnit, err := compileFieldPath("inner.unit")
	require.NoError(t, err)
	unit, found := lookupUnit(fields)
	require.True(t, found)
	assert.Equal(t, "mm", unit)
}

// TestParseProtobufIntegerNormalization pins the int64/float64 widening of
// uint32, uint64, sint32, and float, including the documented uint64 wrap
func TestParseProtobufIntegerNormalization(t *testing.T) {
	payload := prototest.MarshalEnvelope(t, func(m protoreflect.Message) {
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
			payload := prototest.MarshalEnvelope(t, func(m protoreflect.Message) {
				prototest.SetTimestampField(m, "created_at", seconds, tc.nanos)
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
	payload := prototest.MarshalEnvelope(t, func(m protoreflect.Message) {
		prototest.SetStringField(m, "name", "only-me")
	})

	fields := runEnvelopeStep(t, payload)

	assert.Equal(t, map[string]interface{}{"name": "only-me"}, fields)
}

// TestParseProtobufUnknownFields validates that wire fields absent from the
// descriptor are silently ignored (protobuf forward compatibility)
func TestParseProtobufUnknownFields(t *testing.T) {
	payload := prototest.MarshalEnvelope(t, func(m protoreflect.Message) {
		prototest.SetStringField(m, "name", "known")
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

	payload := prototest.MarshalEnvelope(t, func(m protoreflect.Message) {
		prototest.SetStringField(m, "name", "sensor-a")
		m.Set(m.Descriptor().Fields().ByName("ratio"), protoreflect.ValueOfFloat64(0.5))
		prototest.SetTimestampField(m, "created_at", time.Date(2026, 7, 2, 11, 22, 33, 0, time.UTC).Unix(), 500000000)
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
		payload := prototest.MarshalEnvelope(t, func(m protoreflect.Message) {
			prototest.SetStringField(m, "name", "sensor-a")
			m.Set(m.Descriptor().Fields().ByName("ratio"), protoreflect.ValueOfFloat64(0.5))
			prototest.SetTimestampField(m, "created_at", 1751455353, 123456789)
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

			payload := prototest.MarshalEnvelope(t, func(m protoreflect.Message) {
				fields := m.Descriptor().Fields()
				prototest.SetStringField(m, "name", name)
				m.Set(fields.ByName("count"), protoreflect.ValueOfInt64(count))
				m.Set(fields.ByName("ratio"), protoreflect.ValueOfFloat64(ratio))
				prototest.SetTimestampField(m, "created_at", seconds, nanos)
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

// composedPipelineConfig returns the SKF-shaped pipeline over the neutral
// synthetic schema: outer envelope decode -> map entry lift (capability
// filter) -> nested bytes re-decode -> subject token -> split-derived int64
// -> table/index/fields. This is the repo's living reference for composing
// the M2 steps; the real deployment config differs only in names.
func composedPipelineConfig() YAMLConfig {
	return YAMLConfig{
		Output: OutputSchema{
			Columns: []ColumnSchema{
				{Name: "timestamp", Type: "timestamp"},
				{Name: "stream_token", Type: "string"},
				{Name: "value", Type: "double"},
				{Name: "capability_key", Type: "int64"},
				{Name: "revision", Type: "int64"},
				{Name: "stream_id", Type: "string"},
				{Name: "ingested_at", Type: "string"},
			},
		},
		Transformations: []TransformSpec{
			{Step: "parse_protobuf", Config: map[string]interface{}{
				"descriptor_file": testEnvelopeDesc,
				"message_type":    testEnvelopeType,
			}},
			{Step: "extract_map_entry", Config: map[string]interface{}{
				"source":       "blobs",
				"key_target":   "capability_key",
				"value_target": "attribute_raw",
				"on_multiple":  "first",
				"allowed_keys": []interface{}{"3"},
			}},
			{Step: "parse_protobuf", Config: map[string]interface{}{
				"descriptor_file": testEnvelopeDesc,
				"message_type":    testInnerType,
				"source":          "attribute_raw",
				"target":          "attr",
			}},
			{Step: "extract_subject", Config: map[string]interface{}{
				"target":  "stream_token",
				"segment": -2,
				"trim":    "=",
			}},
			{Step: "compute_field", Config: map[string]interface{}{
				"operation": "split",
				"source":    "name",
				"separator": ":",
				"index":     -1,
				"target":    "revision_raw",
			}},
			{Step: "extract_table", Config: map[string]interface{}{"value": "streams"}},
			{Step: "extract_index", Config: map[string]interface{}{
				"source": "created_at",
				"format": "rfc3339nano",
			}},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "stream_token", "target": "stream_token", "type": "string",
			}},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "attr.reading", "target": "value", "type": "float64",
			}},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "capability_key", "target": "capability_key", "type": "int64",
			}},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "revision_raw", "target": "revision", "type": "int64",
			}},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "name", "target": "stream_id", "type": "string",
			}},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "updated_at", "target": "ingested_at", "type": "string",
			}},
		},
	}
}

// composedEnvelope marshals the happy-path message for the composed
// pipeline: capability key 3 carrying a serialized Inner.
func composedEnvelope(t *testing.T, blobKey int32, blobValue []byte) []byte {
	t.Helper()

	return prototest.MarshalEnvelope(t, func(m protoreflect.Message) {
		prototest.SetStringField(m, "name", "stream:token:7")
		prototest.SetBlobsEntry(m, blobKey, blobValue)
		prototest.SetTimestampField(m, "created_at", 1700000000, 123456789)
		prototest.SetTimestampField(m, "updated_at", 1700000001, 42)
	})
}

// columnValue reads the single row value at a column offset.
//
//nolint:ireturn // T is instantiated to concrete types at every call site
func columnValue[T any](t *testing.T, table *qdb.WriterTable, offset int,
	get func(qdb.ColumnData) ([]T, error),
) T {
	t.Helper()

	cd, err := table.GetData(offset)
	require.NoError(t, err)

	xs, err := get(cd)
	require.NoError(t, err)
	require.Len(t, xs, 1)

	return xs[0]
}

// TestPipelineComposedSKFShape is the M2 exit criterion: the parent spec's
// Goal-section pipeline (neutral naming, synthetic descriptor, allowed_keys
// on extract_map_entry) produces correct rows, nanosecond-exact index, a
// clean drop for a disallowed capability, and OutcomePartial - not a drop -
// for wrong-inner-type bytes (the M1 correction, end-to-end)
func TestPipelineComposedSKFShape(t *testing.T) {
	parser, err := NewYAMLParserFromConfig(composedPipelineConfig())
	require.NoError(t, err)

	subject := "t.1.0.ion.streams.ABCDEFG=.value"

	t.Run("happy path produces one exact row", func(t *testing.T) {
		payload := composedEnvelope(t, 3, prototest.MarshalInner(t, 42.5, "mm"))

		res, err := parser.Parse(&nats.Msg{Subject: subject, Data: payload})
		require.NoError(t, err)
		assert.Equal(t, OutcomeOK, res.Outcome)
		assert.Empty(t, res.Errors)
		require.Len(t, res.Tables, 1)

		table := &res.Tables[0]
		assert.Equal(t, "streams", stripNullTerminator(table.GetName()))
		require.Equal(t, 1, table.RowCount())

		index := table.GetIndex()
		require.Len(t, index, 1)
		assert.True(t, index[0].Equal(time.Unix(1700000000, 123456789)),
			"index must be nanosecond-exact, got %v", index[0])

		assert.Equal(t, "ABCDEFG", columnValue(t, table, 1, qdb.GetColumnDataString))
		assert.InDelta(t, 42.5, columnValue(t, table, 2, qdb.GetColumnDataDouble), 0)
		assert.Equal(t, int64(3), columnValue(t, table, 3, qdb.GetColumnDataInt64))
		assert.Equal(t, int64(7), columnValue(t, table, 4, qdb.GetColumnDataInt64))
		assert.Equal(t, "stream:token:7", columnValue(t, table, 5, qdb.GetColumnDataString))
		assert.Equal(t, "2023-11-14T22:13:21.000000042Z", columnValue(t, table, 6, qdb.GetColumnDataString))
	})

	t.Run("disallowed capability drops", func(t *testing.T) {
		payload := composedEnvelope(t, 4, prototest.MarshalInner(t, 1.0, "x"))

		res, err := parser.Parse(&nats.Msg{Subject: subject, Data: payload})
		requireUnusable(t, res, err)
		assert.True(t, errorsContain(res.Errors, "allowed_keys"))
	})

	t.Run("wrong inner type is partial, not drop", func(t *testing.T) {
		wrongInner := prototest.MarshalEnvelope(t, func(m protoreflect.Message) {
			prototest.SetStringField(m, "name", "not-an-inner")
		})
		payload := composedEnvelope(t, 3, wrongInner)

		res, err := parser.Parse(&nats.Msg{Subject: subject, Data: payload})
		require.NoError(t, err)
		assert.Equal(t, OutcomePartial, res.Outcome)
		require.Len(t, res.Tables, 1)
		assert.True(t, errorsContain(res.Errors, "attr.reading"))
	})
}
