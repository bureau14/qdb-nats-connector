// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package parser: extract_map_entry step tests
// Types: -
// Ex: go test -run TestExtractMapEntry ./internal/parser
package parser

import (
	"errors"
	"testing"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/internal/util"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
	"pgregory.net/rapid"
)

// setBlobsEntry stores raw bytes under an int32 key in Envelope.blobs.
func setBlobsEntry(m protoreflect.Message, key int32, value []byte) {
	fd := m.Descriptor().Fields().ByName("blobs")
	m.Mutable(fd).Map().Set(protoreflect.ValueOfInt32(key).MapKey(), protoreflect.ValueOfBytes(value))
}

// runMapEntryStep compiles the step and runs it over the given fields.
func runMapEntryStep(t *testing.T, config, fields map[string]interface{}) (*ParseState, error) {
	t.Helper()

	step, err := makeExtractMapEntryStep(config)
	require.NoError(t, err)

	state := &ParseState{Fields: fields}

	return state, step(state)
}

// mapEntryBaseConfig returns the minimal valid step config.
func mapEntryBaseConfig() map[string]interface{} {
	return map[string]interface{}{
		"source":       "attributes",
		"key_target":   "capability_key",
		"value_target": "attribute_raw",
	}
}

// TestExtractMapEntryFactoryErrors validates fail-fast config handling
func TestExtractMapEntryFactoryErrors(t *testing.T) {
	cases := []struct {
		name   string
		config map[string]interface{}
	}{
		{"missing source", map[string]interface{}{
			"key_target": "k", "value_target": "v",
		}},
		{"missing key_target", map[string]interface{}{
			"source": "attributes", "value_target": "v",
		}},
		{"missing value_target", map[string]interface{}{
			"source": "attributes", "key_target": "k",
		}},
		{"malformed source path", map[string]interface{}{
			"source": "a..b", "key_target": "k", "value_target": "v",
		}},
		{"invalid on_multiple", map[string]interface{}{
			"source": "attributes", "key_target": "k", "value_target": "v",
			"on_multiple": "sometimes",
		}},
		{"allowed_keys not a list", map[string]interface{}{
			"source": "attributes", "key_target": "k", "value_target": "v",
			"allowed_keys": "3",
		}},
		{"allowed_keys empty", map[string]interface{}{
			"source": "attributes", "key_target": "k", "value_target": "v",
			"allowed_keys": []interface{}{},
		}},
		{"allowed_keys element type", map[string]interface{}{
			"source": "attributes", "key_target": "k", "value_target": "v",
			"allowed_keys": []interface{}{true},
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			step, err := makeExtractMapEntryStep(tc.config)
			assert.Nil(t, step)
			require.Error(t, err)

			var connErr *connectorErrors.ConnectorError
			require.True(t, errors.As(err, &connErr))
			assert.Equal(t, "yaml_parser", connErr.Component)
			assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
		})
	}
}

// TestExtractMapEntrySingleEntry lifts the sole entry: integer key becomes
// int64, value passes through untouched
func TestExtractMapEntrySingleEntry(t *testing.T) {
	state, err := runMapEntryStep(t, mapEntryBaseConfig(), map[string]interface{}{
		"attributes": map[string]interface{}{"3": []byte{0xAA, 0xBB}},
	})
	require.NoError(t, err)

	assert.Equal(t, int64(3), state.Fields["capability_key"])
	assert.Equal(t, []byte{0xAA, 0xBB}, state.Fields["attribute_raw"])
}

// TestExtractMapEntryStringKey keeps non-integer keys as strings
func TestExtractMapEntryStringKey(t *testing.T) {
	state, err := runMapEntryStep(t, mapEntryBaseConfig(), map[string]interface{}{
		"attributes": map[string]interface{}{"temperature": 21.5},
	})
	require.NoError(t, err)

	assert.Equal(t, "temperature", state.Fields["capability_key"])
	assert.Equal(t, 21.5, state.Fields["attribute_raw"])
}

// TestExtractMapEntrySourceErrors covers missing/non-map/empty sources
func TestExtractMapEntrySourceErrors(t *testing.T) {
	cases := []struct {
		name   string
		fields map[string]interface{}
	}{
		{"missing source field", map[string]interface{}{}},
		{"source not a map", map[string]interface{}{"attributes": "not-a-map"}},
		{"empty map", map[string]interface{}{"attributes": map[string]interface{}{}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runMapEntryStep(t, mapEntryBaseConfig(), tc.fields)
			requireParsingFailedError(t, err)
		})
	}
}

// TestExtractMapEntryMultiple: omitted on_multiple fails (silent data loss
// requires explicit opt-in), "fail" fails, "first" picks the
// lexicographically smallest stringified key deterministically
func TestExtractMapEntryMultiple(t *testing.T) {
	multi := func() map[string]interface{} {
		return map[string]interface{}{
			"attributes": map[string]interface{}{"b": 1, "a": 2},
		}
	}

	t.Run("default fails", func(t *testing.T) {
		_, err := runMapEntryStep(t, mapEntryBaseConfig(), multi())
		requireParsingFailedError(t, err)
	})

	t.Run("explicit fail fails", func(t *testing.T) {
		config := mapEntryBaseConfig()
		config["on_multiple"] = "fail"
		_, err := runMapEntryStep(t, config, multi())
		requireParsingFailedError(t, err)
	})

	t.Run("first picks sorted key", func(t *testing.T) {
		config := mapEntryBaseConfig()
		config["on_multiple"] = "first"
		state, err := runMapEntryStep(t, config, multi())
		require.NoError(t, err)
		assert.Equal(t, "a", state.Fields["capability_key"])
		assert.Equal(t, 2, state.Fields["attribute_raw"])
	})

	t.Run("first is lexicographic over stringified keys", func(t *testing.T) {
		config := mapEntryBaseConfig()
		config["on_multiple"] = "first"
		state, err := runMapEntryStep(t, config, map[string]interface{}{
			"attributes": map[string]interface{}{"10": "x", "3": "y"},
		})
		require.NoError(t, err)
		assert.Equal(t, int64(10), state.Fields["capability_key"], "\"10\" < \"3\" lexicographically")
	})
}

// TestExtractMapEntryAllowedKeys: the capability filter - allowed keys pass,
// disallowed keys error (structural drop); config elements match as YAML
// ints and as quoted strings
func TestExtractMapEntryAllowedKeys(t *testing.T) {
	fields := func() map[string]interface{} {
		return map[string]interface{}{
			"attributes": map[string]interface{}{"3": []byte{0x01}},
		}
	}

	t.Run("string element passes", func(t *testing.T) {
		config := mapEntryBaseConfig()
		config["allowed_keys"] = []interface{}{"3"}
		state, err := runMapEntryStep(t, config, fields())
		require.NoError(t, err)
		assert.Equal(t, int64(3), state.Fields["capability_key"])
	})

	t.Run("int element passes", func(t *testing.T) {
		config := mapEntryBaseConfig()
		config["allowed_keys"] = []interface{}{3}
		_, err := runMapEntryStep(t, config, fields())
		require.NoError(t, err)
	})

	t.Run("disallowed key errors", func(t *testing.T) {
		config := mapEntryBaseConfig()
		config["allowed_keys"] = []interface{}{"7"}
		_, err := runMapEntryStep(t, config, fields())
		requireParsingFailedError(t, err)
	})
}

// TestExtractMapEntryUnusableThroughPipeline asserts the drop path: a
// disallowed map key yields OutcomeUnusable, zero tables, nil error
func TestExtractMapEntryUnusableThroughPipeline(t *testing.T) {
	config := YAMLConfig{
		Output: OutputSchema{
			Columns: []ColumnSchema{
				{Name: "capability_key", Type: "int64"},
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
				"allowed_keys": []interface{}{"7"},
			}},
			{Step: "extract_table", Config: map[string]interface{}{"value": "events"}},
		},
	}

	parser, err := NewYAMLParserFromConfig(config)
	require.NoError(t, err)

	payload := marshalEnvelope(t, func(m protoreflect.Message) {
		setBlobsEntry(m, 3, []byte{0xAA})
	})

	res, err := parser.Parse(&nats.Msg{Subject: util.RandomTopicName(), Data: payload})
	requireUnusable(t, res, err)
	assert.True(t, errorsContain(res.Errors, "allowed_keys"))
}

// TestExtractMapEntryProperties pins panic-freedom and the single-entry
// lift invariant against generated inputs
func TestExtractMapEntryProperties(t *testing.T) {
	t.Run("never panics, single entry always lifts", func(t *testing.T) {
		rapid.Check(t, func(rt *rapid.T) {
			key := rapid.String().Draw(rt, "key")
			value := rapid.Int64().Draw(rt, "value")
			onMultiple := rapid.SampledFrom([]string{"first", "fail"}).Draw(rt, "on_multiple")

			config := mapEntryBaseConfig()
			config["on_multiple"] = onMultiple

			state, err := runMapEntryStep(t, config, map[string]interface{}{
				"attributes": map[string]interface{}{key: value},
			})
			require.NoError(rt, err)
			require.Equal(rt, value, state.Fields["attribute_raw"])
		})
	})

	t.Run("arbitrary maps never panic", func(t *testing.T) {
		rapid.Check(t, func(rt *rapid.T) {
			entries := rapid.MapOfN(rapid.String(), rapid.Int64(), 0, 8).Draw(rt, "entries")
			onMultiple := rapid.SampledFrom([]string{"first", "fail"}).Draw(rt, "on_multiple")

			m := make(map[string]interface{}, len(entries))
			for k, v := range entries {
				m[k] = v
			}

			config := mapEntryBaseConfig()
			config["on_multiple"] = onMultiple

			state, err := runMapEntryStep(t, config, map[string]interface{}{"attributes": m})
			if err == nil {
				require.Contains(rt, state.Fields, "capability_key")
				require.Contains(rt, state.Fields, "attribute_raw")
			}
		})
	})
}
