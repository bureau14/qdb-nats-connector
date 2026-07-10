// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package parser: explode step tests
// Ex: TestExplodeConfigErrors → invalid explode configs rejected at load
package parser

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// explodeTestConfig returns a compilable waveform-shaped config: unpack
// (scaled int16 -> []float64) exploded into a double column with ordinal
// and broadcast metadata. Tests mutate copies to trigger specific errors.
func explodeTestConfig() YAMLConfig {
	return YAMLConfig{
		Output: OutputSchema{Columns: []ColumnSchema{
			{Name: "value", Type: "double"},
			{Name: "sample_index", Type: "int64"},
			{Name: "stream_id", Type: "string"},
		}},
		Transformations: []TransformSpec{
			{Step: "parse_json", Config: map[string]interface{}{}},
			{Step: "extract_timestamp", Config: map[string]interface{}{
				"source": "ts", "target": "reading_ts", "format": "rfc3339",
			}},
			{Step: "unpack", Config: map[string]interface{}{
				"source": "payload", "type": "int16", "endianness": "little",
				"scale":  map[string]interface{}{"value": 0.5},
				"target": "samples",
			}},
			{Step: "extract_table", Config: map[string]interface{}{"value": "waveform_test"}},
			{Step: "explode", Config: explodeStepConfig()},
		},
	}
}

// explodeStepConfig returns a valid explode step config.
func explodeStepConfig() map[string]interface{} {
	return map[string]interface{}{
		"source":  "samples",
		"target":  "value",
		"ordinal": "sample_index",
		"index": map[string]interface{}{
			"start":    map[string]interface{}{"source": "reading_ts"},
			"interval": map[string]interface{}{"value": "200us"},
		},
	}
}

// withExplodeConfig returns explodeTestConfig with the explode step config
// transformed by fn.
func withExplodeConfig(fn func(map[string]interface{})) YAMLConfig {
	config := explodeTestConfig()
	stepCfg := config.Transformations[len(config.Transformations)-1].Config
	fn(stepCfg)

	return config
}

// TestExplodeConfigErrors validates compile-time rejection of malformed
// explode configs.
func TestExplodeConfigErrors(t *testing.T) {
	cases := []struct {
		name   string
		config YAMLConfig
		want   string
	}{
		{"explode not last", func() YAMLConfig {
			config := explodeTestConfig()
			n := len(config.Transformations)
			config.Transformations[n-1], config.Transformations[n-2] = config.Transformations[n-2], config.Transformations[n-1]

			return config
		}(), "must be the last transformation step"},
		{"multiple explodes", func() YAMLConfig {
			config := explodeTestConfig()
			config.Transformations = append(config.Transformations,
				TransformSpec{Step: "explode", Config: explodeStepConfig()})

			return config
		}(), "at most one explode step"},
		{"with extract_index", func() YAMLConfig {
			config := explodeTestConfig()
			n := len(config.Transformations)
			extra := TransformSpec{Step: "extract_index", Config: map[string]interface{}{"source": "ts"}}
			config.Transformations = append(config.Transformations[:n-1], extra, config.Transformations[n-1])

			return config
		}(), "mutually exclusive"},
		{"missing source", withExplodeConfig(func(c map[string]interface{}) {
			delete(c, "source")
		}), "requires a 'source'"},
		{"bad source path", withExplodeConfig(func(c map[string]interface{}) {
			c["source"] = "a..b"
		}), "not a valid field path"},
		{"missing target", withExplodeConfig(func(c map[string]interface{}) {
			delete(c, "target")
		}), "requires a 'target'"},
		{"dollar target", withExplodeConfig(func(c map[string]interface{}) {
			c["target"] = "$value"
		}), "must not start with '$'"},
		{"target not a column", withExplodeConfig(func(c map[string]interface{}) {
			c["target"] = "nope"
		}), "must reference an output column"},
		{"target wrong type", withExplodeConfig(func(c map[string]interface{}) {
			c["target"] = "stream_id"
			c["source"] = "other" // avoid the unpack trace firing first
		}), "must be a double or int64 column"},
		{"ordinal not a column", withExplodeConfig(func(c map[string]interface{}) {
			c["ordinal"] = "nope"
		}), "'ordinal' must reference an output column"},
		{"ordinal wrong type", withExplodeConfig(func(c map[string]interface{}) {
			c["ordinal"] = "stream_id"
		}), "must be an int64 column"},
		{"ordinal equals target", withExplodeConfig(func(c map[string]interface{}) {
			c["target"] = "sample_index"
			c["ordinal"] = "sample_index"
			c["source"] = "other"
		}), "must be different columns"},
		{"dollar ordinal", withExplodeConfig(func(c map[string]interface{}) {
			c["ordinal"] = "$sample_index"
		}), "'ordinal' must be a non-'$' output column name"},
		{"missing index", withExplodeConfig(func(c map[string]interface{}) {
			delete(c, "index")
		}), "requires an 'index' map"},
		{"missing start", withExplodeConfig(func(c map[string]interface{}) {
			delete(c["index"].(map[string]interface{}), "start")
		}), "index.start requires 'source'"},
		{"empty start source", withExplodeConfig(func(c map[string]interface{}) {
			c["index"].(map[string]interface{})["start"] = map[string]interface{}{"source": ""}
		}), "index.start requires 'source'"},
		{"missing interval", withExplodeConfig(func(c map[string]interface{}) {
			delete(c["index"].(map[string]interface{}), "interval")
		}), "exactly one of 'value', 'source', or 'by_length'"},
		{"interval two modes", withExplodeConfig(func(c map[string]interface{}) {
			c["index"].(map[string]interface{})["interval"] = map[string]interface{}{
				"value": "200us", "by_length": map[string]interface{}{"8192": "200us"},
			}
		}), "exactly one of 'value', 'source', or 'by_length'"},
		{"interval value non-positive", withExplodeConfig(func(c map[string]interface{}) {
			c["index"].(map[string]interface{})["interval"] = map[string]interface{}{"value": "0s"}
		}), "must be a positive Go duration"},
		{"interval value negative", withExplodeConfig(func(c map[string]interface{}) {
			c["index"].(map[string]interface{})["interval"] = map[string]interface{}{"value": "-1us"}
		}), "must be a positive Go duration"},
		{"interval value not a string", withExplodeConfig(func(c map[string]interface{}) {
			c["index"].(map[string]interface{})["interval"] = map[string]interface{}{"value": 200}
		}), "must be a positive Go duration string"},
		{"interval source without unit", withExplodeConfig(func(c map[string]interface{}) {
			c["index"].(map[string]interface{})["interval"] = map[string]interface{}{"source": "dt"}
		}), "requires 'unit'"},
		{"interval unit without source", withExplodeConfig(func(c map[string]interface{}) {
			c["index"].(map[string]interface{})["interval"] = map[string]interface{}{
				"value": "200us", "unit": "us",
			}
		}), "'unit' is only valid with 'source'"},
		{"interval bad unit", withExplodeConfig(func(c map[string]interface{}) {
			c["index"].(map[string]interface{})["interval"] = map[string]interface{}{
				"source": "dt", "unit": "minutes",
			}
		}), "unit must be one of ns, us, ms, s"},
		{"by_length empty", withExplodeConfig(func(c map[string]interface{}) {
			c["index"].(map[string]interface{})["interval"] = map[string]interface{}{
				"by_length": map[string]interface{}{},
			}
		}), "non-empty map"},
		{"by_length bad key", withExplodeConfig(func(c map[string]interface{}) {
			c["index"].(map[string]interface{})["interval"] = map[string]interface{}{
				"by_length": map[string]interface{}{"abc": "200us"},
			}
		}), "keys must be positive integers"},
		{"by_length non-positive key", withExplodeConfig(func(c map[string]interface{}) {
			c["index"].(map[string]interface{})["interval"] = map[string]interface{}{
				"by_length": map[interface{}]interface{}{0: "200us"},
			}
		}), "keys must be positive integers"},
		{"by_length bad duration", withExplodeConfig(func(c map[string]interface{}) {
			c["index"].(map[string]interface{})["interval"] = map[string]interface{}{
				"by_length": map[interface{}]interface{}{8192: "0s"},
			}
		}), "must be a positive Go duration"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parser, err := NewYAMLParserFromConfig(tc.config)
			assert.Nil(t, parser)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

// TestExplodeUnpackTypeTraceConfigError: static element-vs-column type
// mismatches and unbound array outputs are config-load errors.
func TestExplodeUnpackTypeTraceConfigError(t *testing.T) {
	t.Run("unscaled unpack bound to double column", func(t *testing.T) {
		config := explodeTestConfig()
		// Drop the scale: unpack now produces []int64, target stays double.
		delete(config.Transformations[2].Config, "scale")

		parser, err := NewYAMLParserFromConfig(config)
		assert.Nil(t, parser)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `column "value" is double but unpack "samples" produces int64`)
	})

	t.Run("float unpack bound to int64 column", func(t *testing.T) {
		config := withExplodeConfig(func(c map[string]interface{}) {
			c["target"] = "sample_index"
			delete(c, "ordinal")
		})
		config.Transformations[2].Config["type"] = "float64"
		delete(config.Transformations[2].Config, "scale")

		parser, err := NewYAMLParserFromConfig(config)
		assert.Nil(t, parser)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "produces double")
	})

	t.Run("unpack target on output column without explode", func(t *testing.T) {
		config := explodeTestConfig()
		config.Transformations = config.Transformations[:len(config.Transformations)-1]
		// unpack target "samples" is not an output column: fine.
		parser, err := NewYAMLParserFromConfig(config)
		require.NoError(t, err)
		assert.NotNil(t, parser)

		// Retarget unpack at the "value" output column: rejected.
		config.Transformations[2].Config["target"] = "value"
		parser, err = NewYAMLParserFromConfig(config)
		assert.Nil(t, parser)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no explode binds it")
	})
}

// TestExplodeConfigCompiles: valid variants compile, exploded column names
// are exposed, and both YAML by_length key spellings work end-to-end
// through yaml.Unmarshal (integer keys arrive as int, quoted keys as
// string).
func TestExplodeConfigCompiles(t *testing.T) {
	t.Run("full config compiles", func(t *testing.T) {
		parser, err := NewYAMLParserFromConfig(explodeTestConfig())
		require.NoError(t, err)
		require.NotNil(t, parser.explode)
		assert.Equal(t, []string{"value", "sample_index"}, explodedColumnNames(parser.explode))
		assert.Equal(t, intervalModeValue, parser.explode.mode)
		assert.False(t, parser.hasIndexStep)
	})

	t.Run("ordinal optional", func(t *testing.T) {
		config := withExplodeConfig(func(c map[string]interface{}) { delete(c, "ordinal") })
		parser, err := NewYAMLParserFromConfig(config)
		require.NoError(t, err)
		assert.Equal(t, -1, parser.explode.ordinalCol)
		assert.Equal(t, []string{"value"}, explodedColumnNames(parser.explode))
	})

	t.Run("interval source with unit", func(t *testing.T) {
		config := withExplodeConfig(func(c map[string]interface{}) {
			c["index"].(map[string]interface{})["interval"] = map[string]interface{}{
				"source": "dt", "unit": "us",
			}
		})
		parser, err := NewYAMLParserFromConfig(config)
		require.NoError(t, err)
		assert.Equal(t, intervalModeSource, parser.explode.mode)
	})

	t.Run("by_length both yaml key spellings", func(t *testing.T) {
		const doc = `
output:
  columns:
    - name: "value"
      type: "double"
transformations:
  - step: "parse_json"
  - step: "extract_timestamp"
    config: { source: "ts", target: "reading_ts", format: "rfc3339" }
  - step: "unpack"
    config:
      source: "payload"
      type: "int16"
      scale: { value: 0.5 }
      target: "samples"
  - step: "extract_table"
    config: { value: "waveform_test" }
  - step: "explode"
    config:
      source: "samples"
      target: "value"
      index:
        start: { source: "reading_ts" }
        interval:
          by_length: { 8192: "200us", "3200": "312.5us" }
`
		var config YAMLConfig
		require.NoError(t, yaml.Unmarshal([]byte(doc), &config))

		parser, err := NewYAMLParserFromConfig(config)
		require.NoError(t, err)
		require.Equal(t, intervalModeByLength, parser.explode.mode)
		assert.Len(t, parser.explode.intervalByLength, 2)
		assert.Equal(t, 200*time.Microsecond, parser.explode.intervalByLength[8192])
		assert.Equal(t, 312500*time.Nanosecond, parser.explode.intervalByLength[3200])
	})

	t.Run("scalar config keeps nil explode", func(t *testing.T) {
		config := explodeTestConfig()
		config.Transformations = config.Transformations[:len(config.Transformations)-1]
		parser, err := NewYAMLParserFromConfig(config)
		require.NoError(t, err)
		assert.Nil(t, parser.explode)
		assert.Nil(t, explodedColumnNames(nil))
	})
}
