// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package parser: explode step tests
// Ex: TestExplodeConfigErrors → invalid explode configs rejected at load
package parser

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/internal/util"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
	"pgregory.net/rapid"
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

// explodeRuntimeConfig returns a JSON-fed explode config (parse_json arrays
// arrive as []interface{} of float64) with the given interval block.
func explodeRuntimeConfig(interval map[string]interface{}) YAMLConfig {
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
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "stream_id", "target": "stream_id", "type": "string",
			}},
			{Step: "extract_table", Config: map[string]interface{}{"value": "waveform_test"}},
			{Step: "explode", Config: map[string]interface{}{
				"source": "samples", "target": "value", "ordinal": "sample_index",
				"index": map[string]interface{}{
					"start":    map[string]interface{}{"source": "reading_ts"},
					"interval": interval,
				},
			}},
		},
	}
}

// waveformJSON marshals a waveform-shaped JSON payload.
func waveformJSON(t *testing.T, ts, streamID string, samples []float64) []byte {
	t.Helper()

	data, err := json.Marshal(map[string]interface{}{
		"ts": ts, "stream_id": streamID, "samples": samples,
	})
	require.NoError(t, err)

	return data
}

// parseExplodeMsg runs one payload through a fresh parser for the config.
func parseExplodeMsg(t *testing.T, config YAMLConfig, payload []byte) (ParseResult, error) {
	t.Helper()

	parser, err := NewYAMLParserFromConfig(config)
	require.NoError(t, err)

	return parser.Parse(&nats.Msg{Subject: util.RandomTopicName(), Data: payload})
}

// columnDoubles reads a double column back from a WriterTable.
func columnDoubles(t *testing.T, table qdb.WriterTable, col int) []float64 {
	t.Helper()

	cd, err := table.GetData(col)
	require.NoError(t, err)
	xs, err := qdb.GetColumnDataDouble(cd)
	require.NoError(t, err)

	return xs
}

// columnInt64s reads an int64 column back from a WriterTable.
func columnInt64s(t *testing.T, table qdb.WriterTable, col int) []int64 {
	t.Helper()

	cd, err := table.GetData(col)
	require.NoError(t, err)
	xs, err := qdb.GetColumnDataInt64(cd)
	require.NoError(t, err)

	return xs
}

// columnStrings reads a string column back from a WriterTable.
func columnStrings(t *testing.T, table qdb.WriterTable, col int) []string {
	t.Helper()

	cd, err := table.GetData(col)
	require.NoError(t, err)
	xs, err := qdb.GetColumnDataString(cd)
	require.NoError(t, err)

	return xs
}

// TestExplodeBasic: one message becomes one N-row table with exact time
// axis, per-element target, 0-based ordinal, and broadcast metadata, all
// column lengths == N.
func TestExplodeBasic(t *testing.T) {
	config := explodeRuntimeConfig(map[string]interface{}{"value": "250ms"})
	payload := waveformJSON(t, "2026-07-09T20:08:00Z", "abc", []float64{1.5, 2.25, -3.5})

	res, err := parseExplodeMsg(t, config, payload)
	require.NoError(t, err)
	assert.Equal(t, OutcomeOK, res.Outcome)
	require.Len(t, res.Tables, 1)

	table := res.Tables[0]
	assert.Equal(t, "waveform_test", stripNullTerminator(table.GetName()))
	require.Equal(t, 3, table.RowCount())

	start := time.Date(2026, 7, 9, 20, 8, 0, 0, time.UTC)
	idx := table.GetIndex()
	require.Len(t, idx, 3)
	assert.True(t, idx[0].Equal(start), "t[0] must equal start exactly")
	assert.True(t, idx[1].Equal(start.Add(250*time.Millisecond)))
	assert.True(t, idx[2].Equal(start.Add(500*time.Millisecond)))

	assert.Equal(t, []float64{1.5, 2.25, -3.5}, columnDoubles(t, table, 0))
	assert.Equal(t, []int64{0, 1, 2}, columnInt64s(t, table, 1))
	assert.Equal(t, []string{"abc", "abc", "abc"}, columnStrings(t, table, 2))
}

// TestExplodeTimestampMultiplicationExact: at a non-dyadic ~3 kHz interval
// (333333ns; exact 3 kHz is not representable in integer nanoseconds) every
// timestamp equals start + i*interval EXACTLY, and a float32-seconds
// accumulation reference diverges by more than a microsecond over 8192
// samples -- the drift the multiplication contract prevents.
func TestExplodeTimestampMultiplicationExact(t *testing.T) {
	const interval = 333333 * time.Nanosecond

	config := explodeRuntimeConfig(map[string]interface{}{"value": "333333ns"})
	samples := make([]float64, 8192)
	payload := waveformJSON(t, "2026-07-09T20:08:00Z", "abc", samples)

	res, err := parseExplodeMsg(t, config, payload)
	require.NoError(t, err)
	require.Len(t, res.Tables, 1)

	start := time.Date(2026, 7, 9, 20, 8, 0, 0, time.UTC)
	idx := res.Tables[0].GetIndex()
	require.Len(t, idx, 8192)

	for i, ts := range idx {
		require.True(t, ts.Equal(start.Add(time.Duration(i)*interval)),
			"t[%d] must be start + %d*interval exactly", i, i)
	}

	var acc float32
	for range 8191 {
		acc += float32(interval.Seconds())
	}
	accumulated := start.Add(time.Duration(float64(acc) * float64(time.Second)))
	drift := accumulated.Sub(idx[8191])
	if drift < 0 {
		drift = -drift
	}
	assert.Greater(t, drift, time.Microsecond,
		"float32 accumulation must visibly drift; multiplication must not")
}

// TestExplodeTimestampMonotonicityProperty: for any start, positive
// interval, and n, the derived axis starts at start, is strictly
// increasing, and has a constant first difference equal to interval.
func TestExplodeTimestampMonotonicityProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(1, 8192).Draw(rt, "n")
		interval := time.Duration(rapid.Int64Range(1, int64(time.Second)).Draw(rt, "interval"))
		start := time.Unix(
			rapid.Int64Range(0, 4e9).Draw(rt, "sec"),
			rapid.Int64Range(0, 999999999).Draw(rt, "nsec"),
		).UTC()

		idx := buildExplodedIndex(start, interval, n)
		require.Len(rt, idx, n)
		require.True(rt, idx[0].Equal(start))

		for i := 1; i < n; i++ {
			require.Equal(rt, interval, idx[i].Sub(idx[i-1]))
		}
	})
}

// TestExplodeIntervalBySource: sourced intervals resolve per message with
// mandatory unit; non-positive, non-finite, and mistyped sources are
// structural.
func TestExplodeIntervalBySource(t *testing.T) {
	config := explodeRuntimeConfig(map[string]interface{}{"source": "dt", "unit": "us"})

	t.Run("float64 source", func(t *testing.T) {
		payload := []byte(`{"ts":"2026-07-09T20:08:00Z","stream_id":"abc","dt":250.0,"samples":[1.0,2.0]}`)
		res, err := parseExplodeMsg(t, config, payload)
		require.NoError(t, err)
		require.Len(t, res.Tables, 1)

		idx := res.Tables[0].GetIndex()
		assert.Equal(t, 250*time.Microsecond, idx[1].Sub(idx[0]))
	})

	t.Run("structural failures", func(t *testing.T) {
		for name, dt := range map[string]string{
			"missing":      `{"ts":"2026-07-09T20:08:00Z","stream_id":"a","samples":[1.0]}`,
			"zero":         `{"ts":"2026-07-09T20:08:00Z","stream_id":"a","dt":0,"samples":[1.0]}`,
			"negative":     `{"ts":"2026-07-09T20:08:00Z","stream_id":"a","dt":-2.5,"samples":[1.0]}`,
			"string typed": `{"ts":"2026-07-09T20:08:00Z","stream_id":"a","dt":"250","samples":[1.0]}`,
		} {
			t.Run(name, func(t *testing.T) {
				res, err := parseExplodeMsg(t, config, []byte(dt))
				requireUnusable(t, res, err)
				assert.True(t, errorsContain(res.Errors, "index.interval"))
			})
		}
	})

	t.Run("int64 and unit conversion", func(t *testing.T) {
		parser, err := NewYAMLParserFromConfig(config)
		require.NoError(t, err)
		spec := parser.explode

		d, err := spec.resolveInterval(map[string]interface{}{"dt": int64(250)}, 1)
		require.NoError(t, err)
		assert.Equal(t, 250*time.Microsecond, d)

		_, err = spec.resolveInterval(map[string]interface{}{"dt": int64(0)}, 1)
		require.Error(t, err)

		_, err = spec.resolveInterval(map[string]interface{}{"dt": math.NaN()}, 1)
		require.Error(t, err)

		// int64 overflow guard: value * unit would exceed MaxInt64 ns.
		_, err = spec.resolveInterval(map[string]interface{}{"dt": int64(math.MaxInt64/int64(time.Microsecond) + 1)}, 1)
		require.Error(t, err)
	})
}

// TestExplodeIntervalByLength: the array length selects the interval; an
// unmapped length is structural (never guess a time axis).
func TestExplodeIntervalByLength(t *testing.T) {
	config := explodeRuntimeConfig(map[string]interface{}{
		"by_length": map[string]interface{}{"3": "100ms", "2": "1s"},
	})

	t.Run("mapped lengths resolve", func(t *testing.T) {
		res, err := parseExplodeMsg(t, config,
			waveformJSON(t, "2026-07-09T20:08:00Z", "abc", []float64{1, 2, 3}))
		require.NoError(t, err)
		require.Len(t, res.Tables, 1)
		idx := res.Tables[0].GetIndex()
		assert.Equal(t, 100*time.Millisecond, idx[1].Sub(idx[0]))

		res, err = parseExplodeMsg(t, config,
			waveformJSON(t, "2026-07-09T20:08:00Z", "abc", []float64{1, 2}))
		require.NoError(t, err)
		require.Len(t, res.Tables, 1)
		idx = res.Tables[0].GetIndex()
		assert.Equal(t, time.Second, idx[1].Sub(idx[0]))
	})

	t.Run("unmapped length is structural", func(t *testing.T) {
		res, err := parseExplodeMsg(t, config,
			waveformJSON(t, "2026-07-09T20:08:00Z", "abc", []float64{1, 2, 3, 4}))
		requireUnusable(t, res, err)
		assert.True(t, errorsContain(res.Errors, "no entry for array length 4"))
	})
}

// TestExplodeEmptyArrayZeroRowsOK: an empty sample array yields zero rows
// and OutcomeOK -- the message ACKs; fabricating a null sample at t0 would
// be worse than no row (ADR-012).
func TestExplodeEmptyArrayZeroRowsOK(t *testing.T) {
	config := explodeRuntimeConfig(map[string]interface{}{"value": "250ms"})
	res, err := parseExplodeMsg(t, config,
		waveformJSON(t, "2026-07-09T20:08:00Z", "abc", []float64{}))
	require.NoError(t, err)
	assert.Equal(t, OutcomeOK, res.Outcome)
	assert.Empty(t, res.Tables)
	assert.Empty(t, res.Errors)
}

// TestExplodeStructuralInputFailures: missing/mistyped source array, start,
// or per-element types produce OutcomeUnusable with zero tables.
func TestExplodeStructuralInputFailures(t *testing.T) {
	config := explodeRuntimeConfig(map[string]interface{}{"value": "250ms"})

	cases := map[string]struct {
		payload string
		want    string
	}{
		"missing source array": {
			`{"ts":"2026-07-09T20:08:00Z","stream_id":"a"}`,
			"source field 'samples' not found",
		},
		"source not an array": {
			`{"ts":"2026-07-09T20:08:00Z","stream_id":"a","samples":"x"}`,
			"want a numeric array",
		},
		"mistyped element": {
			`{"ts":"2026-07-09T20:08:00Z","stream_id":"a","samples":[1.5,"x"]}`,
			"element 1 has type string",
		},
		"missing start": {
			`{"stream_id":"a","samples":[1.5]}`,
			"index.start field 'reading_ts' not found",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			res, err := parseExplodeMsg(t, config, []byte(tc.payload))
			requireUnusable(t, res, err)
			assert.True(t, errorsContain(res.Errors, tc.want),
				"errors %v must mention %q", res.Errors, tc.want)
		})
	}

	t.Run("start wrong type", func(t *testing.T) {
		badStart := explodeRuntimeConfig(map[string]interface{}{"value": "250ms"})
		stepCfg := badStart.Transformations[len(badStart.Transformations)-1].Config
		stepCfg["index"].(map[string]interface{})["start"] = map[string]interface{}{"source": "stream_id"}

		res, err := parseExplodeMsg(t, badStart,
			waveformJSON(t, "2026-07-09T20:08:00Z", "abc", []float64{1.5}))
		requireUnusable(t, res, err)
		assert.True(t, errorsContain(res.Errors, "must be a time.Time"))
	})
}

// TestExplodeBroadcastMissingFieldPartial: a missing broadcast field keeps
// the row structure (N sentinel copies) and classifies OutcomePartial --
// unchanged in kind from the single-row sentinel-fill invariant.
func TestExplodeBroadcastMissingFieldPartial(t *testing.T) {
	config := explodeRuntimeConfig(map[string]interface{}{"value": "250ms"})
	payload := []byte(`{"ts":"2026-07-09T20:08:00Z","samples":[1.5,2.5]}`)

	res, err := parseExplodeMsg(t, config, payload)
	require.NoError(t, err)
	assert.Equal(t, OutcomePartial, res.Outcome)
	require.NotEmpty(t, res.Errors)
	require.Len(t, res.Tables, 1)

	table := res.Tables[0]
	require.Equal(t, 2, table.RowCount())
	assert.Equal(t, []string{"", ""}, columnStrings(t, table, 2))
	assert.Equal(t, []float64{1.5, 2.5}, columnDoubles(t, table, 0))
}

// TestExplodeInt64ArrayNoScale: the []int64 path (e.g. unscaled unpack)
// materializes into an int64 target column; typed-slice/column mismatches
// are structural. White-box via parseExploded: the scalar pipeline cannot
// produce []int64 from JSON.
func TestExplodeInt64ArrayNoScale(t *testing.T) {
	config := explodeRuntimeConfig(map[string]interface{}{"value": "250ms"})
	config.Output.Columns[0].Type = "int64" // value column now int64

	parser, err := NewYAMLParserFromConfig(config)
	require.NoError(t, err)

	start := time.Date(2026, 7, 9, 20, 8, 0, 0, time.UTC)

	t.Run("int64 array materializes", func(t *testing.T) {
		state := parser.newParseState()
		state.Fields["samples"] = []int64{5, -7}
		state.Fields["reading_ts"] = start
		state.Fields["stream_id"] = "abc"

		res, err := parser.parseExploded(state, "waveform_test\x00")
		require.NoError(t, err)
		assert.Equal(t, OutcomeOK, res.Outcome)
		require.Len(t, res.Tables, 1)

		table := res.Tables[0]
		require.Equal(t, 2, table.RowCount())
		assert.Equal(t, []int64{5, -7}, columnInt64s(t, table, 0))
		assert.Equal(t, []int64{0, 1}, columnInt64s(t, table, 1))
		assert.True(t, table.GetIndex()[0].Equal(start))
	})

	t.Run("float64 array into int64 column is structural", func(t *testing.T) {
		state := parser.newParseState()
		state.Fields["samples"] = []float64{1.5}
		state.Fields["reading_ts"] = start

		res, err := parser.parseExploded(state, "waveform_test\x00")
		requireUnusable(t, res, err)
		assert.True(t, errorsContain(res.Errors, "must be []int64 to match column"))
	})
}
