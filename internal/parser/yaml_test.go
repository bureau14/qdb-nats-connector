// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package parser: YAML-based message transformation pipelines
// Types: YAMLParser, YAMLConfig, ParseState
// Ex: NewYAMLParser(opts).Parse(msg) → []WriterTable
package parser

import (
	"bytes"
	"compress/gzip"
	"crypto/sha1" //nolint:gosec // reference digests for the sharding scheme, not cryptographic use
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/internal/filter"
	"github.com/bureau14/qdb-nats-connector/internal/util"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// stripNullTerminator removes trailing null byte from string if present
func stripNullTerminator(s string) string {
	if s != "" && s[len(s)-1] == 0 {
		return s[:len(s)-1]
	}

	return s
}

// requireUnusable asserts a structural drop: nil error, OutcomeUnusable,
// zero tables, at least one recorded step error.
func requireUnusable(t *testing.T, res ParseResult, err error) {
	t.Helper()
	require.NoError(t, err)
	assert.Equal(t, OutcomeUnusable, res.Outcome)
	assert.Empty(t, res.Tables)
	require.NotEmpty(t, res.Errors)
}

// errorsContain reports whether any recorded step error mentions substr.
func errorsContain(errs []error, substr string) bool {
	for _, e := range errs {
		if strings.Contains(e.Error(), substr) {
			return true
		}
	}

	return false
}

// TestYAMLParserConfigurationErrors validates config error handling
func TestYAMLParserConfigurationErrors(t *testing.T) {
	t.Run("missing config file", func(t *testing.T) {
		parser, err := NewYAMLParser("nonexistent.yaml")
		assert.Nil(t, parser)
		require.Error(t, err)

		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, "yaml_parser", connErr.Component)
		assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
	})

	t.Run("invalid yaml", func(t *testing.T) {
		// Create temporary invalid YAML file
		tempFile, err := os.CreateTemp("", "invalid.yaml")
		require.NoError(t, err)
		defer func() {
			err := os.Remove(tempFile.Name())
			if err != nil {
				t.Logf("Failed to remove temp file: %v", err)
			}
		}()

		_, err = tempFile.WriteString("invalid: yaml: content: [}")
		require.NoError(t, err)
		err = tempFile.Close()
		require.NoError(t, err)

		parser, err := NewYAMLParser(tempFile.Name())
		assert.Nil(t, parser)
		require.Error(t, err)

		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, "yaml_parser", connErr.Component)
		assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
	})

	t.Run("missing extract_table step", func(t *testing.T) {
		config := YAMLConfig{
			Output: OutputSchema{
				Columns: []ColumnSchema{{Name: "col1", Type: "double"}},
			},
			Transformations: []TransformSpec{
				{Step: "parse_json"},
				// Missing extract_table step
			},
		}

		parser, err := NewYAMLParserFromConfig(config)
		assert.Nil(t, parser)
		require.Error(t, err)

		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, "yaml_parser", connErr.Component)
		assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
	})

	t.Run("no columns", func(t *testing.T) {
		config := YAMLConfig{
			Output: OutputSchema{
				Columns: []ColumnSchema{},
			},
			Transformations: []TransformSpec{
				{Step: "extract_table", Config: map[string]interface{}{"value": "test"}},
			},
		}

		parser, err := NewYAMLParserFromConfig(config)
		assert.Nil(t, parser)
		require.Error(t, err)

		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, "yaml_parser", connErr.Component)
		assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
	})

	t.Run("duplicate column names", func(t *testing.T) {
		config := YAMLConfig{
			Output: OutputSchema{
				Columns: []ColumnSchema{
					{Name: "col1", Type: "double"},
					{Name: "col1", Type: "string"},
				},
			},
			Transformations: []TransformSpec{
				{Step: "extract_table", Config: map[string]interface{}{"value": "test"}},
			},
		}

		parser, err := NewYAMLParserFromConfig(config)
		assert.Nil(t, parser)
		require.Error(t, err)

		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, "yaml_parser", connErr.Component)
		assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
	})

	t.Run("invalid column type", func(t *testing.T) {
		config := YAMLConfig{
			Output: OutputSchema{
				Columns: []ColumnSchema{
					{Name: "col1", Type: "invalid_type"},
				},
			},
			Transformations: []TransformSpec{
				{Step: "extract_table", Config: map[string]interface{}{"value": "test"}},
			},
		}

		parser, err := NewYAMLParserFromConfig(config)
		assert.Nil(t, parser)
		require.Error(t, err)

		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, "yaml_parser", connErr.Component)
		assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
	})

	t.Run("empty column name", func(t *testing.T) {
		config := YAMLConfig{
			Output: OutputSchema{
				Columns: []ColumnSchema{
					{Name: "", Type: "double"},
				},
			},
		}

		parser, err := NewYAMLParserFromConfig(config)
		assert.Nil(t, parser)
		require.Error(t, err)

		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, "yaml_parser", connErr.Component)
		assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
	})

	t.Run("unknown transformation step", func(t *testing.T) {
		config := YAMLConfig{
			Output: OutputSchema{
				Columns: []ColumnSchema{{Name: "col1", Type: "double"}},
			},
			Transformations: []TransformSpec{
				{Step: "unknown_step", Config: map[string]interface{}{}},
			},
		}

		parser, err := NewYAMLParserFromConfig(config)
		assert.Nil(t, parser)
		require.Error(t, err)

		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, "yaml_parser", connErr.Component)
		assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
	})

	t.Run("empty pipeline", func(t *testing.T) {
		config := YAMLConfig{
			Output: OutputSchema{
				Columns: []ColumnSchema{{Name: "col1", Type: "double"}},
			},
			Transformations: []TransformSpec{},
		}

		parser, err := NewYAMLParserFromConfig(config)
		assert.Nil(t, parser)
		require.Error(t, err)

		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, "yaml_parser", connErr.Component)
		assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
	})
}

// TestYAMLParserInvalidInputs tests error cases for message parsing
func TestYAMLParserInvalidInputs(t *testing.T) {
	config := YAMLConfig{
		Output: OutputSchema{
			Columns: []ColumnSchema{{Name: "value", Type: "double"}},
		},
		Transformations: []TransformSpec{
			{Step: "parse_json", Config: map[string]interface{}{}},
			{Step: "extract_table", Config: map[string]interface{}{"value": "test_table"}},
		},
	}

	parser, err := NewYAMLParserFromConfig(config)
	require.NoError(t, err)

	// Test nil message handling - should return parsing error
	t.Run("nil message", func(t *testing.T) {
		res, err := parser.Parse(nil)
		tables := res.Tables
		assert.Nil(t, tables)
		require.Error(t, err)

		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, "yaml_parser", connErr.Component)
		assert.Equal(t, connectorErrors.ErrCodeParsingFailed, connErr.Code)
	})

	// Empty message data - structurally unusable, never a NACK loop
	t.Run("empty message", func(t *testing.T) {
		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte{},
		}

		res, err := parser.Parse(msg)
		requireUnusable(t, res, err)
		assert.True(t, errorsContain(res.Errors, "empty message data"))
	})

	// Test invalid JSON parsing - structural failure, classified unusable
	t.Run("invalid json", func(t *testing.T) {
		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"invalid": json`),
		}

		res, err := parser.Parse(msg)
		requireUnusable(t, res, err)
	})
}

// TestYAMLParserValidParsing validates successful transformations
func TestYAMLParserValidParsing(t *testing.T) {
	config := YAMLConfig{
		Output: OutputSchema{
			Columns: []ColumnSchema{
				{Name: "temperature", Type: "double"},
				{Name: "humidity", Type: "double"},
				{Name: "location", Type: "string"},
			},
		},
		Transformations: []TransformSpec{
			{Step: "parse_json", Config: map[string]interface{}{}},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "temp",
				"target": "temperature",
				"type":   "float64",
			}},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "humid",
				"target": "humidity",
				"type":   "float64",
			}},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "loc",
				"target": "location",
				"type":   "string",
			}},
			{Step: "extract_table", Config: map[string]interface{}{"value": "sensors"}},
		},
	}

	parser, err := NewYAMLParserFromConfig(config)
	require.NoError(t, err)

	// Test basic JSON parsing with field extraction
	t.Run("simple parsing", func(t *testing.T) {
		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"temp": 23.5, "humid": 65.2, "loc": "kitchen"}`),
		}

		res, err := parser.Parse(msg)
		tables := res.Tables
		require.NoError(t, err)
		require.Len(t, tables, 1)

		table := tables[0]
		assert.Equal(t, "sensors", stripNullTerminator(table.GetName()))
		assert.Equal(t, 1, table.RowCount())
	})

	// Property-based test with random valid inputs
	t.Run("property-based valid parsing", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			temp := rapid.Float64Range(-100, 100).Draw(t, "temperature")
			humid := rapid.Float64Range(0, 100).Draw(t, "humidity")
			location := rapid.String().Draw(t, "location")

			// json.Marshal guarantees valid JSON for arbitrary strings;
			// %q produces Go escapes (\U...) that JSON does not accept.
			jsonData, marshalErr := json.Marshal(map[string]interface{}{
				"temp": temp, "humid": humid, "loc": location,
			})
			require.NoError(t, marshalErr)
			msg := &nats.Msg{
				Subject: util.RandomTopicName(),
				Data:    jsonData,
			}

			res, err := parser.Parse(msg)
			tables := res.Tables
			require.NoError(t, err)
			require.Len(t, tables, 1)

			table := tables[0]
			assert.Equal(t, "sensors", stripNullTerminator(table.GetName()))
			assert.Equal(t, 1, table.RowCount())
		})
	})
}

// TestYAMLParserTimestamp validates timestamp extraction
func TestYAMLParserTimestamp(t *testing.T) {
	config := YAMLConfig{
		Output: OutputSchema{
			Columns: []ColumnSchema{
				{Name: "timestamp", Type: "timestamp"},
				{Name: "value", Type: "double"},
			},
		},
		Transformations: []TransformSpec{
			{Step: "parse_json", Config: map[string]interface{}{}},
			{Step: "extract_index", Config: map[string]interface{}{
				"source": "ts",
				"format": "rfc3339",
			}},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "val",
				"target": "value",
				"type":   "float64",
			}},
			{Step: "extract_table", Config: map[string]interface{}{"value": "events"}},
		},
	}

	parser, err := NewYAMLParserFromConfig(config)
	require.NoError(t, err)

	// Test RFC3339 format timestamp parsing
	t.Run("rfc3339 timestamp", func(t *testing.T) {
		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"ts": "2024-01-01T12:00:00Z", "val": 42.5}`),
		}

		res, err := parser.Parse(msg)
		tables := res.Tables
		require.NoError(t, err)
		require.Len(t, tables, 1)

		table := tables[0]
		assert.Equal(t, "events", stripNullTerminator(table.GetName()))
		assert.Equal(t, 1, table.RowCount())
	})

	// Test unix epoch timestamp parsing
	t.Run("unix timestamp", func(t *testing.T) {
		unixConfig := config
		unixConfig.Transformations[1].Config["format"] = "unix"

		unixParser, err := NewYAMLParserFromConfig(unixConfig)
		require.NoError(t, err)

		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"ts": 1704110400, "val": 42.5}`),
		}

		res, err := unixParser.Parse(msg)
		tables := res.Tables
		require.NoError(t, err)
		require.Len(t, tables, 1)

		table := tables[0]
		assert.Equal(t, "events", stripNullTerminator(table.GetName()))
		assert.Equal(t, 1, table.RowCount())
	})

	// Test millisecond epoch timestamp parsing
	t.Run("unix_ms timestamp", func(t *testing.T) {
		msConfig := config
		msConfig.Transformations[1].Config["format"] = "unix_ms"

		msParser, err := NewYAMLParserFromConfig(msConfig)
		require.NoError(t, err)

		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"ts": 1704110400123, "val": 42.5}`),
		}

		res, err := msParser.Parse(msg)
		require.NoError(t, err)
		assert.Equal(t, OutcomeOK, res.Outcome)
		require.Len(t, res.Tables, 1)
	})

	// Test nanosecond epoch timestamp parsing. 1704110400000000000 is exactly
	// representable as float64 (divisible by 512), so the JSON float64 path is
	// lossless for this value.
	t.Run("unix_ns timestamp", func(t *testing.T) {
		nsConfig := config
		nsConfig.Transformations[1].Config["format"] = "unix_ns"

		nsParser, err := NewYAMLParserFromConfig(nsConfig)
		require.NoError(t, err)

		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"ts": 1704110400000000000, "val": 42.5}`),
		}

		res, err := nsParser.Parse(msg)
		require.NoError(t, err)
		assert.Equal(t, OutcomeOK, res.Outcome)
		require.Len(t, res.Tables, 1)
	})

	// Regression pin: numeric input with a date format is an error, never an
	// implicit unix-seconds interpretation. For extract_index that makes the
	// message structurally unusable.
	t.Run("numeric with date format is unusable", func(t *testing.T) {
		dateConfig := config
		dateConfig.Transformations[1].Config["format"] = "rfc3339"

		dateParser, err := NewYAMLParserFromConfig(dateConfig)
		require.NoError(t, err)

		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"ts": 1704110400, "val": 42.5}`),
		}

		res, err := dateParser.Parse(msg)
		requireUnusable(t, res, err)
		assert.True(t, errorsContain(res.Errors, "epoch format"))
	})
}

// TestConvertToTimestamp validates the shared conversion helper used by the
// extract_index and extract_timestamp steps.
func TestConvertToTimestamp(t *testing.T) {
	base := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC) // epoch 1704110400

	t.Run("valid conversions", func(t *testing.T) {
		cases := []struct {
			name   string
			value  interface{}
			format string
			want   time.Time
		}{
			{"string rfc3339", "2024-01-01T12:00:00Z", "rfc3339", base},
			{"string rfc3339nano", "2024-01-01T12:00:00.123456789Z", "rfc3339nano", base.Add(123456789 * time.Nanosecond)},
			{"string custom layout", "2024-01-01 12:00:00", "2006-01-02 15:04:05", base},
			{"string unix", "1704110400", "unix", base},
			{"string unix_ms", "1704110400123", "unix_ms", base.Add(123 * time.Millisecond)},
			{"string unix_us", "1704110400123456", "unix_us", base.Add(123456 * time.Microsecond)},
			{"string unix_ns", "1704110400123456789", "unix_ns", base.Add(123456789 * time.Nanosecond)},
			{"string fractional unix", "1704110400.5", "unix", base.Add(500 * time.Millisecond)},
			{"int64 unix", int64(1704110400), "unix", base},
			{"int64 unix_ms", int64(1704110400123), "unix_ms", time.UnixMilli(1704110400123)},
			{"int64 unix_us", int64(1704110400123456), "unix_us", time.UnixMicro(1704110400123456)},
			{"int64 unix_ns", int64(1704110400123456789), "unix_ns", base.Add(123456789 * time.Nanosecond)},
			{"float64 unix fraction", 1704110400.5, "unix", base.Add(500 * time.Millisecond)},
			{"float64 unix_ms fraction", 1704110400123.25, "unix_ms", base.Add(123*time.Millisecond + 250*time.Microsecond)},
			{"float64 unix_us fraction", 1704110400123456.5, "unix_us", base.Add(123456*time.Microsecond + 500*time.Nanosecond)},
			{"float64 unix_ns rounds", 1704110400000000000.0, "unix_ns", base},
			{"time.Time pass-through", base, "rfc3339", base},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				got, err := convertToTimestamp(c.value, c.format)
				require.NoError(t, err)
				assert.True(t, got.Equal(c.want), "got %v, want %v", got, c.want)
			})
		}
	})

	t.Run("errors", func(t *testing.T) {
		cases := []struct {
			name   string
			value  interface{}
			format string
		}{
			{"garbage string", "not-a-timestamp", "rfc3339"},
			{"garbage string epoch", "not-a-number", "unix"},
			{"int64 with rfc3339", int64(1704110400), "rfc3339"},
			{"int64 with custom layout", int64(1704110400), "2006-01-02 15:04:05"},
			{"float64 with rfc3339", 1704110400.0, "rfc3339"},
			{"unsupported type", []byte("2024-01-01"), "rfc3339"},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				_, err := convertToTimestamp(c.value, c.format)
				require.Error(t, err)
			})
		}
	})

	// Property: an instant formatted as RFC3339Nano and expressed as an int64
	// nanosecond epoch both round-trip to the same instant.
	t.Run("property epoch round-trip", func(t *testing.T) {
		rapid.Check(t, func(rt *rapid.T) {
			sec := rapid.Int64Range(0, 4102444800).Draw(rt, "sec") // 1970..2100
			nano := rapid.Int64Range(0, 999999999).Draw(rt, "nano")
			want := time.Unix(sec, nano).UTC()

			viaString, err := convertToTimestamp(want.Format(time.RFC3339Nano), "rfc3339nano")
			require.NoError(rt, err)

			viaNanos, err := convertToTimestamp(want.UnixNano(), "unix_ns")
			require.NoError(rt, err)

			assert.Equal(rt, want.UnixNano(), viaString.UnixNano())
			assert.Equal(rt, want.UnixNano(), viaNanos.UnixNano())
		})
	})
}

// TestYAMLParserExtractTimestamp validates the extract_timestamp value-column step
func TestYAMLParserExtractTimestamp(t *testing.T) {
	makeConfig := func(format string) YAMLConfig {
		return YAMLConfig{
			Output: OutputSchema{
				Columns: []ColumnSchema{
					{Name: "ingested_at", Type: "timestamp"},
					{Name: "value", Type: "double"},
				},
			},
			Transformations: []TransformSpec{
				{Step: "parse_json", Config: map[string]interface{}{}},
				{Step: "extract_timestamp", Config: map[string]interface{}{
					"source": "event_ts",
					"target": "ingested_at",
					"format": format,
				}},
				{Step: "extract_field", Config: map[string]interface{}{
					"source": "val",
					"target": "value",
					"type":   "float64",
				}},
				{Step: "extract_table", Config: map[string]interface{}{"value": "events"}},
			},
		}
	}

	t.Run("step writes time.Time to target field", func(t *testing.T) {
		step, err := makeExtractTimestampStep(map[string]interface{}{
			"source": "event_ts",
			"target": "ingested_at",
			"format": "rfc3339",
		})
		require.NoError(t, err)

		state := &ParseState{Fields: map[string]interface{}{"event_ts": "2024-01-01T12:00:00Z"}}
		require.NoError(t, step(state))

		ts, ok := state.Fields["ingested_at"].(time.Time)
		require.True(t, ok, "target field must hold a time.Time")
		assert.True(t, ts.Equal(time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)))
	})

	t.Run("rfc3339 string source", func(t *testing.T) {
		parser, err := NewYAMLParserFromConfig(makeConfig("rfc3339"))
		require.NoError(t, err)

		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"event_ts": "2024-01-01T12:00:00Z", "val": 42.5}`),
		}

		res, err := parser.Parse(msg)
		require.NoError(t, err)
		assert.Equal(t, OutcomeOK, res.Outcome)
		require.Len(t, res.Tables, 1)
		assert.Equal(t, 1, res.Tables[0].RowCount())
	})

	// parse_json decodes all JSON numbers as float64: this is the hot path
	// for numeric timestamp columns.
	t.Run("numeric source via parse_json", func(t *testing.T) {
		for format, payload := range map[string]string{
			"unix":    `{"event_ts": 1704110400.5, "val": 42.5}`,
			"unix_ms": `{"event_ts": 1704110400123, "val": 42.5}`,
		} {
			parser, err := NewYAMLParserFromConfig(makeConfig(format))
			require.NoError(t, err)

			res, err := parser.Parse(&nats.Msg{Subject: util.RandomTopicName(), Data: []byte(payload)})
			require.NoError(t, err)
			assert.Equal(t, OutcomeOK, res.Outcome, "format %s", format)
			require.Len(t, res.Tables, 1)
		}
	})

	// A failed extract_timestamp is a field-level failure: the row is still
	// written (Partial) with the MinTimespec null sentinel, unlike a failed
	// extract_index which makes the whole message unusable.
	t.Run("missing source is partial", func(t *testing.T) {
		parser, err := NewYAMLParserFromConfig(makeConfig("rfc3339"))
		require.NoError(t, err)

		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"val": 42.5}`),
		}

		res, err := parser.Parse(msg)
		require.NoError(t, err)
		assert.Equal(t, OutcomePartial, res.Outcome)
		require.Len(t, res.Tables, 1)
		assert.True(t, errorsContain(res.Errors, "extract_timestamp"))
	})

	t.Run("numeric with date format is partial", func(t *testing.T) {
		parser, err := NewYAMLParserFromConfig(makeConfig("rfc3339"))
		require.NoError(t, err)

		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"event_ts": 1704110400, "val": 42.5}`),
		}

		res, err := parser.Parse(msg)
		require.NoError(t, err)
		assert.Equal(t, OutcomePartial, res.Outcome)
		require.Len(t, res.Tables, 1)
		assert.True(t, errorsContain(res.Errors, "epoch format"))
	})

	t.Run("config validation", func(t *testing.T) {
		_, err := makeExtractTimestampStep(map[string]interface{}{"target": "x"})
		require.Error(t, err, "missing source must be rejected")

		_, err = makeExtractTimestampStep(map[string]interface{}{"source": "x"})
		require.Error(t, err, "missing target must be rejected")

		_, err = makeExtractTimestampStep(map[string]interface{}{"source": "x", "target": "$timestamp"})
		require.Error(t, err, "reserved $-prefixed target must be rejected")
	})
}

// TestYAMLParserSymbolColumn validates symbol column configuration and writing
func TestYAMLParserSymbolColumn(t *testing.T) {
	t.Run("symbol type maps to TsColumnSymbol", func(t *testing.T) {
		assert.True(t, isValidColumnType("symbol"))
		assert.Equal(t, qdb.TsColumnSymbol, stringToColumnType("symbol"))
	})

	// The batch push data_type is the CLIENT-side type: symbols travel as
	// strings, everything else maps to itself.
	t.Run("writer data type maps symbol to string", func(t *testing.T) {
		assert.Equal(t, qdb.TsColumnString, writerDataType(qdb.TsColumnSymbol))
		assert.Equal(t, qdb.TsColumnDouble, writerDataType(qdb.TsColumnDouble))

		writerCols, colTypes, err := createColumnMapping([]ColumnSchema{{Name: "s", Type: "symbol"}})
		require.NoError(t, err)
		assert.Equal(t, qdb.TsColumnString, writerCols[0].ColumnType)
		assert.Equal(t, qdb.TsColumnSymbol, colTypes[0])
	})

	config := YAMLConfig{
		Output: OutputSchema{
			Columns: []ColumnSchema{
				{Name: "stock_id", Type: "symbol"},
				{Name: "price", Type: "double"},
			},
		},
		Transformations: []TransformSpec{
			{Step: "parse_json", Config: map[string]interface{}{}},
			{Step: "extract_index", Config: map[string]interface{}{
				"source": "ts",
				"format": "unix",
			}},
			{Step: "extract_table", Config: map[string]interface{}{"value": "trades"}},
		},
	}

	parser, err := NewYAMLParserFromConfig(config)
	require.NoError(t, err)

	t.Run("symbol column written from string field", func(t *testing.T) {
		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"stock_id": "AAPL", "price": 123.45, "ts": 1704110400}`),
		}

		res, err := parser.Parse(msg)
		require.NoError(t, err)
		assert.Equal(t, OutcomeOK, res.Outcome)
		require.Len(t, res.Tables, 1)
		assert.Equal(t, 1, res.Tables[0].RowCount())
	})

	t.Run("missing symbol field falls back to empty sentinel", func(t *testing.T) {
		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"price": 123.45, "ts": 1704110400}`),
		}

		res, err := parser.Parse(msg)
		require.NoError(t, err)
		require.Len(t, res.Tables, 1)
		assert.Equal(t, 1, res.Tables[0].RowCount())
	})
}

// TestYAMLParserComputeField validates computed field operations
func TestYAMLParserComputeField(t *testing.T) {
	config := YAMLConfig{
		Output: OutputSchema{
			Columns: []ColumnSchema{
				{Name: "tag_id", Type: "string"},
				{Name: "value", Type: "double"},
			},
		},
		Transformations: []TransformSpec{
			{Step: "parse_json", Config: map[string]interface{}{}},
			{Step: "extract_table", Config: map[string]interface{}{
				"value": "computed",
			}},
			{Step: "compute_field", Config: map[string]interface{}{
				"operation": "concat",
				"target":    "tag_id",
				"fields":    []interface{}{"facility", "\":\"", "tag"},
			}},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "val",
				"target": "value",
				"type":   "float64",
			}},
		},
	}

	parser, err := NewYAMLParserFromConfig(config)
	require.NoError(t, err)

	// Test string concatenation with literal and field values
	t.Run("string concatenation", func(t *testing.T) {
		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"facility": "plant1", "tag": "sensor01", "val": 42.5}`),
		}

		res, err := parser.Parse(msg)
		tables := res.Tables
		require.NoError(t, err)
		require.Len(t, tables, 1)

		table := tables[0]
		assert.Equal(t, "computed", stripNullTerminator(table.GetName()))
		assert.Equal(t, 1, table.RowCount())
	})
}

// TestYAMLParserNumberParsing validates graceful number conversion
func TestYAMLParserNumberParsing(t *testing.T) {
	config := YAMLConfig{
		Output: OutputSchema{
			Columns: []ColumnSchema{
				{Name: "safe_value", Type: "double"},
				{Name: "original", Type: "string"},
			},
		},
		Transformations: []TransformSpec{
			{Step: "parse_json", Config: map[string]interface{}{}},
			{Step: "extract_table", Config: map[string]interface{}{
				"value": "numbers",
			}},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "raw",
				"target": "original",
				"type":   "string",
			}},
			{Step: "safe_parse_number", Config: map[string]interface{}{
				"source":   "raw",
				"target":   "safe_value",
				"on_error": "null",
			}},
		},
	}

	parser, err := NewYAMLParserFromConfig(config)
	require.NoError(t, err)

	// Test parsing valid numeric string
	t.Run("valid number string", func(t *testing.T) {
		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"raw": "42.5"}`),
		}

		res, err := parser.Parse(msg)
		tables := res.Tables
		require.NoError(t, err)
		require.Len(t, tables, 1)

		table := tables[0]
		assert.Equal(t, "numbers", stripNullTerminator(table.GetName()))
		assert.Equal(t, 1, table.RowCount())
	})

	// Test handling invalid numeric string - should return null
	t.Run("invalid number string", func(t *testing.T) {
		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"raw": "not_a_number"}`),
		}

		res, err := parser.Parse(msg)
		tables := res.Tables
		require.NoError(t, err)
		require.Len(t, tables, 1)

		table := tables[0]
		assert.Equal(t, "numbers", stripNullTerminator(table.GetName()))
		assert.Equal(t, 1, table.RowCount())
	})
}

// TestYAMLParserErrorHandling validates outcome classification: structural
// failures are unusable (no fabricated rows), field failures are partial
// (sentinel-filled row per ADR-005).
func TestYAMLParserErrorHandling(t *testing.T) {
	config := YAMLConfig{
		Output: OutputSchema{
			Columns: []ColumnSchema{
				{Name: "timestamp", Type: "timestamp"},
				{Name: "value", Type: "double"},
			},
		},
		Transformations: []TransformSpec{
			{Step: "parse_json", Config: map[string]interface{}{}},
			{Step: "extract_index", Config: map[string]interface{}{
				"source": "ts",
				"format": "rfc3339",
			}},
			{Step: "extract_table", Config: map[string]interface{}{
				"value": "test",
			}},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "val",
				"target": "value",
				"type":   "float64",
			}},
		},
	}

	parser, err := NewYAMLParserFromConfig(config)
	require.NoError(t, err)

	t.Run("undecodable payload is unusable without warning cascade", func(t *testing.T) {
		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte("\x0a\x22\x08\x96\x01\x12\x07\x74\x65\x73\x74"), // protobuf junk
		}

		res, err := parser.Parse(msg)
		requireUnusable(t, res, err)
		assert.Len(t, res.Errors, 1, "pipeline must stop at the structural failure")
	})

	t.Run("missing index source with extract_index configured is unusable", func(t *testing.T) {
		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"val": 42.5}`), // valid JSON, no "ts" field
		}

		res, err := parser.Parse(msg)
		requireUnusable(t, res, err)
		assert.True(t, errorsContain(res.Errors, "$timestamp"),
			"floor check must record the missing index: %v", res.Errors)
	})

	t.Run("missing non-index field is partial with sentinel fill", func(t *testing.T) {
		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"ts": "2024-01-15T10:30:00Z"}`), // "val" missing
		}

		res, err := parser.Parse(msg)
		require.NoError(t, err)
		assert.Equal(t, OutcomePartial, res.Outcome)
		require.NotEmpty(t, res.Errors)
		require.Len(t, res.Tables, 1)
		assert.Equal(t, "test", stripNullTerminator(res.Tables[0].GetName()))
		assert.Equal(t, 1, res.Tables[0].RowCount())
	})

	t.Run("fully valid message is ok", func(t *testing.T) {
		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"ts": "2024-01-15T10:30:00Z", "val": 42.5}`),
		}

		res, err := parser.Parse(msg)
		require.NoError(t, err)
		assert.Equal(t, OutcomeOK, res.Outcome)
		assert.Empty(t, res.Errors)
		require.Len(t, res.Tables, 1)
	})

	t.Run("no extract_index step keeps time.Now fallback", func(t *testing.T) {
		noIndexConfig := YAMLConfig{
			Output: OutputSchema{
				Columns: []ColumnSchema{{Name: "value", Type: "double"}},
			},
			Transformations: []TransformSpec{
				{Step: "parse_json", Config: map[string]interface{}{}},
				{Step: "extract_table", Config: map[string]interface{}{
					"value": "test",
				}},
				{Step: "extract_field", Config: map[string]interface{}{
					"source": "val",
					"target": "value",
					"type":   "float64",
				}},
			},
		}

		noIndexParser, err := NewYAMLParserFromConfig(noIndexConfig)
		require.NoError(t, err)

		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"val": 42.5}`),
		}

		res, err := noIndexParser.Parse(msg)
		require.NoError(t, err)
		assert.Equal(t, OutcomeOK, res.Outcome)
		require.Len(t, res.Tables, 1)
		assert.Equal(t, 1, res.Tables[0].RowCount())
	})

	t.Run("property: non-JSON bytes never produce a row", func(t *testing.T) {
		nonJSON := rapid.SliceOfN(rapid.Byte(), 1, 64).
			Filter(func(b []byte) bool { return !json.Valid(b) })

		rapid.Check(t, func(rt *rapid.T) {
			msg := &nats.Msg{
				Subject: util.RandomTopicName(),
				Data:    nonJSON.Draw(rt, "payload"),
			}

			res, err := parser.Parse(msg)
			require.NoError(rt, err)
			assert.Equal(rt, OutcomeUnusable, res.Outcome)
			assert.Empty(rt, res.Tables)
		})
	})
}

// TestYAMLParserInterfaceCompliance ensures Parser interface implementation
func TestYAMLParserInterfaceCompliance(t *testing.T) {
	config := YAMLConfig{
		Output: OutputSchema{
			Columns: []ColumnSchema{{Name: "value", Type: "double"}},
		},
		Transformations: []TransformSpec{
			{Step: "parse_json", Config: map[string]interface{}{}},
			{Step: "extract_table", Config: map[string]interface{}{
				"value": "test",
			}},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "val",
				"target": "value",
				"type":   "float64",
			}},
		},
	}

	parser, err := NewYAMLParserFromConfig(config)
	require.NoError(t, err)
	require.NotNil(t, parser)

	// Compile-time check that YAMLParser implements Parser interface
	var _ Parser = parser

	// Verify basic parsing functionality works correctly
	msg := &nats.Msg{
		Subject: util.RandomTopicName(),
		Data:    []byte(`{"val": 42.5}`),
	}

	res, err := parser.Parse(msg)
	tables := res.Tables
	require.NoError(t, err)
	require.Len(t, tables, 1)

	table := tables[0]
	assert.Equal(t, "test", stripNullTerminator(table.GetName()))
	assert.Equal(t, 1, table.RowCount())
}

// TestYAMLParserTransformationSteps validates individual transformation steps
func TestYAMLParserTransformationSteps(t *testing.T) {
	// Test gzip decompression transformation step
	t.Run("decompress step", func(t *testing.T) {
		config := YAMLConfig{
			Output: OutputSchema{
				Columns: []ColumnSchema{{Name: "value", Type: "string"}},
			},
			Transformations: []TransformSpec{
				{Step: "decompress", Config: map[string]interface{}{
					"algorithm": "gzip",
				}},
				{Step: "parse_json", Config: map[string]interface{}{}},
				{Step: "extract_table", Config: map[string]interface{}{
					"value": "test",
				}},
				{Step: "extract_field", Config: map[string]interface{}{
					"source": "msg",
					"target": "value",
					"type":   "string",
				}},
			},
		}

		parser, err := NewYAMLParserFromConfig(config)
		require.NoError(t, err)

		// Create gzip-compressed JSON data for testing decompression
		jsonData := `{"msg": "hello world"}`
		compressedData := compressGzip(t, []byte(jsonData))

		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    compressedData,
		}

		res, err := parser.Parse(msg)
		tables := res.Tables
		require.NoError(t, err)
		require.Len(t, tables, 1)

		table := tables[0]
		assert.Equal(t, "test", stripNullTerminator(table.GetName()))
		assert.Equal(t, 1, table.RowCount())
	})
}

// Helper functions

// compressGzip compresses data using gzip algorithm

func compressGzip(t *testing.T, data []byte) []byte {
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	_, err := writer.Write(data)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	return buf.Bytes()
}

// TestYAMLParserColumnMismatch tests column configuration mismatches that could cause segfaults
func TestYAMLParserColumnMismatch(t *testing.T) {
	t.Run("matching columns - normal case", func(t *testing.T) {
		// Test that parser correctly handles when column counts and names match
		config := YAMLConfig{
			Output: OutputSchema{
				Columns: []ColumnSchema{
					{Name: "temp", Type: "double"},
					{Name: "humidity", Type: "double"},
					{Name: "location", Type: "string"},
				},
			},
			Transformations: []TransformSpec{
				{Step: "parse_json", Config: map[string]interface{}{}},
				{Step: "extract_table", Config: map[string]interface{}{
					"value": "sensors",
				}},
			},
		}

		parser, err := NewYAMLParserFromConfig(config)
		require.NoError(t, err)

		// Verify internal state matches configuration
		assert.Equal(t, 3, len(parser.columns))
		assert.Equal(t, 3, len(parser.columnTypes))
		assert.Equal(t, "temp\x00", parser.columns[0].ColumnName)
		assert.Equal(t, "humidity\x00", parser.columns[1].ColumnName)
		assert.Equal(t, "location\x00", parser.columns[2].ColumnName)

		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"temp": 25.5, "humidity": 60.0, "location": "office"}`),
		}

		res, err := parser.Parse(msg)
		tables := res.Tables
		require.NoError(t, err)
		require.Len(t, tables, 1)
		assert.Equal(t, "sensors", stripNullTerminator(tables[0].GetName()))
	})

	t.Run("buffer bounds validation - all column types", func(t *testing.T) {
		// Test that buffer allocation matches column count exactly
		config := YAMLConfig{
			Output: OutputSchema{
				Columns: []ColumnSchema{
					{Name: "double_col", Type: "double"},
					{Name: "int64_col", Type: "int64"},
					{Name: "string_col", Type: "string"},
					{Name: "blob_col", Type: "blob"},
					{Name: "timestamp_col", Type: "timestamp"},
					{Name: "symbol_col", Type: "symbol"},
				},
			},
			Transformations: []TransformSpec{
				{Step: "parse_json", Config: map[string]interface{}{}},
				{Step: "extract_table", Config: map[string]interface{}{
					"value": "all_types",
				}},
				{Step: "extract_index", Config: map[string]interface{}{
					"source": "ts",
					"format": "unix",
				}},
			},
		}

		parser, err := NewYAMLParserFromConfig(config)
		require.NoError(t, err)

		// Verify that column count matches buffer allocation
		assert.Equal(t, 6, len(parser.columns))
		assert.Equal(t, 6, len(parser.columnTypes))

		// Test with data for all column types (symbols share stringVals)
		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"double_col": 42.5, "int64_col": 123, "string_col": "test", "blob_col": "YmluYXJ5", "symbol_col": "sym_a", "ts": 1704110400}`),
		}

		// Parse and verify no buffer overflow occurs
		res, err := parser.Parse(msg)
		tables := res.Tables
		require.NoError(t, err)
		require.Len(t, tables, 1)

		// Verify parse state buffer allocation by inspecting new state
		state := parser.newParseState()
		assert.Equal(t, 6, len(state.doubleVals))
		assert.Equal(t, 6, len(state.int64Vals))
		assert.Equal(t, 6, len(state.stringVals))
		assert.Equal(t, 6, len(state.blobVals))
		assert.Equal(t, 6, len(state.timestampVals))
	})

	t.Run("column name consistency validation", func(t *testing.T) {
		// Test that column names in p.columns match configuration exactly
		config := YAMLConfig{
			Output: OutputSchema{
				Columns: []ColumnSchema{
					{Name: "sensor_id", Type: "string"},
					{Name: "measurement_value", Type: "double"},
					{Name: "recorded_at", Type: "timestamp"},
				},
			},
			Transformations: []TransformSpec{
				{Step: "parse_json", Config: map[string]interface{}{}},
				{Step: "extract_table", Config: map[string]interface{}{
					"value": "consistency_test",
				}},
			},
		}

		parser, err := NewYAMLParserFromConfig(config)
		require.NoError(t, err)

		// Verify that internal column mapping preserves exact names from config (with null terminator)
		require.Equal(t, 3, len(parser.columns))
		for i, expectedCol := range config.Output.Columns {
			assert.Equal(t, expectedCol.Name+"\x00", parser.columns[i].ColumnName)
		}

		// Test parsing with matching field names
		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"sensor_id": "temp_001", "measurement_value": 23.7, "recorded_at": "2024-01-01T12:00:00Z"}`),
		}

		res, err := parser.Parse(msg)
		tables := res.Tables
		require.NoError(t, err)
		require.Len(t, tables, 1)
	})

	t.Run("empty columns configuration", func(t *testing.T) {
		// Test that parser properly rejects empty column configurations
		// This prevents potential divide-by-zero or null pointer issues
		config := YAMLConfig{
			Output: OutputSchema{
				Columns: []ColumnSchema{}, // Empty columns should be rejected
			},
			Transformations: []TransformSpec{
				{Step: "parse_json", Config: map[string]interface{}{}},
			},
		}

		parser, err := NewYAMLParserFromConfig(config)
		assert.Nil(t, parser)
		require.Error(t, err)

		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, "yaml_parser", connErr.Component)
		assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
		assert.Contains(t, err.Error(), "at least one column is required")
	})

	t.Run("buffer overflow prevention - bounds checking", func(t *testing.T) {
		// Test that buffer bounds are properly validated during parsing
		// This simulates the scenario that caused the original segfault
		config := YAMLConfig{
			Output: OutputSchema{
				Columns: []ColumnSchema{
					{Name: "col1", Type: "double"},
					{Name: "col2", Type: "string"},
				},
			},
			Transformations: []TransformSpec{
				{Step: "parse_json", Config: map[string]interface{}{}},
				{Step: "extract_table", Config: map[string]interface{}{
					"value": "bounds_test",
				}},
			},
		}

		parser, err := NewYAMLParserFromConfig(config)
		require.NoError(t, err)

		// Test with actual parsing instead of directly calling createWriterTable
		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"col1": 42.5, "col2": "test"}`),
		}

		res, err := parser.Parse(msg)
		tables := res.Tables
		require.NoError(t, err)
		require.Len(t, tables, 1)
		assert.Equal(t, "bounds_test", stripNullTerminator(tables[0].GetName()))

		// Test internal buffer sizes match column count by creating a new state
		state := parser.newParseState()
		assert.Equal(t, len(parser.columns), len(state.doubleVals))
		assert.Equal(t, len(parser.columns), len(state.stringVals))
		assert.Equal(t, len(parser.columns), len(state.int64Vals))
		assert.Equal(t, len(parser.columns), len(state.blobVals))
		assert.Equal(t, len(parser.columns), len(state.timestampVals))
	})

	t.Run("column type consistency - mapping validation", func(t *testing.T) {
		// Test that column types are consistently mapped from config to internal structures
		config := YAMLConfig{
			Output: OutputSchema{
				Columns: []ColumnSchema{
					{Name: "double_field", Type: "double"},
					{Name: "int64_field", Type: "int64"},
					{Name: "string_field", Type: "string"},
					{Name: "blob_field", Type: "blob"},
					{Name: "timestamp_field", Type: "timestamp"},
				},
			},
			Transformations: []TransformSpec{
				{Step: "parse_json", Config: map[string]interface{}{}},
				{Step: "extract_table", Config: map[string]interface{}{
					"value": "type_test",
				}},
			},
		}

		parser, err := NewYAMLParserFromConfig(config)
		require.NoError(t, err)

		// Verify that column types match expected QDB types
		require.Equal(t, 5, len(parser.columnTypes))
		require.Equal(t, 5, len(parser.columns))

		expectedTypes := []string{"double", "int64", "string", "blob", "timestamp"}
		for i, expectedType := range expectedTypes {
			configType := config.Output.Columns[i].Type
			assert.Equal(t, expectedType, configType)

			// Verify internal mapping is consistent
			expectedQdbType := stringToColumnType(configType)
			assert.Equal(t, expectedQdbType, parser.columnTypes[i])
			assert.Equal(t, expectedQdbType, parser.columns[i].ColumnType)
		}
	})

	t.Run("memory safety - concurrent buffer allocation", func(t *testing.T) {
		// Test that each Parse call gets its own buffer allocation
		// This prevents the memory corruption issues
		config := YAMLConfig{
			Output: OutputSchema{
				Columns: []ColumnSchema{
					{Name: "value", Type: "double"},
				},
			},
			Transformations: []TransformSpec{
				{Step: "parse_json", Config: map[string]interface{}{}},
				{Step: "extract_table", Config: map[string]interface{}{
					"value": "memory_test",
				}},
			},
		}

		parser, err := NewYAMLParserFromConfig(config)
		require.NoError(t, err)

		// Create multiple parse states to verify independent allocation
		state1 := parser.newParseState()
		state2 := parser.newParseState()

		// Verify they have separate buffer instances
		assert.NotSame(t, &state1.doubleVals, &state2.doubleVals)
		assert.NotSame(t, &state1.stringVals, &state2.stringVals)
		assert.NotSame(t, &state1.Fields, &state2.Fields)

		// Verify buffer sizes match column count
		assert.Equal(t, len(parser.columns), len(state1.doubleVals))
		assert.Equal(t, len(parser.columns), len(state2.doubleVals))
	})
}

// TestYAMLParserColumnSynchronizationValidation tests the new column synchronization validation
// added to prevent segfault issues during parser initialization
//
//nolint:dupl // sub-tests intentionally mirror each other to validate distinct mismatch scenarios
func TestYAMLParserColumnSynchronizationValidation(t *testing.T) {
	t.Run("valid synchronization passes", func(t *testing.T) {
		// Test that properly synchronized columns pass validation
		config := YAMLConfig{
			Output: OutputSchema{
				Columns: []ColumnSchema{
					{Name: "temperature", Type: "double"},
					{Name: "pressure", Type: "int64"},
					{Name: "location", Type: "string"},
				},
			},
			Transformations: []TransformSpec{
				{Step: "parse_json", Config: map[string]interface{}{}},
				{Step: "extract_table", Config: map[string]interface{}{
					"value": "sensors",
				}},
			},
		}

		parser, err := NewYAMLParserFromConfig(config)
		require.NoError(t, err)
		require.NotNil(t, parser)

		// Verify synchronization is maintained
		assert.Equal(t, len(config.Output.Columns), len(parser.columns))
		assert.Equal(t, len(config.Output.Columns), len(parser.columnTypes))

		for i, configCol := range config.Output.Columns {
			assert.Equal(t, configCol.Name+"\x00", parser.columns[i].ColumnName)
			assert.Equal(t, stringToColumnType(configCol.Type), parser.columnTypes[i])
			assert.Equal(t, stringToColumnType(configCol.Type), parser.columns[i].ColumnType)
		}
	})

	t.Run("column synchronization validation function", func(t *testing.T) {
		// Test the validateColumnSynchronization function directly
		configColumns := []ColumnSchema{
			{Name: "temp", Type: "double"},
			{Name: "humid", Type: "int64"},
		}

		writerColumns := []qdb.WriterColumn{
			{ColumnName: "temp\x00", ColumnType: qdb.TsColumnDouble},
			{ColumnName: "humid\x00", ColumnType: qdb.TsColumnInt64},
		}

		columnTypes := []qdb.TsColumnType{
			qdb.TsColumnDouble,
			qdb.TsColumnInt64,
		}

		// This should pass - everything is synchronized
		err := validateColumnSynchronization(configColumns, writerColumns, columnTypes)
		assert.NoError(t, err)
	})

	t.Run("column count mismatch detected", func(t *testing.T) {
		configColumns := []ColumnSchema{
			{Name: "temp", Type: "double"},
			{Name: "humid", Type: "int64"},
			{Name: "pressure", Type: "double"}, // Extra column in config
		}

		writerColumns := []qdb.WriterColumn{
			{ColumnName: "temp\x00", ColumnType: qdb.TsColumnDouble},
			{ColumnName: "humid\x00", ColumnType: qdb.TsColumnInt64},
		}

		columnTypes := []qdb.TsColumnType{
			qdb.TsColumnDouble,
			qdb.TsColumnInt64,
		}

		err := validateColumnSynchronization(configColumns, writerColumns, columnTypes)
		require.Error(t, err)

		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, "yaml_parser", connErr.Component)
		assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
		assert.Contains(t, err.Error(), "column count mismatch: schema has 3 columns but internal mapping has 2 columns")
	})

	t.Run("column name mismatch detected", func(t *testing.T) {
		configColumns := []ColumnSchema{
			{Name: "temperature", Type: "double"}, // Different name
			{Name: "humidity", Type: "int64"},
		}

		writerColumns := []qdb.WriterColumn{
			{ColumnName: "temp\x00", ColumnType: qdb.TsColumnDouble}, // Different name (missing null terminator test)
			{ColumnName: "humidity\x00", ColumnType: qdb.TsColumnInt64},
		}

		columnTypes := []qdb.TsColumnType{
			qdb.TsColumnDouble,
			qdb.TsColumnInt64,
		}

		err := validateColumnSynchronization(configColumns, writerColumns, columnTypes)
		require.Error(t, err)

		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, "yaml_parser", connErr.Component)
		assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
		assert.Contains(t, err.Error(), "column name mismatch at index 0: config has 'temperature' but internal mapping has 'temp\x00'")
	})

	t.Run("column type mismatch detected", func(t *testing.T) {
		configColumns := []ColumnSchema{
			{Name: "temp", Type: "double"},
			{Name: "humid", Type: "string"}, // Different type
		}

		writerColumns := []qdb.WriterColumn{
			{ColumnName: "temp\x00", ColumnType: qdb.TsColumnDouble},
			{ColumnName: "humid\x00", ColumnType: qdb.TsColumnInt64}, // Different type
		}

		columnTypes := []qdb.TsColumnType{
			qdb.TsColumnDouble,
			qdb.TsColumnInt64, // Different type
		}

		err := validateColumnSynchronization(configColumns, writerColumns, columnTypes)
		require.Error(t, err)

		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, "yaml_parser", connErr.Component)
		assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
		assert.Contains(t, err.Error(), "column type mismatch at index 1 for column 'humid': config has type 'string' but internal has type")
	})

	t.Run("internal consistency mismatch detected", func(t *testing.T) {
		configColumns := []ColumnSchema{
			{Name: "temp", Type: "double"},
			{Name: "humid", Type: "int64"},
		}

		writerColumns := []qdb.WriterColumn{
			{ColumnName: "temp\x00", ColumnType: qdb.TsColumnDouble},
		} // Missing second column

		columnTypes := []qdb.TsColumnType{
			qdb.TsColumnDouble,
			qdb.TsColumnInt64,
		}

		err := validateColumnSynchronization(configColumns, writerColumns, columnTypes)
		require.Error(t, err)

		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, "yaml_parser", connErr.Component)
		assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
		assert.Contains(t, err.Error(), "column count mismatch: schema has 2 columns but internal mapping has 1 columns")
	})
}

// TestYAMLParser_ExtractTableStatic tests static table extraction
func TestYAMLParser_ExtractTableStatic(t *testing.T) {
	config := YAMLConfig{
		Output: OutputSchema{
			Columns: []ColumnSchema{
				{Name: "value", Type: "double"},
			},
		},
		Transformations: []TransformSpec{
			{Step: "parse_json"},
			{Step: "extract_table", Config: map[string]interface{}{
				"value": "static_table_name",
			}},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "data",
				"target": "value",
				"type":   "float64",
			}},
		},
	}

	parser, err := NewYAMLParserFromConfig(config)
	require.NoError(t, err)

	// Test message
	testData := `{"data": 42.5}`
	msg := &nats.Msg{
		Subject: "test.subject",
		Data:    []byte(testData),
	}

	res, err := parser.Parse(msg)
	tables := res.Tables
	require.NoError(t, err)
	require.Len(t, tables, 1)

	// Verify table name
	assert.Equal(t, "static_table_name", stripNullTerminator(tables[0].TableName))
}

// TestYAMLParser_ExtractTableDynamic tests dynamic table from field
func TestYAMLParser_ExtractTableDynamic(t *testing.T) {
	config := YAMLConfig{
		Output: OutputSchema{
			Columns: []ColumnSchema{
				{Name: "value", Type: "double"},
			},
		},
		Transformations: []TransformSpec{
			{Step: "parse_json"},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "table_name",
				"target": "$table",
				"type":   "string",
			}},
			{Step: "extract_table"}, // Uses default source: "$table"
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "data",
				"target": "value",
				"type":   "float64",
			}},
		},
	}

	parser, err := NewYAMLParserFromConfig(config)
	require.NoError(t, err)

	// Test message with dynamic table name
	testData := `{"table_name": "dynamic_sensors", "data": 25.7}`
	msg := &nats.Msg{
		Subject: "test.subject",
		Data:    []byte(testData),
	}

	res, err := parser.Parse(msg)
	tables := res.Tables
	require.NoError(t, err)
	require.Len(t, tables, 1)

	// Verify dynamic table name
	assert.Equal(t, "dynamic_sensors", stripNullTerminator(tables[0].TableName))
}

// TestYAMLParser_ExtractTableFromComputedField tests complex dynamic routing
func TestYAMLParser_ExtractTableFromComputedField(t *testing.T) {
	config := YAMLConfig{
		Output: OutputSchema{
			Columns: []ColumnSchema{
				{Name: "price", Type: "double"},
				{Name: "symbol", Type: "string"},
				{Name: "exchange", Type: "string"},
			},
		},
		Transformations: []TransformSpec{
			{Step: "parse_json"},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "market.exchange",
				"target": "exchange",
				"type":   "string",
			}},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "market.symbol",
				"target": "symbol",
				"type":   "string",
			}},
			{Step: "compute_field", Config: map[string]interface{}{
				"operation": "concat",
				"target":    "table_name",
				"fields":    []interface{}{"\"finance\"", "\".\"", "exchange", "\".\"", "symbol"},
			}},
			{Step: "extract_table", Config: map[string]interface{}{
				"source": "table_name", // Uses computed table name
			}},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "price",
				"target": "price",
				"type":   "float64",
			}},
		},
	}

	parser, err := NewYAMLParserFromConfig(config)
	require.NoError(t, err)

	// Test message with exchange and symbol for computed table name
	testData := `{"market": {"exchange": "NASDAQ", "symbol": "AAPL"}, "price": 150.25}`
	msg := &nats.Msg{
		Subject: "test.subject",
		Data:    []byte(testData),
	}

	res, err := parser.Parse(msg)
	tables := res.Tables
	require.NoError(t, err)
	require.Len(t, tables, 1)

	// Verify computed table name
	assert.Equal(t, "finance.NASDAQ.AAPL", stripNullTerminator(tables[0].TableName))
}

// TestYAMLParser_MissingExtractTableStep tests error case for missing extract_table step
func TestYAMLParser_MissingExtractTableStep(t *testing.T) {
	config := YAMLConfig{
		Output: OutputSchema{
			Columns: []ColumnSchema{
				{Name: "value", Type: "double"},
			},
		},
		Transformations: []TransformSpec{
			{Step: "parse_json"},
			// Missing extract_table step
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "data",
				"target": "value",
				"type":   "float64",
			}},
		},
	}

	parser, err := NewYAMLParserFromConfig(config)
	assert.Nil(t, parser)
	require.Error(t, err)

	var connErr *connectorErrors.ConnectorError
	require.True(t, errors.As(err, &connErr))
	assert.Equal(t, "yaml_parser", connErr.Component)
	assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
	assert.Contains(t, err.Error(), "transformation pipeline must include an extract_table step")
}

// TestYAMLParser_EmptyTableName tests error case for empty table name
//
//nolint:dupl // mirrors TestYAMLParser_MissingSourceField but exercises a different failure mode
func TestYAMLParser_EmptyTableName(t *testing.T) {
	config := YAMLConfig{
		Output: OutputSchema{
			Columns: []ColumnSchema{
				{Name: "value", Type: "double"},
			},
		},
		Transformations: []TransformSpec{
			{Step: "parse_json"},
			{Step: "extract_table", Config: map[string]interface{}{
				"value": "", // Empty table name
			}},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "data",
				"target": "value",
				"type":   "float64",
			}},
		},
	}

	parser, err := NewYAMLParserFromConfig(config)
	require.NoError(t, err)

	// Test message
	testData := `{"data": 42.5}`
	msg := &nats.Msg{
		Subject: "test.subject",
		Data:    []byte(testData),
	}

	res, err := parser.Parse(msg)
	requireUnusable(t, res, err)
	assert.True(t, errorsContain(res.Errors, "empty"),
		"empty table name must be recorded: %v", res.Errors)
}

// TestYAMLParser_MissingSourceField tests error case for missing source field
//
//nolint:dupl // mirrors TestYAMLParser_EmptyTableName but exercises a different failure mode
func TestYAMLParser_MissingSourceField(t *testing.T) {
	config := YAMLConfig{
		Output: OutputSchema{
			Columns: []ColumnSchema{
				{Name: "value", Type: "double"},
			},
		},
		Transformations: []TransformSpec{
			{Step: "parse_json"},
			{Step: "extract_table", Config: map[string]interface{}{
				"source": "non_existent_field", // Field doesn't exist
			}},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "data",
				"target": "value",
				"type":   "float64",
			}},
		},
	}

	parser, err := NewYAMLParserFromConfig(config)
	require.NoError(t, err)

	// Test message without the required field
	testData := `{"data": 42.5}`
	msg := &nats.Msg{
		Subject: "test.subject",
		Data:    []byte(testData),
	}

	res, err := parser.Parse(msg)
	requireUnusable(t, res, err)
	assert.True(t, errorsContain(res.Errors, "source field 'non_existent_field' not found in extract_table"),
		"missing table source must be recorded: %v", res.Errors)
}

// TestYAMLParser_TableNameSecurityValidation tests security validation for table names
func TestYAMLParser_TableNameValidation(t *testing.T) {
	// Only two rules: non-empty, first character alphanumeric.
	// Everything else (slashes, dots, spaces, any length) is accepted;
	// QuasarDB is liberal and the connector must not be more restrictive.
	testCases := []struct {
		name       string
		tableName  string
		shouldFail bool
		errorMsg   string
	}{
		// Conventional names
		{"valid_basic", "sensors", false, ""},
		{"valid_with_dots", "finance.nasdaq.aapl", false, ""},
		{"valid_with_underscores", "network_metrics_v2", false, ""},
		{"valid_with_hyphens", "industrial-sensors", false, ""},
		{"valid_mixed", "finance.nasdaq_v2.aapl-data", false, ""},
		{"valid_numeric_start", "5g_network_metrics", false, ""},

		// Sharded names (GP scheme: <prefix>/<shard>)
		{"valid_shard_slash", "skf/b831", false, ""},
		{"valid_shard_prefix", "process_info/4c93", false, ""},

		// Liberal charset: formerly rejected, now accepted
		{"valid_interior_dotdot", "table/../../secret", false, ""},
		{"valid_backslash", "table\\..\\secret", false, ""},
		{"valid_spaces", "table with spaces", false, ""},
		{"valid_special_chars", "table@#$%", false, ""},
		{"valid_brackets", "table[0]", false, ""},
		{"valid_semicolon", "table;DROP TABLE", false, ""},
		{"valid_quotes", "table\"name", false, ""},
		{"valid_long", strings.Repeat("a", 256), false, ""},

		// Rejected: empty
		{"empty_name", "", true, "cannot be empty"},

		// Rejected: first character not alphanumeric
		{"starts_with_dot", ".hidden_table", true, "must start with an alphanumeric"},
		{"starts_with_underscore", "_private_table", true, "must start with an alphanumeric"},
		{"starts_with_hyphen", "-bad_table", true, "must start with an alphanumeric"},
		{"starts_with_slash", "/leading_slash", true, "must start with an alphanumeric"},
		{"starts_with_traversal", "../../../etc/passwd", true, "must start with an alphanumeric"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := YAMLConfig{
				Output: OutputSchema{
					Columns: []ColumnSchema{
						{Name: "value", Type: "double"},
					},
				},
				Transformations: []TransformSpec{
					{Step: "parse_json"},
					{Step: "extract_table", Config: map[string]interface{}{
						"value": tc.tableName,
					}},
					{Step: "extract_field", Config: map[string]interface{}{
						"source": "data",
						"target": "value",
						"type":   "float64",
					}},
				},
			}

			parser, err := NewYAMLParserFromConfig(config)
			require.NoError(t, err)

			// Test message
			testData := `{"data": 42.5}`
			msg := &nats.Msg{
				Subject: "test.subject",
				Data:    []byte(testData),
			}

			res, err := parser.Parse(msg)
			tables := res.Tables

			if tc.shouldFail {
				// Invalid name: no table routing -> structurally unusable
				requireUnusable(t, res, err)
				assert.True(t, errorsContain(res.Errors, tc.errorMsg),
					"expected %q in step errors for table name %q: %v", tc.errorMsg, tc.tableName, res.Errors)
			} else {
				// Should succeed with valid table name
				require.NoError(t, err, "Valid table name should not fail: %s", tc.tableName)
				require.Len(t, tables, 1)
				assert.Equal(t, tc.tableName, stripNullTerminator(tables[0].TableName))
			}
		})
	}
}

// TestYAMLParser_DynamicTableValidation tests validation of dynamic table names
func TestYAMLParser_DynamicTableValidation(t *testing.T) {
	config := YAMLConfig{
		Output: OutputSchema{
			Columns: []ColumnSchema{
				{Name: "value", Type: "double"},
			},
		},
		Transformations: []TransformSpec{
			{Step: "parse_json"},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "table_name",
				"target": "$table",
				"type":   "string",
			}},
			{Step: "extract_table"}, // Uses dynamic table from field
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "data",
				"target": "value",
				"type":   "float64",
			}},
		},
	}

	parser, err := NewYAMLParserFromConfig(config)
	require.NoError(t, err)

	t.Run("slash_table_accepted", func(t *testing.T) {
		// End-to-end: sharded table name (GP scheme) flows through Parse
		msg := &nats.Msg{
			Subject: "test.subject",
			Data:    []byte(`{"table_name": "skf/b831", "data": 42.5}`),
		}

		res, err := parser.Parse(msg)
		require.NoError(t, err)
		require.Len(t, res.Tables, 1)
		assert.Equal(t, "skf/b831", stripNullTerminator(res.Tables[0].TableName))
	})

	t.Run("non_alphanumeric_start_rejected", func(t *testing.T) {
		msg := &nats.Msg{
			Subject: "test.subject",
			Data:    []byte(`{"table_name": "../../../etc/passwd", "data": 42.5}`),
		}

		res, err := parser.Parse(msg)
		requireUnusable(t, res, err)
		assert.True(t, errorsContain(res.Errors, "must start with an alphanumeric"),
			"first-char rule must be recorded: %v", res.Errors)
	})
}

// TestYAMLParserFilterValidation exercises the filters block through
// NewYAMLParserFromConfig using a minimal valid base config.
func TestYAMLParserFilterValidation(t *testing.T) {
	// baseConfig is a valid config that satisfies schema + pipeline validation,
	// so any error from these sub-tests originates in filter.New.
	baseConfig := func(f filter.Spec) YAMLConfig {
		return YAMLConfig{
			Output: OutputSchema{Columns: []ColumnSchema{{Name: "data_type", Type: "int64"}}},
			Transformations: []TransformSpec{
				{Step: "parse_json"},
				{Step: "extract_table", Config: map[string]interface{}{"value": "t1"}},
				{Step: "extract_index", Config: map[string]interface{}{"source": "ts", "format": "2006-01-02 15:04:05"}},
				{Step: "extract_field", Config: map[string]interface{}{"source": "data_type", "target": "data_type", "type": "int64"}},
			},
			Filters: f,
		}
	}

	t.Run("valid whitelist builds non-nil filter", func(t *testing.T) {
		p, err := NewYAMLParserFromConfig(baseConfig(filter.Spec{
			Mode:  "whitelist",
			Match: []filter.MatchEntry{{Column: "data_type", Value: 1}},
		}))
		require.NoError(t, err)
		assert.NotNil(t, p.rowFilter)
	})

	t.Run("absent filters yields nil filter", func(t *testing.T) {
		p, err := NewYAMLParserFromConfig(baseConfig(filter.Spec{}))
		require.NoError(t, err)
		assert.Nil(t, p.rowFilter)
	})

	t.Run("invalid mode returns ErrCodeInvalidConfig", func(t *testing.T) {
		_, err := NewYAMLParserFromConfig(baseConfig(filter.Spec{
			Mode:  "bogus",
			Match: []filter.MatchEntry{{Column: "data_type", Value: 1}},
		}))
		require.Error(t, err)
		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
	})

	t.Run("unknown column returns ErrCodeInvalidConfig", func(t *testing.T) {
		_, err := NewYAMLParserFromConfig(baseConfig(filter.Spec{
			Mode:  "whitelist",
			Match: []filter.MatchEntry{{Column: "nope", Value: 1}},
		}))
		require.Error(t, err)
		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
	})

	t.Run("value type mismatch returns ErrCodeInvalidConfig", func(t *testing.T) {
		_, err := NewYAMLParserFromConfig(baseConfig(filter.Spec{
			Mode:  "whitelist",
			Match: []filter.MatchEntry{{Column: "data_type", Value: "x"}},
		}))
		require.Error(t, err)
		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
	})
}

// runComputeFieldStep compiles a compute_field step and runs it.
func runComputeFieldStep(t *testing.T, config, fields map[string]interface{}) (*ParseState, error) {
	t.Helper()

	step, err := makeComputeFieldStep(config)
	require.NoError(t, err)

	state := &ParseState{Fields: fields}

	return state, step(state)
}

// splitBaseConfig returns a valid compute_field split config.
func splitBaseConfig(index int) map[string]interface{} {
	return map[string]interface{}{
		"operation": "split",
		"target":    "revision_raw",
		"source":    "stream_id",
		"separator": ":",
		"index":     index,
	}
}

// TestComputeFieldFactoryErrors validates fail-fast config handling across
// all compute_field operations
func TestComputeFieldFactoryErrors(t *testing.T) {
	cases := []struct {
		name   string
		config map[string]interface{}
	}{
		{"missing source", map[string]interface{}{
			"operation": "split", "target": "t", "separator": ":", "index": 0,
		}},
		{"missing separator", map[string]interface{}{
			"operation": "split", "target": "t", "source": "s", "index": 0,
		}},
		{"empty separator", map[string]interface{}{
			"operation": "split", "target": "t", "source": "s", "separator": "", "index": 0,
		}},
		{"missing index", map[string]interface{}{
			"operation": "split", "target": "t", "source": "s", "separator": ":",
		}},
		{"index not an integer", map[string]interface{}{
			"operation": "split", "target": "t", "source": "s", "separator": ":", "index": "x",
		}},
		{"unsupported operation", map[string]interface{}{
			"operation": "reverse", "target": "t",
		}},
		{"invalid source path", map[string]interface{}{
			"operation": "split", "target": "t", "source": "a..b", "separator": ":", "index": 0,
		}},
		{"reserved $-prefixed target", map[string]interface{}{
			"operation": "concat", "target": "$table", "fields": []interface{}{"\"x\""},
		}},
		{"concat missing fields", map[string]interface{}{
			"operation": "concat", "target": "t",
		}},
		{"concat non-array fields", map[string]interface{}{
			"operation": "concat", "target": "t", "fields": "a",
		}},
		{"concat empty fields", map[string]interface{}{
			"operation": "concat", "target": "t", "fields": []interface{}{},
		}},
		{"concat non-string fields entry", map[string]interface{}{
			"operation": "concat", "target": "t", "fields": []interface{}{"a", 5},
		}},
		{"concat invalid field path entry", map[string]interface{}{
			"operation": "concat", "target": "t", "fields": []interface{}{"a..b"},
		}},
		{"hash missing source", map[string]interface{}{
			"operation": "hash", "target": "t", "algorithm": "sha1",
		}},
		{"hash empty source", map[string]interface{}{
			"operation": "hash", "target": "t", "source": "", "algorithm": "sha1",
		}},
		{"hash invalid source path", map[string]interface{}{
			"operation": "hash", "target": "t", "source": "a..b", "algorithm": "sha1",
		}},
		{"hash missing algorithm", map[string]interface{}{
			"operation": "hash", "target": "t", "source": "s",
		}},
		{"hash non-string algorithm", map[string]interface{}{
			"operation": "hash", "target": "t", "source": "s", "algorithm": 5,
		}},
		{"hash unknown algorithm", map[string]interface{}{
			"operation": "hash", "target": "t", "source": "s", "algorithm": "md5",
		}},
		{"slice missing source", map[string]interface{}{
			"operation": "slice", "target": "t", "start": 0,
		}},
		{"slice invalid source path", map[string]interface{}{
			"operation": "slice", "target": "t", "source": ".a", "start": 0,
		}},
		{"slice missing start", map[string]interface{}{
			"operation": "slice", "target": "t", "source": "s",
		}},
		{"slice non-integer start", map[string]interface{}{
			"operation": "slice", "target": "t", "source": "s", "start": "0",
		}},
		{"slice non-integer end", map[string]interface{}{
			"operation": "slice", "target": "t", "source": "s", "start": 0, "end": 4.5,
		}},
		{"str missing source", map[string]interface{}{
			"operation": "str", "target": "t",
		}},
		{"str invalid source path", map[string]interface{}{
			"operation": "str", "target": "t", "source": "a..b",
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			step, err := makeComputeFieldStep(tc.config)
			assert.Nil(t, step)
			require.Error(t, err)

			var connErr *connectorErrors.ConnectorError
			require.True(t, errors.As(err, &connErr))
			assert.Equal(t, "yaml_parser", connErr.Component)
			assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
		})
	}
}

// TestComputeFieldSplit covers selection, negative indexing, Go semantics
// for an absent separator, and runtime failures
func TestComputeFieldSplit(t *testing.T) {
	streamID := map[string]interface{}{"stream_id": "skf-cxrn:123:ion-stream:TOKEN:7"}

	t.Run("positive index", func(t *testing.T) {
		state, err := runComputeFieldStep(t, splitBaseConfig(1), streamID)
		require.NoError(t, err)
		assert.Equal(t, "123", state.Fields["revision_raw"])
	})

	t.Run("negative index", func(t *testing.T) {
		state, err := runComputeFieldStep(t, splitBaseConfig(-1), streamID)
		require.NoError(t, err)
		assert.Equal(t, "7", state.Fields["revision_raw"])
	})

	t.Run("separator absent yields whole string at 0 and -1", func(t *testing.T) {
		noSep := map[string]interface{}{"stream_id": "abc"}

		for _, index := range []int{0, -1} {
			state, err := runComputeFieldStep(t, splitBaseConfig(index), map[string]interface{}{"stream_id": "abc"})
			require.NoError(t, err)
			assert.Equal(t, "abc", state.Fields["revision_raw"])
		}

		for _, index := range []int{1, -2} {
			_, err := runComputeFieldStep(t, splitBaseConfig(index), noSep)
			requireParsingFailedError(t, err)
		}
	})

	t.Run("out of range", func(t *testing.T) {
		for _, index := range []int{5, -6} {
			_, err := runComputeFieldStep(t, splitBaseConfig(index), streamID)
			requireParsingFailedError(t, err)
		}
	})

	t.Run("missing source field", func(t *testing.T) {
		_, err := runComputeFieldStep(t, splitBaseConfig(0), map[string]interface{}{})
		requireParsingFailedError(t, err)
	})

	t.Run("non-string source", func(t *testing.T) {
		_, err := runComputeFieldStep(t, splitBaseConfig(0), map[string]interface{}{"stream_id": int64(7)})
		requireParsingFailedError(t, err)
	})

	t.Run("nested dot-path source", func(t *testing.T) {
		config := splitBaseConfig(-1)
		config["source"] = "envelope.stream_id"

		state, err := runComputeFieldStep(t, config, map[string]interface{}{
			"envelope": map[string]interface{}{"stream_id": "a:b:c"},
		})
		require.NoError(t, err)
		assert.Equal(t, "c", state.Fields["revision_raw"])
	})
}

// TestComputeFieldSplitProperties pins panic-freedom and agreement with the
// strings.Split reference against generated inputs
func TestComputeFieldSplitProperties(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		value := rapid.String().Draw(rt, "value")
		separator := rapid.StringN(1, 3, -1).Draw(rt, "separator")
		index := rapid.IntRange(-8, 8).Draw(rt, "index")

		config := map[string]interface{}{
			"operation": "split",
			"target":    "out",
			"source":    "in",
			"separator": separator,
			"index":     index,
		}

		step, err := makeComputeFieldStep(config)
		require.NoError(rt, err)

		state := &ParseState{Fields: map[string]interface{}{"in": value}}
		err = step(state)

		parts := strings.Split(value, separator)
		idx := index
		if idx < 0 {
			idx += len(parts)
		}

		if idx >= 0 && idx < len(parts) {
			require.NoError(rt, err)
			require.Equal(rt, parts[idx], state.Fields["out"])
		} else {
			require.Error(rt, err)
		}
	})
}

// hashConfig returns a valid compute_field hash config over the "in" field.
func hashConfig(algorithm string) map[string]interface{} {
	return map[string]interface{}{
		"operation": "hash",
		"target":    "out",
		"source":    "in",
		"algorithm": algorithm,
	}
}

// TestComputeFieldHash pins the golden GP sharding vectors (SHA-1 digest,
// first 4 hex chars = shard) and runtime failures
func TestComputeFieldHash(t *testing.T) {
	t.Run("sha1 golden shard vectors", func(t *testing.T) {
		shards := map[string]string{
			"sensor1234":                           "4c93",
			"Brunswick_724_1038":                   "7e14",
			"Monticello_1_2":                       "2f6b",
			"Cedar_Springs_10_20":                  "6d55",
			"test":                                 "a94a",
			"skf-cxrn:tenant1:ion-stream:ABC123:1": "b831",
			"skf-cxrn:tenant1:ion-stream:ABC123:2": "b4a9",
		}

		for input, shard := range shards {
			state, err := runComputeFieldStep(t, hashConfig("sha1"), map[string]interface{}{"in": input})
			require.NoError(t, err)

			digest, ok := state.Fields["out"].(string)
			require.True(t, ok)
			require.Len(t, digest, 40)
			assert.Equal(t, shard, digest[:4], "input %q", input)
		}
	})

	t.Run("sha1 full digest", func(t *testing.T) {
		state, err := runComputeFieldStep(t, hashConfig("sha1"), map[string]interface{}{"in": "test"})
		require.NoError(t, err)
		assert.Equal(t, "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3", state.Fields["out"])
	})

	t.Run("sha256 full digest", func(t *testing.T) {
		state, err := runComputeFieldStep(t, hashConfig("sha256"), map[string]interface{}{"in": "test"})
		require.NoError(t, err)
		assert.Equal(t, "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08", state.Fields["out"])
	})

	t.Run("nested dot-path source", func(t *testing.T) {
		config := hashConfig("sha1")
		config["source"] = "envelope.stream_id"

		state, err := runComputeFieldStep(t, config, map[string]interface{}{
			"envelope": map[string]interface{}{"stream_id": "test"},
		})
		require.NoError(t, err)
		assert.Equal(t, "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3", state.Fields["out"])
	})

	t.Run("missing source field", func(t *testing.T) {
		_, err := runComputeFieldStep(t, hashConfig("sha1"), map[string]interface{}{})
		requireParsingFailedError(t, err)
	})

	t.Run("non-string source", func(t *testing.T) {
		_, err := runComputeFieldStep(t, hashConfig("sha1"), map[string]interface{}{"in": int64(7)})
		requireParsingFailedError(t, err)
	})
}

// TestComputeFieldHashProperties pins agreement with the crypto/sha1 and
// crypto/sha256 reference digests against generated inputs
func TestComputeFieldHashProperties(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		value := rapid.String().Draw(rt, "value")
		algorithm := rapid.SampledFrom([]string{"sha1", "sha256"}).Draw(rt, "algorithm")

		step, err := makeComputeFieldStep(hashConfig(algorithm))
		require.NoError(rt, err)

		state := &ParseState{Fields: map[string]interface{}{"in": value}}
		require.NoError(rt, step(state))

		var want string

		if algorithm == "sha1" {
			d := sha1.Sum([]byte(value)) //nolint:gosec // reference digest for the sharding scheme
			want = hex.EncodeToString(d[:])
		} else {
			d := sha256.Sum256([]byte(value))
			want = hex.EncodeToString(d[:])
		}

		require.Equal(rt, want, state.Fields["out"])
	})
}

// sliceConfig returns a compute_field slice config over the "in" field;
// omit end for open-ended slices.
func sliceConfig(start int, end ...int) map[string]interface{} {
	config := map[string]interface{}{
		"operation": "slice",
		"target":    "out",
		"source":    "in",
		"start":     start,
	}

	if len(end) > 0 {
		config["end"] = end[0]
	}

	return config
}

// TestComputeFieldSlice covers Python slice semantics (negative indices,
// clamping, inverted ranges, omitted end) and runtime failures
func TestComputeFieldSlice(t *testing.T) {
	cases := []struct {
		name   string
		config map[string]interface{}
		want   string
	}{
		{"basic", sliceConfig(0, 4), "abcd"},
		{"omitted end", sliceConfig(2), "cdef"},
		{"negative start", sliceConfig(-2), "ef"},
		{"negative end", sliceConfig(0, -1), "abcde"},
		{"end clamped past length", sliceConfig(0, 10), "abcdef"},
		{"negative start clamped", sliceConfig(-10, 2), "ab"},
		{"inverted range", sliceConfig(4, 2), ""},
		{"start beyond length", sliceConfig(10, 20), ""},
		{"zero-width", sliceConfig(3, 3), ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state, err := runComputeFieldStep(t, tc.config, map[string]interface{}{"in": "abcdef"})
			require.NoError(t, err)
			assert.Equal(t, tc.want, state.Fields["out"])
		})
	}

	t.Run("empty source string", func(t *testing.T) {
		state, err := runComputeFieldStep(t, sliceConfig(0, 4), map[string]interface{}{"in": ""})
		require.NoError(t, err)
		assert.Equal(t, "", state.Fields["out"])
	})

	t.Run("nested dot-path source", func(t *testing.T) {
		config := sliceConfig(0, 4)
		config["source"] = "envelope.digest"

		state, err := runComputeFieldStep(t, config, map[string]interface{}{
			"envelope": map[string]interface{}{"digest": "b831ff00"},
		})
		require.NoError(t, err)
		assert.Equal(t, "b831", state.Fields["out"])
	})

	t.Run("missing source field", func(t *testing.T) {
		_, err := runComputeFieldStep(t, sliceConfig(0, 4), map[string]interface{}{})
		requireParsingFailedError(t, err)
	})

	t.Run("non-string source", func(t *testing.T) {
		_, err := runComputeFieldStep(t, sliceConfig(0, 4), map[string]interface{}{"in": 1.5})
		requireParsingFailedError(t, err)
	})
}

// pythonSliceRef is an inline Python-slice-semantics reference: negative
// indices count from the end, indices clamp to [0, len], inverted
// ranges yield "".
func pythonSliceRef(s string, start, end int, hasEnd bool) string {
	norm := func(idx int) int {
		if idx < 0 {
			idx += len(s)
		}

		if idx < 0 {
			return 0
		}

		if idx > len(s) {
			return len(s)
		}

		return idx
	}

	lo, hi := norm(start), len(s)
	if hasEnd {
		hi = norm(end)
	}

	if lo >= hi {
		return ""
	}

	return s[lo:hi]
}

// TestComputeFieldSliceProperties pins panic-freedom and agreement with the
// Python-semantics reference against generated inputs
func TestComputeFieldSliceProperties(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		value := rapid.String().Draw(rt, "value")
		start := rapid.IntRange(-20, 20).Draw(rt, "start")
		hasEnd := rapid.Bool().Draw(rt, "hasEnd")

		config := sliceConfig(start)

		end := 0
		if hasEnd {
			end = rapid.IntRange(-20, 20).Draw(rt, "end")
			config["end"] = end
		}

		step, err := makeComputeFieldStep(config)
		require.NoError(rt, err)

		state := &ParseState{Fields: map[string]interface{}{"in": value}}
		require.NoError(rt, step(state))

		require.Equal(rt, pythonSliceRef(value, start, end, hasEnd), state.Fields["out"])
	})
}

// strConfig returns a valid compute_field str config over the "in" field.
func strConfig() map[string]interface{} {
	return map[string]interface{}{
		"operation": "str",
		"target":    "out",
		"source":    "in",
	}
}

// TestComputeFieldStr pins the strconv outputs per supported type and the
// strict per-message error on anything else
func TestComputeFieldStr(t *testing.T) {
	supported := []struct {
		name  string
		value interface{}
		want  string
	}{
		{"int64", int64(42), "42"},
		{"negative int64", int64(-7), "-7"},
		{"float64", 2.5, "2.5"},
		{"float64 integral", 100.0, "100"},
		{"float64 scientific", 1e21, "1e+21"},
		{"bool true", true, "true"},
		{"bool false", false, "false"},
		{"string passthrough", "already", "already"},
	}

	for _, tc := range supported {
		t.Run(tc.name, func(t *testing.T) {
			state, err := runComputeFieldStep(t, strConfig(), map[string]interface{}{"in": tc.value})
			require.NoError(t, err)
			assert.Equal(t, tc.want, state.Fields["out"])
		})
	}

	unsupported := []struct {
		name  string
		value interface{}
	}{
		{"int", 7},
		{"nil", nil},
		{"map", map[string]interface{}{}},
		{"slice", []interface{}{}},
		{"time", time.Time{}},
	}

	for _, tc := range unsupported {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runComputeFieldStep(t, strConfig(), map[string]interface{}{"in": tc.value})
			requireParsingFailedError(t, err)
		})
	}

	t.Run("missing source field", func(t *testing.T) {
		_, err := runComputeFieldStep(t, strConfig(), map[string]interface{}{})
		requireParsingFailedError(t, err)
	})
}

// concatConfig returns a valid compute_field concat config over fields[].
func concatConfig(fields ...interface{}) map[string]interface{} {
	return map[string]interface{}{
		"operation": "concat",
		"target":    "out",
		"fields":    fields,
	}
}

// TestComputeFieldConcat covers strict concat semantics: quoted entries are
// literals, unquoted entries are dot-path lookups that must resolve to a
// string -- no coercion, no missing-field fallback.
func TestComputeFieldConcat(t *testing.T) {
	t.Run("literals only", func(t *testing.T) {
		state, err := runComputeFieldStep(t, concatConfig("\"skf\"", "\"/\"", "\"b831\""), map[string]interface{}{})
		require.NoError(t, err)
		assert.Equal(t, "skf/b831", state.Fields["out"])
	})

	t.Run("empty quoted literal", func(t *testing.T) {
		state, err := runComputeFieldStep(t, concatConfig("\"\"", "\"x\""), map[string]interface{}{})
		require.NoError(t, err)
		assert.Equal(t, "x", state.Fields["out"])
	})

	t.Run("literals and fields mixed", func(t *testing.T) {
		fields := map[string]interface{}{"facility": "plant1", "tag": "sensor01"}
		state, err := runComputeFieldStep(t, concatConfig("facility", "\":\"", "tag"), fields)
		require.NoError(t, err)
		assert.Equal(t, "plant1:sensor01", state.Fields["out"])
	})

	t.Run("nested dot-path source", func(t *testing.T) {
		fields := map[string]interface{}{
			"market": map[string]interface{}{"exchange": "NASDAQ"},
		}
		state, err := runComputeFieldStep(t, concatConfig("\"finance.\"", "market.exchange"), fields)
		require.NoError(t, err)
		assert.Equal(t, "finance.NASDAQ", state.Fields["out"])
	})

	t.Run("missing field is a parsing error, not a literal", func(t *testing.T) {
		_, err := runComputeFieldStep(t, concatConfig("absent"), map[string]interface{}{})
		requireParsingFailedError(t, err)
	})

	t.Run("non-string field is a parsing error, not coerced", func(t *testing.T) {
		_, err := runComputeFieldStep(t, concatConfig("num"), map[string]interface{}{"num": 42.5})
		requireParsingFailedError(t, err)
	})

	t.Run("lone quote is a field lookup, not a panic", func(t *testing.T) {
		_, err := runComputeFieldStep(t, concatConfig("\""), map[string]interface{}{})
		requireParsingFailedError(t, err)
	})
}

// TestYAMLParser_ShardedTableRouting chains hash → slice → concat →
// extract_table: the GP poor-man's-sharding composition. The revision pair
// pins the accepted property that hashing the full stream_id as-is moves a
// revision bump to a different shard.
func TestYAMLParser_ShardedTableRouting(t *testing.T) {
	config := YAMLConfig{
		Output: OutputSchema{
			Columns: []ColumnSchema{
				{Name: "value", Type: "double"},
			},
		},
		Transformations: []TransformSpec{
			{Step: "parse_json"},
			{Step: "compute_field", Config: map[string]interface{}{
				"operation": "hash",
				"algorithm": "sha1",
				"source":    "stream_id",
				"target":    "stream_id_hashed",
			}},
			{Step: "compute_field", Config: map[string]interface{}{
				"operation": "slice",
				"source":    "stream_id_hashed",
				"target":    "shard",
				"start":     0,
				"end":       4,
			}},
			{Step: "compute_field", Config: map[string]interface{}{
				"operation": "concat",
				"target":    "table_name",
				"fields":    []interface{}{"\"skf/\"", "shard"},
			}},
			{Step: "extract_table", Config: map[string]interface{}{
				"source": "table_name",
			}},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "value",
				"target": "value",
				"type":   "float64",
			}},
		},
	}

	parser, err := NewYAMLParserFromConfig(config)
	require.NoError(t, err)

	shards := map[string]string{
		"skf-cxrn:tenant1:ion-stream:ABC123:1": "skf/b831",
		"skf-cxrn:tenant1:ion-stream:ABC123:2": "skf/b4a9",
	}

	for streamID, table := range shards {
		msg := &nats.Msg{
			Subject: "test.subject",
			Data:    []byte(`{"stream_id": "` + streamID + `", "value": 1.5}`),
		}

		res, err := parser.Parse(msg)
		require.NoError(t, err)
		assert.Equal(t, OutcomeOK, res.Outcome)
		require.Len(t, res.Tables, 1)
		assert.Equal(t, table, stripNullTerminator(res.Tables[0].TableName))
	}

	// A message without a stream_id cannot produce a shard: hash fails,
	// $table is never set, and the structural floor drops the message.
	t.Run("missing stream_id is unusable", func(t *testing.T) {
		msg := &nats.Msg{
			Subject: "test.subject",
			Data:    []byte(`{"value": 1.5}`),
		}

		res, err := parser.Parse(msg)
		requireUnusable(t, res, err)
	})
}

// BenchmarkComputeFieldPipeline measures the per-message cost of the GP
// sharded-routing composition: SHA-1 hash → slice 0:4 → concat "skf/"+shard.
// ADR-005 claims <5% pipeline overhead for computed fields; this produces
// the actual number.
func BenchmarkComputeFieldPipeline(b *testing.B) {
	configs := []map[string]interface{}{
		{"operation": "hash", "algorithm": "sha1", "source": "stream_id", "target": "stream_id_hashed"},
		{"operation": "slice", "source": "stream_id_hashed", "target": "shard", "start": 0, "end": 4},
		{"operation": "concat", "target": "table_name", "fields": []interface{}{"\"skf/\"", "shard"}},
	}

	steps := make([]TransformationStep, 0, len(configs))

	for _, config := range configs {
		step, err := makeComputeFieldStep(config)
		if err != nil {
			b.Fatal(err)
		}

		steps = append(steps, step)
	}

	b.ReportAllocs()

	for b.Loop() {
		state := &ParseState{Fields: map[string]interface{}{
			"stream_id": "skf-cxrn:tenant1:ion-stream:ABC123:1",
		}}

		for _, step := range steps {
			err := step(state)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

// TestCompileFieldPath covers config-time path validation and the compiled
// lookup closure: nested resolution, missing keys, non-map intermediates.
func TestCompileFieldPath(t *testing.T) {
	t.Run("invalid path formats fail at compile time", func(t *testing.T) {
		for _, path := range []string{"", ".a", "a.", "a..b"} {
			lookup, err := compileFieldPath(path)
			assert.Nil(t, lookup)
			require.Error(t, err, "path %q must be rejected", path)
		}
	})

	fields := map[string]interface{}{
		"flat": "v",
		"nested": map[string]interface{}{
			"inner": map[string]interface{}{"leaf": 42.5},
		},
		"scalar": "not-a-map",
	}

	cases := []struct {
		name  string
		path  string
		value interface{}
		found bool
	}{
		{"flat hit", "flat", "v", true},
		{"nested hit", "nested.inner.leaf", 42.5, true},
		{"missing leaf", "nested.inner.absent", nil, false},
		{"missing intermediate", "nested.absent.leaf", nil, false},
		{"non-map intermediate", "scalar.leaf", nil, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lookup, err := compileFieldPath(tc.path)
			require.NoError(t, err)

			value, found := lookup(fields)
			assert.Equal(t, tc.found, found)
			assert.Equal(t, tc.value, value)
		})
	}
}

// TestSafeParseNumberDotPath pins the compiled dot-path retrofit: nested
// sources resolve, malformed paths fail at compile time.
func TestSafeParseNumberDotPath(t *testing.T) {
	t.Run("invalid source path fails at compile time", func(t *testing.T) {
		step, err := makeSafeParseNumberStep(map[string]interface{}{
			"source": "a..b", "target": "out",
		})
		assert.Nil(t, step)
		require.Error(t, err)

		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
	})

	t.Run("nested source resolves", func(t *testing.T) {
		step, err := makeSafeParseNumberStep(map[string]interface{}{
			"source": "metrics.raw", "target": "out",
		})
		require.NoError(t, err)

		state := &ParseState{Fields: map[string]interface{}{
			"metrics": map[string]interface{}{"raw": "42.5"},
		}}
		require.NoError(t, step(state))
		assert.Equal(t, 42.5, state.Fields["out"])
	})
}

// TestExtractFieldInvalidPathCompileError pins the fail-fast change: a
// malformed extract_field source is a startup error, not a per-message one.
func TestExtractFieldInvalidPathCompileError(t *testing.T) {
	step, err := makeExtractFieldStep(map[string]interface{}{
		"source": ".bad", "target": "out", "type": "auto",
	})
	assert.Nil(t, step)
	require.Error(t, err)

	var connErr *connectorErrors.ConnectorError
	require.True(t, errors.As(err, &connErr))
	assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
}

// TestExtractFieldTypeRequired pins the compile-time requirement: omitting
// 'type' is a Configuration error; explicit "auto" stays valid and passes
// parser-native types through untouched
func TestExtractFieldTypeRequired(t *testing.T) {
	pipelineConfig := func(fieldConfig map[string]interface{}) YAMLConfig {
		return YAMLConfig{
			Output: OutputSchema{
				Columns: []ColumnSchema{
					{Name: "value", Type: "int64"},
				},
			},
			Transformations: []TransformSpec{
				{Step: "parse_json", Config: map[string]interface{}{}},
				{Step: "extract_field", Config: fieldConfig},
				{Step: "extract_table", Config: map[string]interface{}{"value": "events"}},
			},
		}
	}

	t.Run("missing type fails compile", func(t *testing.T) {
		_, err := NewYAMLParserFromConfig(pipelineConfig(map[string]interface{}{
			"source": "value",
			"target": "value",
		}))
		require.Error(t, err)

		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, connectorErrors.ErrCodeInvalidConfig, connErr.Code)
		assert.Contains(t, connErr.Error(), "extract_field")
	})

	t.Run("empty type fails compile", func(t *testing.T) {
		_, err := NewYAMLParserFromConfig(pipelineConfig(map[string]interface{}{
			"source": "value",
			"target": "value",
			"type":   "",
		}))
		require.Error(t, err)
	})

	t.Run("explicit auto compiles and passes through", func(t *testing.T) {
		step, err := makeExtractFieldStep(map[string]interface{}{
			"source": "value",
			"target": "out",
			"type":   "auto",
		})
		require.NoError(t, err)

		state := &ParseState{Fields: map[string]interface{}{"value": int64(42)}}
		require.NoError(t, step(state))
		assert.Equal(t, int64(42), state.Fields["out"])
	})
}
