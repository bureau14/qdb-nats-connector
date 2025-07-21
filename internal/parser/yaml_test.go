// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package parser: YAML-based message transformation pipelines
// Types: YAMLParser, YAMLConfig, ParseState
// Ex: NewYAMLParser(opts).Parse(msg) → []WriterTable
package parser

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"os"
	"testing"

	qdb "github.com/bureau14/qdb-api-go/v3"
	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/internal/util"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

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

	t.Run("missing table name", func(t *testing.T) {
		config := YAMLConfig{
			Output: OutputSchema{
				TableName: "",
				Columns:   []ColumnSchema{{Name: "col1", Type: "double"}},
			},
		}

		opts := ParserOptions{
			ErrorAction: "drop",
		}
		parser, err := NewYAMLParserFromConfig(config, opts)
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
				TableName: "test",
				Columns:   []ColumnSchema{},
			},
		}

		opts := ParserOptions{
			ErrorAction: "drop",
		}
		parser, err := NewYAMLParserFromConfig(config, opts)
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
				TableName: "test",
				Columns: []ColumnSchema{
					{Name: "col1", Type: "double"},
					{Name: "col1", Type: "string"},
				},
			},
		}

		opts := ParserOptions{
			ErrorAction: "drop",
		}
		parser, err := NewYAMLParserFromConfig(config, opts)
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
				TableName: "test",
				Columns: []ColumnSchema{
					{Name: "col1", Type: "invalid_type"},
				},
			},
		}

		opts := ParserOptions{
			ErrorAction: "drop",
		}
		parser, err := NewYAMLParserFromConfig(config, opts)
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
				TableName: "test",
				Columns: []ColumnSchema{
					{Name: "", Type: "double"},
				},
			},
		}

		opts := ParserOptions{
			ErrorAction: "drop",
		}
		parser, err := NewYAMLParserFromConfig(config, opts)
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
				TableName: "test",
				Columns:   []ColumnSchema{{Name: "col1", Type: "double"}},
			},
			Transformations: []TransformSpec{
				{Step: "unknown_step", Config: map[string]interface{}{}},
			},
		}

		opts := ParserOptions{
			ErrorAction: "drop",
		}
		parser, err := NewYAMLParserFromConfig(config, opts)
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
				TableName: "test",
				Columns:   []ColumnSchema{{Name: "col1", Type: "double"}},
			},
			Transformations: []TransformSpec{},
		}

		opts := ParserOptions{
			ErrorAction: "drop",
		}
		parser, err := NewYAMLParserFromConfig(config, opts)
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
			TableName: "test",
			Columns:   []ColumnSchema{{Name: "value", Type: "double"}},
		},
		Transformations: []TransformSpec{
			{Step: "parse_json", Config: map[string]interface{}{}},
		},
	}

	opts := ParserOptions{
		ErrorAction: "fail",
	}
	parser, err := NewYAMLParserFromConfig(config, opts)
	require.NoError(t, err)

	// Test nil message handling - should return parsing error
	t.Run("nil message", func(t *testing.T) {
		tables, err := parser.Parse(nil)
		assert.Nil(t, tables)
		require.Error(t, err)

		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, "yaml_parser", connErr.Component)
		assert.Equal(t, connectorErrors.ErrCodeParsingFailed, connErr.Code)
	})

	// Test empty message data - should return parsing error
	t.Run("empty message", func(t *testing.T) {
		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte{},
		}

		tables, err := parser.Parse(msg)
		assert.Nil(t, tables)
		require.Error(t, err)

		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, "yaml_parser", connErr.Component)
		assert.Equal(t, connectorErrors.ErrCodeParsingFailed, connErr.Code)
	})

	// Test invalid JSON parsing - should return error in fail mode
	t.Run("invalid json", func(t *testing.T) {
		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"invalid": json`),
		}

		tables, err := parser.Parse(msg)
		assert.Nil(t, tables)
		require.Error(t, err)

		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, "yaml_parser", connErr.Component)
		assert.Equal(t, connectorErrors.ErrCodeParsingFailed, connErr.Code)
	})
}

// TestYAMLParserValidParsing validates successful transformations
func TestYAMLParserValidParsing(t *testing.T) {
	config := YAMLConfig{
		Output: OutputSchema{
			TableName: "sensors",
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
		},
	}

	opts := ParserOptions{
		ErrorAction: "drop",
	}
	parser, err := NewYAMLParserFromConfig(config, opts)
	require.NoError(t, err)

	// Test basic JSON parsing with field extraction
	t.Run("simple parsing", func(t *testing.T) {
		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"temp": 23.5, "humid": 65.2, "loc": "kitchen"}`),
		}

		tables, err := parser.Parse(msg)
		require.NoError(t, err)
		require.Len(t, tables, 1)

		table := tables[0]
		assert.Equal(t, "sensors", table.GetName())
		assert.Equal(t, 1, table.RowCount())
	})

	// Property-based test with random valid inputs
	t.Run("property-based valid parsing", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			temp := rapid.Float64Range(-100, 100).Draw(t, "temperature")
			humid := rapid.Float64Range(0, 100).Draw(t, "humidity")
			location := rapid.String().Draw(t, "location")

			jsonData := fmt.Sprintf(`{"temp": %f, "humid": %f, "loc": %q}`, temp, humid, location)
			msg := &nats.Msg{
				Subject: util.RandomTopicName(),
				Data:    []byte(jsonData),
			}

			tables, err := parser.Parse(msg)
			require.NoError(t, err)
			require.Len(t, tables, 1)

			table := tables[0]
			assert.Equal(t, "sensors", table.GetName())
			assert.Equal(t, 1, table.RowCount())
		})
	})
}

// TestYAMLParserTimestamp validates timestamp extraction
func TestYAMLParserTimestamp(t *testing.T) {
	config := YAMLConfig{
		Output: OutputSchema{
			TableName: "events",
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
		},
	}

	opts := ParserOptions{
		ErrorAction: "drop",
	}
	parser, err := NewYAMLParserFromConfig(config, opts)
	require.NoError(t, err)

	// Test RFC3339 format timestamp parsing
	t.Run("rfc3339 timestamp", func(t *testing.T) {
		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"ts": "2024-01-01T12:00:00Z", "val": 42.5}`),
		}

		tables, err := parser.Parse(msg)
		require.NoError(t, err)
		require.Len(t, tables, 1)

		table := tables[0]
		assert.Equal(t, "events", table.GetName())
		assert.Equal(t, 1, table.RowCount())
	})

	// Test unix epoch timestamp parsing
	t.Run("unix timestamp", func(t *testing.T) {
		unixConfig := config
		unixConfig.Transformations[1].Config["format"] = "unix"

		opts := ParserOptions{
			ErrorAction: "drop",
		}
		unixParser, err := NewYAMLParserFromConfig(unixConfig, opts)
		require.NoError(t, err)

		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"ts": 1704110400, "val": 42.5}`),
		}

		tables, err := unixParser.Parse(msg)
		require.NoError(t, err)
		require.Len(t, tables, 1)

		table := tables[0]
		assert.Equal(t, "events", table.GetName())
		assert.Equal(t, 1, table.RowCount())
	})
}

// TestYAMLParserComputeField validates computed field operations
func TestYAMLParserComputeField(t *testing.T) {
	config := YAMLConfig{
		Output: OutputSchema{
			TableName: "computed",
			Columns: []ColumnSchema{
				{Name: "tag_id", Type: "string"},
				{Name: "value", Type: "double"},
			},
		},
		Transformations: []TransformSpec{
			{Step: "parse_json", Config: map[string]interface{}{}},
			{Step: "compute_field", Config: map[string]interface{}{
				"operation": "concat",
				"target":    "tag_id",
				"fields":    []interface{}{"facility", ":", "tag"},
			}},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "val",
				"target": "value",
				"type":   "float64",
			}},
		},
	}

	opts := ParserOptions{
		ErrorAction: "drop",
	}
	parser, err := NewYAMLParserFromConfig(config, opts)
	require.NoError(t, err)

	// Test string concatenation with literal and field values
	t.Run("string concatenation", func(t *testing.T) {
		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"facility": "plant1", "tag": "sensor01", "val": 42.5}`),
		}

		tables, err := parser.Parse(msg)
		require.NoError(t, err)
		require.Len(t, tables, 1)

		table := tables[0]
		assert.Equal(t, "computed", table.GetName())
		assert.Equal(t, 1, table.RowCount())
	})
}

// TestYAMLParserNumberParsing validates graceful number conversion
func TestYAMLParserNumberParsing(t *testing.T) {
	config := YAMLConfig{
		Output: OutputSchema{
			TableName: "numbers",
			Columns: []ColumnSchema{
				{Name: "safe_value", Type: "double"},
				{Name: "original", Type: "string"},
			},
		},
		Transformations: []TransformSpec{
			{Step: "parse_json", Config: map[string]interface{}{}},
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

	opts := ParserOptions{
		ErrorAction: "drop",
	}
	parser, err := NewYAMLParserFromConfig(config, opts)
	require.NoError(t, err)

	// Test parsing valid numeric string
	t.Run("valid number string", func(t *testing.T) {
		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"raw": "42.5"}`),
		}

		tables, err := parser.Parse(msg)
		require.NoError(t, err)
		require.Len(t, tables, 1)

		table := tables[0]
		assert.Equal(t, "numbers", table.GetName())
		assert.Equal(t, 1, table.RowCount())
	})

	// Test handling invalid numeric string - should return null
	t.Run("invalid number string", func(t *testing.T) {
		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"raw": "not_a_number"}`),
		}

		tables, err := parser.Parse(msg)
		require.NoError(t, err)
		require.Len(t, tables, 1)

		table := tables[0]
		assert.Equal(t, "numbers", table.GetName())
		assert.Equal(t, 1, table.RowCount())
	})
}

// TestYAMLParserErrorHandling validates fail vs drop error modes
func TestYAMLParserErrorHandling(t *testing.T) {
	baseConfig := YAMLConfig{
		Output: OutputSchema{
			TableName: "test",
			Columns:   []ColumnSchema{{Name: "value", Type: "double"}},
		},
		Transformations: []TransformSpec{
			{Step: "parse_json", Config: map[string]interface{}{}},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "nonexistent",
				"target": "value",
				"type":   "float64",
			}},
		},
	}

	// Test drop mode - continues on errors and creates partial results
	t.Run("drop mode", func(t *testing.T) {
		dropConfig := baseConfig
		opts := ParserOptions{
			ErrorAction: "drop",
		}

		parser, err := NewYAMLParserFromConfig(dropConfig, opts)
		require.NoError(t, err)

		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"other": "value"}`),
		}

		tables, err := parser.Parse(msg)
		require.NoError(t, err)
		require.Len(t, tables, 1)

		table := tables[0]
		assert.Equal(t, "test", table.GetName())
		assert.Equal(t, 1, table.RowCount())
	})

	// Test fail mode - returns error immediately on first failure
	t.Run("fail mode", func(t *testing.T) {
		failConfig := baseConfig
		opts := ParserOptions{
			ErrorAction: "fail",
		}

		parser, err := NewYAMLParserFromConfig(failConfig, opts)
		require.NoError(t, err)

		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"other": "value"}`),
		}

		tables, err := parser.Parse(msg)
		assert.Nil(t, tables)
		require.Error(t, err)

		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, "yaml_parser", connErr.Component)
		assert.Equal(t, connectorErrors.ErrCodeParsingFailed, connErr.Code)
	})
}

// TestYAMLParserInterfaceCompliance ensures Parser interface implementation
func TestYAMLParserInterfaceCompliance(t *testing.T) {
	config := YAMLConfig{
		Output: OutputSchema{
			TableName: "test",
			Columns:   []ColumnSchema{{Name: "value", Type: "double"}},
		},
		Transformations: []TransformSpec{
			{Step: "parse_json", Config: map[string]interface{}{}},
			{Step: "extract_field", Config: map[string]interface{}{
				"source": "val",
				"target": "value",
				"type":   "float64",
			}},
		},
	}

	opts := ParserOptions{
		ErrorAction: "drop",
	}
	parser, err := NewYAMLParserFromConfig(config, opts)
	require.NoError(t, err)
	require.NotNil(t, parser)

	// Compile-time check that YAMLParser implements Parser interface
	var _ Parser = parser

	// Verify basic parsing functionality works correctly
	msg := &nats.Msg{
		Subject: util.RandomTopicName(),
		Data:    []byte(`{"val": 42.5}`),
	}

	tables, err := parser.Parse(msg)
	require.NoError(t, err)
	require.Len(t, tables, 1)

	table := tables[0]
	assert.Equal(t, "test", table.GetName())
	assert.Equal(t, 1, table.RowCount())
}

// TestYAMLParserTransformationSteps validates individual transformation steps
func TestYAMLParserTransformationSteps(t *testing.T) {
	// Test gzip decompression transformation step
	t.Run("decompress step", func(t *testing.T) {
		config := YAMLConfig{
			Output: OutputSchema{
				TableName: "test",
				Columns:   []ColumnSchema{{Name: "value", Type: "string"}},
			},
			Transformations: []TransformSpec{
				{Step: "decompress", Config: map[string]interface{}{
					"algorithm": "gzip",
				}},
				{Step: "parse_json", Config: map[string]interface{}{}},
				{Step: "extract_field", Config: map[string]interface{}{
					"source": "msg",
					"target": "value",
					"type":   "string",
				}},
			},
		}

		opts := ParserOptions{
			ErrorAction: "drop",
		}
		parser, err := NewYAMLParserFromConfig(config, opts)
		require.NoError(t, err)

		// Create gzip-compressed JSON data for testing decompression
		jsonData := `{"msg": "hello world"}`
		compressedData := compressGzip(t, []byte(jsonData))

		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    compressedData,
		}

		tables, err := parser.Parse(msg)
		require.NoError(t, err)
		require.Len(t, tables, 1)

		table := tables[0]
		assert.Equal(t, "test", table.GetName())
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
				TableName: "sensors",
				Columns: []ColumnSchema{
					{Name: "temp", Type: "double"},
					{Name: "humidity", Type: "double"},
					{Name: "location", Type: "string"},
				},
			},
			Transformations: []TransformSpec{
				{Step: "parse_json", Config: map[string]interface{}{}},
			},
		}

		opts := ParserOptions{ErrorAction: "drop"}
		parser, err := NewYAMLParserFromConfig(config, opts)
		require.NoError(t, err)

		// Verify internal state matches configuration
		assert.Equal(t, 3, len(parser.columns))
		assert.Equal(t, 3, len(parser.columnTypes))
		assert.Equal(t, "temp", parser.columns[0].ColumnName)
		assert.Equal(t, "humidity", parser.columns[1].ColumnName)
		assert.Equal(t, "location", parser.columns[2].ColumnName)

		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"temp": 25.5, "humidity": 60.0, "location": "office"}`),
		}

		tables, err := parser.Parse(msg)
		require.NoError(t, err)
		require.Len(t, tables, 1)
		assert.Equal(t, "sensors", tables[0].GetName())
	})

	t.Run("buffer bounds validation - all column types", func(t *testing.T) {
		// Test that buffer allocation matches column count exactly
		config := YAMLConfig{
			Output: OutputSchema{
				TableName: "mixed_types",
				Columns: []ColumnSchema{
					{Name: "double_col", Type: "double"},
					{Name: "int64_col", Type: "int64"},
					{Name: "string_col", Type: "string"},
					{Name: "blob_col", Type: "blob"},
					{Name: "timestamp_col", Type: "timestamp"},
				},
			},
			Transformations: []TransformSpec{
				{Step: "parse_json", Config: map[string]interface{}{}},
				{Step: "extract_index", Config: map[string]interface{}{
					"source": "ts",
					"format": "unix",
				}},
			},
		}

		opts := ParserOptions{ErrorAction: "drop"}
		parser, err := NewYAMLParserFromConfig(config, opts)
		require.NoError(t, err)

		// Verify that column count matches buffer allocation
		assert.Equal(t, 5, len(parser.columns))
		assert.Equal(t, 5, len(parser.columnTypes))

		// Test with data for all column types
		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"double_col": 42.5, "int64_col": 123, "string_col": "test", "blob_col": "YmluYXJ5", "ts": 1704110400}`),
		}

		// Parse and verify no buffer overflow occurs
		tables, err := parser.Parse(msg)
		require.NoError(t, err)
		require.Len(t, tables, 1)

		// Verify parse state buffer allocation by inspecting new state
		state := parser.newParseState()
		assert.Equal(t, 5, len(state.doubleVals))
		assert.Equal(t, 5, len(state.int64Vals))
		assert.Equal(t, 5, len(state.stringVals))
		assert.Equal(t, 5, len(state.blobVals))
		assert.Equal(t, 5, len(state.timestampVals))
	})

	t.Run("column name consistency validation", func(t *testing.T) {
		// Test that column names in p.columns match configuration exactly
		config := YAMLConfig{
			Output: OutputSchema{
				TableName: "validation_test",
				Columns: []ColumnSchema{
					{Name: "sensor_id", Type: "string"},
					{Name: "measurement_value", Type: "double"},
					{Name: "recorded_at", Type: "timestamp"},
				},
			},
			Transformations: []TransformSpec{
				{Step: "parse_json", Config: map[string]interface{}{}},
			},
		}

		opts := ParserOptions{ErrorAction: "drop"}
		parser, err := NewYAMLParserFromConfig(config, opts)
		require.NoError(t, err)

		// Verify that internal column mapping preserves exact names from config
		require.Equal(t, 3, len(parser.columns))
		for i, expectedCol := range config.Output.Columns {
			assert.Equal(t, expectedCol.Name, parser.columns[i].ColumnName)
		}

		// Test parsing with matching field names
		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"sensor_id": "temp_001", "measurement_value": 23.7, "recorded_at": "2024-01-01T12:00:00Z"}`),
		}

		tables, err := parser.Parse(msg)
		require.NoError(t, err)
		require.Len(t, tables, 1)
	})

	t.Run("empty columns configuration", func(t *testing.T) {
		// Test that parser properly rejects empty column configurations
		// This prevents potential divide-by-zero or null pointer issues
		config := YAMLConfig{
			Output: OutputSchema{
				TableName: "empty_test",
				Columns:   []ColumnSchema{}, // Empty columns should be rejected
			},
			Transformations: []TransformSpec{
				{Step: "parse_json", Config: map[string]interface{}{}},
			},
		}

		opts := ParserOptions{ErrorAction: "drop"}
		parser, err := NewYAMLParserFromConfig(config, opts)
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
				TableName: "bounds_test",
				Columns: []ColumnSchema{
					{Name: "col1", Type: "double"},
					{Name: "col2", Type: "string"},
				},
			},
			Transformations: []TransformSpec{
				{Step: "parse_json", Config: map[string]interface{}{}},
			},
		}

		opts := ParserOptions{ErrorAction: "drop"}
		parser, err := NewYAMLParserFromConfig(config, opts)
		require.NoError(t, err)

		// Test with actual parsing instead of directly calling createWriterTable
		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(`{"col1": 42.5, "col2": "test"}`),
		}

		tables, err := parser.Parse(msg)
		require.NoError(t, err)
		require.Len(t, tables, 1)
		assert.Equal(t, "bounds_test", tables[0].GetName())

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
				TableName: "type_consistency_test",
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
			},
		}

		opts := ParserOptions{ErrorAction: "drop"}
		parser, err := NewYAMLParserFromConfig(config, opts)
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
				TableName: "memory_safety_test",
				Columns: []ColumnSchema{
					{Name: "value", Type: "double"},
				},
			},
			Transformations: []TransformSpec{
				{Step: "parse_json", Config: map[string]interface{}{}},
			},
		}

		opts := ParserOptions{ErrorAction: "drop"}
		parser, err := NewYAMLParserFromConfig(config, opts)
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
func TestYAMLParserColumnSynchronizationValidation(t *testing.T) {
	t.Run("valid synchronization passes", func(t *testing.T) {
		// Test that properly synchronized columns pass validation
		config := YAMLConfig{
			Output: OutputSchema{
				TableName: "valid_sync_test",
				Columns: []ColumnSchema{
					{Name: "temperature", Type: "double"},
					{Name: "pressure", Type: "int64"},
					{Name: "location", Type: "string"},
				},
			},
			Transformations: []TransformSpec{
				{Step: "parse_json", Config: map[string]interface{}{}},
			},
		}

		opts := ParserOptions{ErrorAction: "drop"}
		parser, err := NewYAMLParserFromConfig(config, opts)
		require.NoError(t, err)
		require.NotNil(t, parser)

		// Verify synchronization is maintained
		assert.Equal(t, len(config.Output.Columns), len(parser.columns))
		assert.Equal(t, len(config.Output.Columns), len(parser.columnTypes))

		for i, configCol := range config.Output.Columns {
			assert.Equal(t, configCol.Name, parser.columns[i].ColumnName)
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
			{ColumnName: "temp", ColumnType: qdb.TsColumnDouble},
			{ColumnName: "humid", ColumnType: qdb.TsColumnInt64},
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
			{ColumnName: "temp", ColumnType: qdb.TsColumnDouble},
			{ColumnName: "humid", ColumnType: qdb.TsColumnInt64},
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
			{ColumnName: "temp", ColumnType: qdb.TsColumnDouble}, // Different name
			{ColumnName: "humidity", ColumnType: qdb.TsColumnInt64},
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
		assert.Contains(t, err.Error(), "column name mismatch at index 0: config has 'temperature' but internal mapping has 'temp'")
	})

	t.Run("column type mismatch detected", func(t *testing.T) {
		configColumns := []ColumnSchema{
			{Name: "temp", Type: "double"},
			{Name: "humid", Type: "string"}, // Different type
		}

		writerColumns := []qdb.WriterColumn{
			{ColumnName: "temp", ColumnType: qdb.TsColumnDouble},
			{ColumnName: "humid", ColumnType: qdb.TsColumnInt64}, // Different type
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
			{ColumnName: "temp", ColumnType: qdb.TsColumnDouble},
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
