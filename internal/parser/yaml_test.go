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
		defer os.Remove(tempFile.Name())

		_, err = tempFile.WriteString("invalid: yaml: content: [}")
		require.NoError(t, err)
		tempFile.Close()

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

			jsonData := fmt.Sprintf(`{"temp": %f, "humid": %f, "loc": "%s"}`, temp, humid, location)
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
			{Step: "extract_timestamp", Config: map[string]interface{}{
				"source": "ts",
				"target": "timestamp",
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
