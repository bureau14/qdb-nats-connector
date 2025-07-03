package parser

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"testing"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/internal/util"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// TestJsonParserInvalidInputs tests all error cases using property-based testing
func TestJsonParserInvalidInputs(t *testing.T) {
	parser, err := NewJsonParser()
	require.NoError(t, err)

	t.Run("nil message", func(t *testing.T) {
		tables, err := parser.Parse(nil)
		assert.Nil(t, tables)
		require.Error(t, err)

		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, "json_parser", connErr.Component)
		assert.Equal(t, connectorErrors.ErrCodeParsingFailed, connErr.Code)
		assert.Contains(t, err.Error(), "parse message")
	})

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
		assert.Equal(t, "json_parser", connErr.Component)
		assert.Equal(t, connectorErrors.ErrCodeParsingFailed, connErr.Code)
		assert.Contains(t, err.Error(), "parse message")
	})

	t.Run("invalid json strings", func(t *testing.T) {
		invalidJsonPatterns := []string{
			`{"key": invalid}`,
			`{"key": "value"`,
			`{"key": "value}`,
			`{"key": "val\x00ue"}`,
			`{"key": "value",}`,
			`{key: "value"}`,
			`{'key': 'value'}`,
			`{"key": "value", "key2":}`,
		}

		for _, pattern := range invalidJsonPatterns {
			msg := &nats.Msg{
				Subject: util.RandomTopicName(),
				Data:    []byte(pattern),
			}

			tables, err := parser.Parse(msg)
			assert.Nil(t, tables)
			require.Error(t, err)

			var connErr *connectorErrors.ConnectorError
			require.True(t, errors.As(err, &connErr))
			assert.Equal(t, "json_parser", connErr.Component)
			assert.Equal(t, connectorErrors.ErrCodeParsingFailed, connErr.Code)
			assert.Contains(t, err.Error(), "parse message")
		}
	})

	t.Run("missing table key", func(t *testing.T) {
		invalidStructures := []string{
			`{}`,
			`{"key": "value"}`,
			`{"field1": "value1", "field2": 42}`,
		}

		for _, structure := range invalidStructures {
			msg := &nats.Msg{
				Subject: util.RandomTopicName(),
				Data:    []byte(structure),
			}

			tables, err := parser.Parse(msg)
			assert.Nil(t, tables)
			require.Error(t, err)

			var connErr *connectorErrors.ConnectorError
			require.True(t, errors.As(err, &connErr))
			assert.Equal(t, "json_parser", connErr.Component)
			assert.Equal(t, connectorErrors.ErrCodeParsingFailed, connErr.Code)
		}
	})

	t.Run("nested structures", func(t *testing.T) {
		nestedStructures := []string{
			`{"$table": "foobar", "key": {"nested": "value"}}`,
			`{"$table": "foobar", "key": ["value1", "value2"]}`,
			`{"$table": "foobar", "key1": "value1", "key2": {"nested": "value"}}`,
			`{"$table": "foobar", "key": [{"nested": "value"}]}`,
			`{"$table": "foobar", "key": {"level1": {"level2": "value"}}}`,
		}

		for _, structure := range nestedStructures {
			msg := &nats.Msg{
				Subject: util.RandomTopicName(),
				Data:    []byte(structure),
			}

			tables, err := parser.Parse(msg)
			assert.Nil(t, tables)
			require.Error(t, err)

			var connErr *connectorErrors.ConnectorError
			require.True(t, errors.As(err, &connErr))
			assert.Equal(t, "json_parser", connErr.Component)
			assert.Equal(t, connectorErrors.ErrCodeParsingFailed, connErr.Code)
		}
	})

	t.Run("all null values", func(t *testing.T) {
		nullOnlyJson := `{"$table": "foobar", "field1": null, "field2": null}`
		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    []byte(nullOnlyJson),
		}

		tables, err := parser.Parse(msg)
		assert.Nil(t, tables)
		require.Error(t, err)

		var connErr *connectorErrors.ConnectorError
		require.True(t, errors.As(err, &connErr))
		assert.Equal(t, "json_parser", connErr.Component)
		assert.Equal(t, connectorErrors.ErrCodeParsingFailed, connErr.Code)
	})

	t.Run("property-based invalid json fuzzing", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			// Generate random byte sequences that are likely invalid JSON
			data := rapid.SliceOfN(rapid.Byte(), 1, 1000).Draw(t, "invalidData")

			// Skip if it accidentally creates valid JSON
			var testMap map[string]interface{}
			if json.Unmarshal(data, &testMap) == nil && len(testMap) > 0 {
				// Check if it contains nested structures or is missing $table
				hasNested := false
				hasTable := false
				for k, v := range testMap {
					if k == "$table" {
						hasTable = true
					}
					switch v.(type) {
					case map[string]interface{}, []interface{}:
						hasNested = true
					}
				}
				if !hasNested && hasTable {
					t.Skip("accidentally generated valid single-depth JSON with table")
				}
			}

			msg := &nats.Msg{
				Subject: util.RandomTopicName(),
				Data:    data,
			}

			tables, err := parser.Parse(msg)
			assert.Nil(t, tables)
			assert.Error(t, err)

			var connErr *connectorErrors.ConnectorError
			require.True(t, errors.As(err, &connErr))
			assert.Equal(t, "json_parser", connErr.Component)
			assert.Equal(t, connectorErrors.ErrCodeParsingFailed, connErr.Code)
		})
	})
}

// TestJsonParserValidTypes tests all valid type scenarios using property-based testing
func TestJsonParserValidTypes(t *testing.T) {
	parser, err := NewJsonParser()
	require.NoError(t, err)

	t.Run("single valid types", func(t *testing.T) {
		testCases := []struct {
			name     string
			valueGen func(*rapid.T) interface{}
			skipCond func(interface{}) bool
		}{
			{
				name: "string values",
				valueGen: func(t *rapid.T) interface{} {
					return rapid.String().Draw(t, "value")
				},
				skipCond: nil,
			},
			{
				name: "numeric values",
				valueGen: func(t *rapid.T) interface{} {
					return rapid.Float64Range(-1e10, 1e10).Draw(t, "value")
				},
				skipCond: func(v interface{}) bool {
					if f, ok := v.(float64); ok {
						return math.IsNaN(f) || math.IsInf(f, 0)
					}

					return false
				},
			},
			{
				name: "boolean values",
				valueGen: func(t *rapid.T) interface{} {
					return rapid.Bool().Draw(t, "value")
				},
				skipCond: nil,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				rapid.Check(t, func(t *rapid.T) {
					key := rapid.StringMatching(`[a-zA-Z][a-zA-Z0-9_]*`).Draw(t, "key")
					value := tc.valueGen(t)

					if tc.skipCond != nil && tc.skipCond(value) {
						t.Skip("skipping special values")
					}

					jsonData := map[string]interface{}{
						"$table": "foobar",
						key:      value,
					}

					jsonBytes, err := json.Marshal(jsonData)
					require.NoError(t, err)

					msg := &nats.Msg{
						Subject: util.RandomTopicName(),
						Data:    jsonBytes,
					}

					tables, err := parser.Parse(msg)
					require.NoError(t, err)
					require.Len(t, tables, 1)

					table := tables[0]
					assert.Equal(t, "foobar", table.GetName())
					assert.Equal(t, 1, table.RowCount())
					assert.NotNil(t, table)
				})
			})
		}
	})

	t.Run("mixed valid types", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			// Generate between 1 and 10 random fields with valid types
			numFields := rapid.IntRange(1, 10).Draw(t, "numFields")
			jsonData := make(map[string]interface{})
			jsonData["$table"] = "foobar"

			for i := range numFields {
				key := rapid.StringMatching(`[a-zA-Z][a-zA-Z0-9_]*`).Draw(t, fmt.Sprintf("key_%d", i))

				// Choose random valid type
				typeChoice := rapid.IntRange(0, 2).Draw(t, fmt.Sprintf("type_%d", i))
				switch typeChoice {
				case 0: // string
					jsonData[key] = rapid.String().Draw(t, fmt.Sprintf("str_val_%d", i))
				case 1: // number
					value := rapid.Float64Range(-1e10, 1e10).Draw(t, fmt.Sprintf("num_val_%d", i))
					if !math.IsNaN(value) && !math.IsInf(value, 0) {
						jsonData[key] = value
					} else {
						jsonData[key] = 42.0 // fallback for special values
					}
				case 2: // boolean
					jsonData[key] = rapid.Bool().Draw(t, fmt.Sprintf("bool_val_%d", i))
				}
			}

			jsonBytes, err := json.Marshal(jsonData)
			require.NoError(t, err)

			msg := &nats.Msg{
				Subject: util.RandomTopicName(),
				Data:    jsonBytes,
			}

			tables, err := parser.Parse(msg)
			require.NoError(t, err)
			require.Len(t, tables, 1)

			table := tables[0]
			assert.Equal(t, "foobar", table.GetName())
			assert.Equal(t, 1, table.RowCount())
			assert.NotNil(t, table)
		})
	})

	t.Run("special numeric values", func(t *testing.T) {
		specialValues := []interface{}{
			0, -42, 1000000, -1000000, 0.0, 0.00001, -3.14159,
			9007199254740991, // 2^53 - 1
		}

		for _, value := range specialValues {
			jsonData := map[string]interface{}{
				"$table": "foobar",
				"value":  value,
			}

			jsonBytes, err := json.Marshal(jsonData)
			require.NoError(t, err)

			msg := &nats.Msg{
				Subject: util.RandomTopicName(),
				Data:    jsonBytes,
			}

			tables, err := parser.Parse(msg)
			require.NoError(t, err)
			require.Len(t, tables, 1)

			table := tables[0]
			assert.Equal(t, "foobar", table.GetName())
			assert.Equal(t, 1, table.RowCount())
			assert.NotNil(t, table)
		}
	})

	t.Run("mixed null and valid values", func(t *testing.T) {
		jsonData := map[string]interface{}{
			"$table": "foobar",
			"field1": nil,
			"field2": "valid",
			"field3": nil,
			"field4": 42,
		}

		jsonBytes, err := json.Marshal(jsonData)
		require.NoError(t, err)

		msg := &nats.Msg{
			Subject: util.RandomTopicName(),
			Data:    jsonBytes,
		}

		tables, err := parser.Parse(msg)
		require.NoError(t, err)
		require.Len(t, tables, 1)

		table := tables[0]
		assert.Equal(t, "foobar", table.GetName())
		assert.Equal(t, 1, table.RowCount())
		assert.NotNil(t, table)
	})

	t.Run("consistent table names", func(t *testing.T) {
		validJsonStrings := []string{
			`{"$table": "foobar", "key": "value"}`,
			`{"$table": "foobar", "temperature": 23.5}`,
			`{"$table": "foobar", "active": true}`,
			`{"$table": "foobar", "name": "test", "count": 42, "enabled": false}`,
		}

		for i, jsonStr := range validJsonStrings {
			t.Run(fmt.Sprintf("test_%d", i), func(t *testing.T) {
				msg := &nats.Msg{
					Subject: util.RandomTopicName(),
					Data:    []byte(jsonStr),
				}

				tables, err := parser.Parse(msg)
				require.NoError(t, err)
				require.Len(t, tables, 1, "should produce exactly one table")

				table := tables[0]
				assert.Equal(t, "foobar", table.GetName(), "table name should always be 'foobar'")
				assert.Equal(t, 1, table.RowCount(), "should have exactly one row")
			})
		}
	})
}

// TestJsonParserInterfaceCompliance tests that the parser correctly implements the Parser interface
func TestJsonParserInterfaceCompliance(t *testing.T) {
	parser, err := NewJsonParser()
	require.NoError(t, err)
	require.NotNil(t, parser)

	// Test that parser implements Parser interface
	var _ Parser = parser

	// Test basic functionality
	validJson := `{"$table": "foobar", "key": "value"}`
	msg := &nats.Msg{
		Subject: util.RandomTopicName(),
		Data:    []byte(validJson),
	}

	tables, err := parser.Parse(msg)
	require.NoError(t, err)
	require.Len(t, tables, 1)

	table := tables[0]
	assert.Equal(t, "foobar", table.GetName())
	assert.Equal(t, 1, table.RowCount())
	assert.NotNil(t, table)
}
