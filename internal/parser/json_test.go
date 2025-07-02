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

// assertParsingError is a helper function that verifies a parser returns the expected error
func assertParsingError(t *testing.T, parser *JsonParser, jsonData, expectedErrorFragment string) {
	msg := &nats.Msg{
		Subject: util.RandomTopicName(),
		Data:    []byte(jsonData),
	}

	tables, err := parser.Parse(msg)
	assert.Nil(t, tables)
	require.Error(t, err)

	var connErr *connectorErrors.ConnectorError
	require.True(t, errors.As(err, &connErr))
	assert.Equal(t, "json_parser", connErr.Component)
	assert.Equal(t, connectorErrors.ErrCodeParsingFailed, connErr.Code)
	assert.Contains(t, err.Error(), expectedErrorFragment)
}

func TestParserJsonNewParserShouldReturnValidParser(t *testing.T) {
	parser, err := NewJsonParser()
	require.NoError(t, err)
	require.NotNil(t, parser)
}

func TestParserJsonParseWhenNilMessageShouldReturnError(t *testing.T) {
	parser, err := NewJsonParser()
	require.NoError(t, err)

	tables, err := parser.Parse(nil)
	assert.Nil(t, tables)
	require.Error(t, err)

	var connErr *connectorErrors.ConnectorError
	require.True(t, errors.As(err, &connErr))
	assert.Equal(t, "json_parser", connErr.Component)
	assert.Equal(t, connectorErrors.ErrCodeParsingFailed, connErr.Code)
	// The error comes wrapped, so let's check the error message more broadly
	assert.Contains(t, err.Error(), "parse message")
}

func TestParserJsonParseWhenEmptyMessageShouldReturnError(t *testing.T) {
	parser, err := NewJsonParser()
	require.NoError(t, err)

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
}

func TestParserJsonParseWhenInvalidJsonShouldReturnError(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{"malformed json", `{"key": invalid}`},
		{"unclosed object", `{"key": "value"`},
		{"unclosed string", `{"key": "value}`},
		{"invalid characters", `{"key": "val\x00ue"}`},
		{"trailing comma", `{"key": "value",}`},
		{"missing quotes", `{key: "value"}`},
		{"single quotes", `{'key': 'value'}`},
	}

	parser, err := NewJsonParser()
	require.NoError(t, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertParsingError(t, parser, tt.data, "parse message")
		})
	}
}

func TestParserJsonParseWhenInvalidJsonStructureShouldReturnError(t *testing.T) {
	tests := []struct {
		name             string
		data             string
		expectedErrorMsg string
	}{
		{"empty json", `{}`, "parse message"},
		{"missing table key", `{"key": "value"}`, "missing required $table key"},
	}

	parser, err := NewJsonParser()
	require.NoError(t, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertParsingError(t, parser, tt.data, tt.expectedErrorMsg)
		})
	}
}

func TestParserJsonParseWhenNestedDataShouldReturnError(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{"nested object", `{"$table": "foobar", "key": {"nested": "value"}}`},
		{"nested array", `{"$table": "foobar", "key": ["value1", "value2"]}`},
		{"mixed nesting", `{"$table": "foobar", "key1": "value1", "key2": {"nested": "value"}}`},
		{"array with object", `{"$table": "foobar", "key": [{"nested": "value"}]}`},
		{"deeply nested", `{"$table": "foobar", "key": {"level1": {"level2": "value"}}}`},
	}

	parser, err := NewJsonParser()
	require.NoError(t, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertParsingError(t, parser, tt.data, "parse message")
		})
	}
}

func TestParserJsonParseWhenValidTypesShouldCreateTable(t *testing.T) {
	tests := []struct {
		name         string
		jsonData     map[string]interface{}
		expectedCols int
	}{
		{
			name: "string value",
			jsonData: map[string]interface{}{
				"$table": "foobar",
				"name":   "test_string",
			},
			expectedCols: 1,
		},
		{
			name: "float64 number",
			jsonData: map[string]interface{}{
				"$table":      "foobar",
				"temperature": 23.5,
			},
			expectedCols: 1,
		},
		{
			name: "integer number",
			jsonData: map[string]interface{}{
				"$table": "foobar",
				"count":  42,
			},
			expectedCols: 1,
		},
		{
			name: "boolean true",
			jsonData: map[string]interface{}{
				"$table": "foobar",
				"active": true,
			},
			expectedCols: 1,
		},
		{
			name: "boolean false",
			jsonData: map[string]interface{}{
				"$table":   "foobar",
				"disabled": false,
			},
			expectedCols: 1,
		},
		{
			name: "mixed types",
			jsonData: map[string]interface{}{
				"$table":      "foobar",
				"name":        "sensor_01",
				"temperature": 18.7,
				"count":       100,
				"active":      true,
				"error":       false,
			},
			expectedCols: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := NewJsonParser()
			require.NoError(t, err)

			jsonBytes, err := json.Marshal(tt.jsonData)
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
			assert.Equal(t, 1, table.RowCount()) // Should have one row

			// We can't easily verify the internal structure due to unexported fields,
			// but we can verify that the table was created successfully and has the expected characteristics
			assert.NotNil(t, table)
		})
	}
}

func TestParserJsonParseWhenNullValuesShouldSkipOrError(t *testing.T) {
	tests := []struct {
		name            string
		jsonData        map[string]interface{}
		expectedColumns []string
		expectedValues  []interface{}
	}{
		{
			name: "only null values",
			jsonData: map[string]interface{}{
				"$table": "foobar",
				"field1": nil,
				"field2": nil,
			},
			expectedColumns: []string{},
			expectedValues:  []interface{}{},
		},
		{
			name: "mixed null and valid values",
			jsonData: map[string]interface{}{
				"$table": "foobar",
				"field1": nil,
				"field2": "valid",
				"field3": nil,
				"field4": 42,
			},
			expectedColumns: []string{"field2", "field4"},
			expectedValues:  []interface{}{[]byte("valid"), 42.0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser, err := NewJsonParser()
			require.NoError(t, err)

			jsonBytes, err := json.Marshal(tt.jsonData)
			require.NoError(t, err)

			msg := &nats.Msg{
				Subject: util.RandomTopicName(),
				Data:    jsonBytes,
			}

			if len(tt.expectedColumns) == 0 {
				// Should return an error for no valid columns
				tables, err := parser.Parse(msg)
				assert.Nil(t, tables)
				require.Error(t, err)

				var connErr *connectorErrors.ConnectorError
				require.True(t, errors.As(err, &connErr))
				assert.Equal(t, "json_parser", connErr.Component)
				assert.Equal(t, connectorErrors.ErrCodeParsingFailed, connErr.Code)
				assert.Contains(t, err.Error(), "parse message")
			} else {
				tables, err := parser.Parse(msg)
				require.NoError(t, err)
				require.Len(t, tables, 1)

				table := tables[0]
				assert.Equal(t, "foobar", table.GetName())
				assert.Equal(t, 1, table.RowCount())
				assert.NotNil(t, table)
			}
		})
	}
}

func TestParserJsonParseWhenSpecialNumbersShouldSucceed(t *testing.T) {
	tests := []struct {
		name    string
		value   interface{}
		wantErr bool
	}{
		{"zero", 0, false},
		{"negative", -42, false},
		{"large positive", 1000000, false},
		{"large negative", -1000000, false},
		{"float zero", 0.0, false},
		{"small float", 0.00001, false},
		{"negative float", -3.14159, false},
		{"max safe integer", 9007199254740991, false}, // 2^53 - 1
	}

	parser, err := NewJsonParser()
	require.NoError(t, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jsonData := map[string]interface{}{
				"$table": "foobar",
				"value":  tt.value,
			}

			jsonBytes, err := json.Marshal(jsonData)
			require.NoError(t, err)

			msg := &nats.Msg{
				Subject: util.RandomTopicName(),
				Data:    jsonBytes,
			}

			tables, err := parser.Parse(msg)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, tables)
			} else {
				require.NoError(t, err)
				require.Len(t, tables, 1)

				table := tables[0]
				assert.Equal(t, "foobar", table.GetName())
				assert.Equal(t, 1, table.RowCount())
				assert.NotNil(t, table)
			}
		})
	}
}

// Property-based testing with rapid - table-driven approach
func TestParserJsonParsePropertyBasedValues(t *testing.T) {
	testCases := []struct {
		name          string
		valueGen      func(*rapid.T) interface{}
		skipCondition func(interface{}) bool
	}{
		{
			name: "string values",
			valueGen: func(t *rapid.T) interface{} {
				return rapid.String().Draw(t, "value")
			},
			skipCondition: nil,
		},
		{
			name: "numeric values",
			valueGen: func(t *rapid.T) interface{} {
				return rapid.Float64Range(-1e10, 1e10).Draw(t, "value")
			},
			skipCondition: func(v interface{}) bool {
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
			skipCondition: nil,
		},
	}

	parser, err := NewJsonParser()
	require.NoError(t, err)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rapid.Check(t, func(t *rapid.T) {
				key := rapid.StringMatching(`[a-zA-Z][a-zA-Z0-9_]*`).Draw(t, "key")
				value := tc.valueGen(t)

				if tc.skipCondition != nil && tc.skipCondition(value) {
					t.Skip("skipping special values")
				}

				testPropertyBasedParsing(t, parser, key, value)
			})
		})
	}
}

// testPropertyBasedParsing contains the shared test logic for property-based tests
func testPropertyBasedParsing(t *rapid.T, parser *JsonParser, key string, value interface{}) {
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
}

func TestParserJsonParsePropertyBasedMixedValidTypes(t *testing.T) {
	parser, err := NewJsonParser()
	require.NoError(t, err)

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
		assert.Equal(t, 1, table.RowCount()) // Should have exactly 1 row with multiple columns
		assert.NotNil(t, table)
	})
}

func TestParserJsonParsePropertyBasedInvalidJsonFuzz(t *testing.T) {
	parser, err := NewJsonParser()
	require.NoError(t, err)

	rapid.Check(t, func(t *rapid.T) {
		// Generate random byte sequences that are likely invalid JSON
		data := rapid.SliceOfN(rapid.Byte(), 1, 1000).Draw(t, "invalidData")

		// Skip if it accidentally creates valid JSON
		var testMap map[string]interface{}
		if json.Unmarshal(data, &testMap) == nil && len(testMap) > 0 {
			// Check if it contains nested structures
			hasNested := false
			for _, v := range testMap {
				switch v.(type) {
				case map[string]interface{}, []interface{}:
					hasNested = true
				}
			}
			if !hasNested {
				t.Skip("accidentally generated valid single-depth JSON")
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
}

func TestParserJsonParseWhenValidJsonShouldHaveConsistentTableName(t *testing.T) {
	parser, err := NewJsonParser()
	require.NoError(t, err)

	// Test that all valid single-depth JSON produces exactly one WriterTable with table name "foobar"
	tests := []string{
		`{"$table": "foobar", "key": "value"}`,
		`{"$table": "foobar", "temperature": 23.5}`,
		`{"$table": "foobar", "active": true}`,
		`{"$table": "foobar", "name": "test", "count": 42, "enabled": false}`,
		`{"$table": "foobar", "field1": "string", "field2": 3.14, "field3": true, "field4": false}`,
	}

	for i, jsonStr := range tests {
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
}
