package parser

import (
	"testing"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParserInterface ensures that different parser implementations
// can be used interchangeably through the Parser interface.
func TestParserInterface(t *testing.T) {
	tests := []struct {
		name        string
		parser      Parser
		msg         *nats.Msg
		wantErr     bool
		errContains string
	}{
		{
			name:   "NoopParser with valid message",
			parser: &NoopParser{},
			msg: &nats.Msg{
				Subject: "test.topic",
				Data:    []byte("test data"),
			},
			wantErr: false,
		},
		{
			name:        "NoopParser with nil message",
			parser:      &NoopParser{},
			msg:         nil,
			wantErr:     true,
			errContains: "failed to parse message",
		},
		{
			name:   "NoopParser with empty data",
			parser: &NoopParser{},
			msg: &nats.Msg{
				Subject: "test.topic",
				Data:    []byte{},
			},
			wantErr:     true,
			errContains: "failed to parse message",
		},
		{
			name: "JsonParser through interface",
			parser: func() Parser {
				jp, _ := NewJsonParser()
				return jp
			}(),
			msg: &nats.Msg{
				Subject: "test.topic",
				Data:    []byte(`{"key": "value"}`),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test through interface
			var p Parser = tt.parser
			tables, err := p.Parse(tt.msg)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				require.NoError(t, err)
				require.NotNil(t, tables)
			}
		})
	}
}

// TestParserPolymorphism demonstrates that parsers can be swapped at runtime.
func TestParserPolymorphism(t *testing.T) {
	msg := &nats.Msg{
		Subject: "test.topic",
		Data:    []byte(`{"temperature": 23.5}`),
	}

	// Create different parser implementations
	noopParser, err := NewNoopParser()
	require.NoError(t, err)

	jsonParser, err := NewJsonParser()
	require.NoError(t, err)

	// Function that accepts any Parser implementation
	processMessage := func(p Parser, msg *nats.Msg) ([]qdb.WriterTable, error) {
		return p.Parse(msg)
	}

	// Test with noop parser
	tables, err := processMessage(noopParser, msg)
	require.NoError(t, err)
	assert.Empty(t, tables) // NoopParser returns empty result

	// Test with JSON parser
	tables, err = processMessage(jsonParser, msg)
	require.NoError(t, err)
	assert.Len(t, tables, 1) // JsonParser creates one table
}

// mockParser is a test parser that returns predefined results.
type mockParser struct {
	returnTables []qdb.WriterTable
	returnError  error
}

func (m *mockParser) Parse(msg *nats.Msg) ([]qdb.WriterTable, error) {
	if m.returnError != nil {
		return nil, m.returnError
	}
	return m.returnTables, nil
}

// TestCustomParserImplementation shows how custom parsers can implement the interface.
func TestCustomParserImplementation(t *testing.T) {
	// Create a mock parser with predefined behavior
	columns := []qdb.WriterColumn{
		{
			ColumnName: "test_col",
			ColumnType: qdb.TsColumnTypes[0], // blob type
		},
	}
	table, err := qdb.NewWriterTable("test_table", columns)
	require.NoError(t, err)

	mock := &mockParser{
		returnTables: []qdb.WriterTable{table},
		returnError:  nil,
	}

	// Verify it implements Parser interface
	var _ Parser = mock

	// Use it through the interface
	var p Parser = mock
	tables, err := p.Parse(&nats.Msg{Data: []byte("test")})

	require.NoError(t, err)
	require.Len(t, tables, 1)
	// WriterTable fields are mostly unexported, so we just verify we got a table back
	assert.Equal(t, "test_table", tables[0].TableName)
}
