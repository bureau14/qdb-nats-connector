package parser

import (
	"io"
	"strings"
	"testing"

	"github.com/bureau14/qdb-nats-connector/tools/loader/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockParser for testing
type mockParser struct {
	format int
}

func (m *mockParser) Parse(reader io.Reader) (messages <-chan internal.Message, errors <-chan error) {
	messageChan := make(chan internal.Message, 1)
	errorChan := make(chan error)

	go func() {
		defer close(messageChan)
		defer close(errorChan)

		messageChan <- internal.Message{
			Data:   []byte("mock data"),
			Format: m.format,
		}
	}()

	return messageChan, errorChan
}

func TestParserRegistry_RegisterAndGet(t *testing.T) {
	registry := NewParserRegistry()

	// Test format that doesn't exist
	const testFormat = 999

	// Should return error for unregistered format
	_, err := registry.GetParser(testFormat)
	require.Error(t, err)

	// Register a mock parser
	registry.Register(testFormat, func() Parser {
		return &mockParser{format: testFormat}
	})

	// Should now return the parser
	parser, err := registry.GetParser(testFormat)
	require.NoError(t, err)
	require.NotNil(t, parser)

	// Test that it's actually our mock parser
	mockReader := strings.NewReader("test")
	messages, _ := parser.Parse(mockReader)
	msg := <-messages

	assert.Equal(t, []byte("mock data"), msg.Data)
	assert.Equal(t, testFormat, msg.Format)
}

func TestParserRegistry_SupportedFormats(t *testing.T) {
	registry := NewParserRegistry()

	// Initially empty
	formats := registry.SupportedFormats()
	assert.Empty(t, formats)

	// Add some formats
	registry.Register(100, func() Parser { return &mockParser{format: 100} })
	registry.Register(200, func() Parser { return &mockParser{format: 200} })

	formats = registry.SupportedFormats()
	assert.Len(t, formats, 2)
	assert.Contains(t, formats, 100)
	assert.Contains(t, formats, 200)
}

func TestDefaultRegistry_RegisteredParsers(t *testing.T) {
	// Test that default parsers are registered
	formats := GetSupportedFormats()
	assert.Contains(t, formats, internal.FormatJSONLines)
	assert.Contains(t, formats, internal.FormatBase64)

	// Test getting registered parsers
	jsonParser, err := GetRegisteredParser(internal.FormatJSONLines)
	require.NoError(t, err)
	assert.NotNil(t, jsonParser)

	base64Parser, err := GetRegisteredParser(internal.FormatBase64)
	require.NoError(t, err)
	assert.NotNil(t, base64Parser)

	// Test unregistered format
	_, err = GetRegisteredParser(999)
	require.Error(t, err)
}

func TestRegisterParser_PackageLevel(t *testing.T) {
	const testFormat = 888

	// Register at package level
	RegisterParser(testFormat, func() Parser {
		return &mockParser{format: testFormat}
	})

	// Should be able to get it back
	parser, err := GetRegisteredParser(testFormat)
	require.NoError(t, err)
	assert.NotNil(t, parser)

	// Clean up by creating a new registry
	defaultRegistry = NewParserRegistry()
	// Re-register defaults
	RegisterParser(internal.FormatJSONLines, func() Parser {
		return NewJSONLinesParser()
	})
	RegisterParser(internal.FormatBase64, func() Parser {
		return NewBase64Parser(internal.FormatJSONLines)
	})
}
