package parser

import (
	"testing"

	"github.com/bureau14/qdb-nats-connector/tools/loader/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetParser_Registry(t *testing.T) {
	// Test registered parsers via GetParser
	parser, err := GetParser(internal.FormatJSONLines)
	require.NoError(t, err)
	assert.NotNil(t, parser)

	parser, err = GetParser(internal.FormatBase64)
	require.NoError(t, err)
	assert.NotNil(t, parser)
}

func TestGetParser_Fallback(t *testing.T) {
	// Test fallback for unimplemented formats
	_, err := GetParser(internal.FormatGzipJSON)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Gzip format not yet implemented")

	_, err = GetParser(internal.FormatParquet)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Parquet format not yet implemented")

	// Test unknown format
	_, err = GetParser(999)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Unknown data format")
}

func TestGetParser_RegistryPriority(t *testing.T) {
	// Register a custom parser for GzipJSON to test registry priority
	const customFormat = internal.FormatGzipJSON
	RegisterParser(customFormat, func() Parser {
		return &mockParser{format: customFormat}
	})

	// Should now get the registry version instead of fallback error
	parser, err := GetParser(customFormat)
	require.NoError(t, err)
	assert.NotNil(t, parser)

	// Clean up
	defaultRegistry = NewParserRegistry()
	// Re-register defaults
	RegisterParser(internal.FormatJSONLines, func() Parser {
		return NewJSONLinesParser()
	})
	RegisterParser(internal.FormatBase64, func() Parser {
		return NewBase64Parser(internal.FormatJSONLines)
	})
}
