package detector

import (
	"testing"

	"github.com/bureau14/qdb-nats-connector/tools/loader/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDetectorRegistry_RegisterAndDetect(t *testing.T) {
	registry := NewDetectorRegistry()

	// Test detection with no detectors
	_, err := registry.Detect([]byte("test"))
	require.Error(t, err)

	// Register a detector for a custom format
	const customFormat = 999
	registry.Register(10, func(header []byte) (int, bool) {
		if string(header) == "CUSTOM" {
			return customFormat, true
		}

		return 0, false
	})

	// Should detect our custom format
	format, err := registry.Detect([]byte("CUSTOM"))
	require.NoError(t, err)
	assert.Equal(t, customFormat, format)

	// Should not detect other data
	_, err = registry.Detect([]byte("OTHER"))
	require.Error(t, err)
}

func TestDetectorRegistry_Priority(t *testing.T) {
	registry := NewDetectorRegistry()

	// Register detectors with different priorities
	registry.Register(20, func(header []byte) (int, bool) {
		if string(header) == "TEST" {
			return 200, true // Lower priority
		}

		return 0, false
	})

	registry.Register(10, func(header []byte) (int, bool) {
		if string(header) == "TEST" {
			return 100, true // Higher priority
		}

		return 0, false
	})

	// Should return the higher priority detector result
	format, err := registry.Detect([]byte("TEST"))
	require.NoError(t, err)
	assert.Equal(t, 100, format) // Higher priority wins
}

func TestDetectorRegistry_DefaultDetectors(t *testing.T) {
	// Test default detectors
	tests := []struct {
		name     string
		data     []byte
		expected int
	}{
		{
			name:     "parquet_magic",
			data:     []byte("PAR1test"),
			expected: internal.FormatParquet,
		},
		{
			name:     "gzip_magic",
			data:     []byte{0x1f, 0x8b, 0x08, 0x00},
			expected: internal.FormatGzipJSON,
		},
		{
			name:     "json_object",
			data:     []byte(`{"key": "value"}`),
			expected: internal.FormatJSONLines,
		},
		{
			name:     "json_array",
			data:     []byte(`[1, 2, 3]`),
			expected: internal.FormatJSONLines,
		},
		{
			name:     "base64_data",
			data:     []byte("aGVsbG8gd29ybGQ="),
			expected: internal.FormatBase64,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			format, err := DetectFormat(tt.data)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, format)
		})
	}
}

func TestDetectorRegistry_NoMatch(t *testing.T) {
	// Test with data that doesn't match any detector
	_, err := DetectFormat([]byte("some random text that doesn't match"))
	require.Error(t, err)
}

func TestRegisterDetector_PackageLevel(t *testing.T) {
	const testFormat = 777

	// Register at package level
	RegisterDetector(5, func(header []byte) (int, bool) {
		if string(header) == "PACKAGE_TEST" {
			return testFormat, true
		}

		return 0, false
	})

	// Should be able to detect it
	format, err := DetectFormat([]byte("PACKAGE_TEST"))
	require.NoError(t, err)
	assert.Equal(t, testFormat, format)

	// Clean up by creating a new registry for subsequent tests
	// This would normally not be done in production code
	defaultDetectorRegistry = NewDetectorRegistry()
	// Re-register defaults would happen in init(), but we're testing
}
