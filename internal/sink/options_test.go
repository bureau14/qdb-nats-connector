package sink

import (
	"testing"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/stretchr/testify/assert"
)

// TestNewOptions verifies default configuration values are set correctly.
func TestNewOptionsDefaults(t *testing.T) {
	opts := NewOptions()

	assert.Equal(t, qdb.WriterPushModeAsync, opts.PushMode)
	assert.Equal(t, qdb.CompFast, opts.Compression)
	assert.Equal(t, 4, opts.NumWriters)
	assert.Equal(t, 100, opts.QueueSize)
	assert.Equal(t, 10, opts.RetryAttempts)
}

// TestWithPushMode verifies WithPushMode option function works correctly.
func TestWithPushMode(t *testing.T) {
	tests := []struct {
		name     string
		mode     qdb.WriterPushMode
		expected qdb.WriterPushMode
	}{
		{
			name:     "transactional mode",
			mode:     qdb.WriterPushModeTransactional,
			expected: qdb.WriterPushModeTransactional,
		},
		{
			name:     "async mode",
			mode:     qdb.WriterPushModeAsync,
			expected: qdb.WriterPushModeAsync,
		},
		{
			name:     "fast mode",
			mode:     qdb.WriterPushModeFast,
			expected: qdb.WriterPushModeFast,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := NewOptions(WithPushMode(tt.mode))
			assert.Equal(t, tt.expected, opts.PushMode)
		})
	}
}

// TestNewOptionsWithMultipleOptions verifies functional options can be combined.
func TestNewOptionsWithMultipleOptions(t *testing.T) {
	opts := NewOptions(
		WithPushMode(qdb.WriterPushModeFast),
		WithCompression(qdb.CompNone),
		WithNumWriters(8),
		WithQueueSize(200),
	)

	assert.Equal(t, qdb.WriterPushModeFast, opts.PushMode)
	assert.Equal(t, qdb.CompNone, opts.Compression)
	assert.Equal(t, 8, opts.NumWriters)
	assert.Equal(t, 200, opts.QueueSize)
	assert.Equal(t, 10, opts.RetryAttempts) // Should keep default
}

// TestWithPushModeOverrideOrder verifies last option wins when multiple WithPushMode calls.
func TestWithPushModeOverrideOrder(t *testing.T) {
	opts := NewOptions(
		WithPushMode(qdb.WriterPushModeTransactional),
		WithPushMode(qdb.WriterPushModeAsync),
		WithPushMode(qdb.WriterPushModeFast),
	)

	assert.Equal(t, qdb.WriterPushModeFast, opts.PushMode)
}
