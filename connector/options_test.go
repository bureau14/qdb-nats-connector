package connector

import (
	"os"
	"path/filepath"
	"testing"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLoadConfigDefaults verifies default configuration values are set correctly.
func TestLoadConfigDefaults(t *testing.T) {
	opts, err := LoadConfig([]string{}, func() {})
	require.NoError(t, err)
	require.NotNil(t, opts)

	assert.Equal(t, nats.DefaultURL, opts.Endpoint())
	assert.Equal(t, "", opts.Topic()) // No default topic
	assert.Equal(t, "", opts.PidFile)
}

// TestLoadConfigCLIFlags verifies CLI flag parsing works correctly.
func TestLoadConfigCLIFlags(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		expected func(*testing.T, *Options)
	}{
		{
			name: "nats endpoint flag",
			args: []string{"--nats", "nats://custom:4222"},
			expected: func(t *testing.T, opts *Options) {
				assert.Equal(t, "nats://custom:4222", opts.Endpoint())
			},
		},
		{
			name: "topic flag",
			args: []string{"--topic", "sensors.data"},
			expected: func(t *testing.T, opts *Options) {
				assert.Equal(t, "sensors.data", opts.Topic())
			},
		},
		{
			name: "pid file flag",
			args: []string{"--pid", "/tmp/test.pid"},
			expected: func(t *testing.T, opts *Options) {
				assert.Equal(t, "/tmp/test.pid", opts.PidFile)
			},
		},
		{
			name: "qdb cluster uri flag",
			args: []string{"--qdb", "qdb://custom:2836"},
			expected: func(t *testing.T, opts *Options) {
				assert.Equal(t, "qdb://custom:2836", opts.ClusterUri())
			},
		},
		{
			name: "multiple flags",
			args: []string{"--nats", "nats://test:4222", "--topic", "test.topic", "--qdb", "qdb://test:2836"},
			expected: func(t *testing.T, opts *Options) {
				assert.Equal(t, "nats://test:4222", opts.Endpoint())
				assert.Equal(t, "test.topic", opts.Topic())
				assert.Equal(t, "qdb://test:2836", opts.ClusterUri())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := LoadConfig(tt.args, func() {})
			require.NoError(t, err)
			require.NotNil(t, opts)
			tt.expected(t, opts)
		})
	}
}

// TestLoadConfigShortFlags verifies short CLI flag parsing works correctly.
func TestLoadConfigShortFlags(t *testing.T) {
	opts, err := LoadConfig([]string{"-n", "nats://short:4222", "-t", "short.topic"}, func() {})
	require.NoError(t, err)
	require.NotNil(t, opts)

	assert.Equal(t, "nats://short:4222", opts.Endpoint())
	assert.Equal(t, "short.topic", opts.Topic())
}

// TestLoadConfigHelp verifies help flag returns nil options.
func TestLoadConfigHelp(t *testing.T) {
	helpCalled := false
	printHelp := func() { helpCalled = true }

	opts, err := LoadConfig([]string{"--help"}, printHelp)
	require.NoError(t, err)
	require.Nil(t, opts)
	assert.True(t, helpCalled)
}

// TestLoadConfigHelpShort verifies short help flag returns nil options.
func TestLoadConfigHelpShort(t *testing.T) {
	helpCalled := false
	printHelp := func() { helpCalled = true }

	opts, err := LoadConfig([]string{"-h"}, printHelp)
	require.NoError(t, err)
	require.Nil(t, opts)
	assert.True(t, helpCalled)
}

// TestLoadConfigInvalidConfigFile verifies error handling for invalid config files.
func TestLoadConfigInvalidConfigFile(t *testing.T) {
	// Create invalid config file
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "invalid.yaml")
	err := os.WriteFile(configFile, []byte("invalid: yaml: content: ["), 0o600)
	require.NoError(t, err)

	opts, err := LoadConfig([]string{"--config", configFile}, func() {})
	require.Error(t, err)
	require.Nil(t, opts)
	assert.Contains(t, err.Error(), "error reading config file")
}

// TestLoadConfigNonExistentConfigFile verifies error handling for non-existent config files.
func TestLoadConfigNonExistentConfigFile(t *testing.T) {
	opts, err := LoadConfig([]string{"--config", "/nonexistent/config.yaml"}, func() {})
	require.Error(t, err)
	require.Nil(t, opts)
	assert.Contains(t, err.Error(), "error reading config file")
}

// TestParsePushMode verifies parsePushMode function works correctly.
func TestParsePushMode(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected qdb.WriterPushMode
		wantErr  bool
	}{
		{
			name:     "transactional mode",
			input:    "transactional",
			expected: qdb.WriterPushModeTransactional,
			wantErr:  false,
		},
		{
			name:     "async mode",
			input:    "async",
			expected: qdb.WriterPushModeAsync,
			wantErr:  false,
		},
		{
			name:     "fast mode",
			input:    "fast",
			expected: qdb.WriterPushModeFast,
			wantErr:  false,
		},
		{
			name:     "invalid mode",
			input:    "invalid",
			expected: qdb.WriterPushModeAsync,
			wantErr:  true,
		},
		{
			name:     "empty string",
			input:    "",
			expected: qdb.WriterPushModeAsync,
			wantErr:  true,
		},
		{
			name:     "case sensitive",
			input:    "TRANSACTIONAL",
			expected: qdb.WriterPushModeAsync,
			wantErr:  true,
		},
		{
			name:     "with spaces",
			input:    " transactional ",
			expected: qdb.WriterPushModeAsync,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parsePushMode(tt.input)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "invalid push mode value")
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// TestPushModeFlagString verifies pushModeFlag String method.
func TestPushModeFlagString(t *testing.T) {
	tests := []struct {
		name     string
		mode     qdb.WriterPushMode
		expected string
	}{
		{
			name:     "transactional mode",
			mode:     qdb.WriterPushModeTransactional,
			expected: "transactional",
		},
		{
			name:     "async mode",
			mode:     qdb.WriterPushModeAsync,
			expected: "async",
		},
		{
			name:     "fast mode",
			mode:     qdb.WriterPushModeFast,
			expected: "fast",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flag := &pushModeFlag{dst: &tt.mode}
			result := flag.String()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestPushModeFlagStringNilCases verifies pushModeFlag String method with nil values.
func TestPushModeFlagStringNilCases(t *testing.T) {
	tests := []struct {
		name string
		flag *pushModeFlag
	}{
		{
			name: "nil flag",
			flag: nil,
		},
		{
			name: "nil dst",
			flag: &pushModeFlag{dst: nil},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.flag.String()
			assert.Equal(t, "", result)
		})
	}
}

// TestPushModeFlagSet verifies pushModeFlag Set method.
func TestPushModeFlagSet(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected qdb.WriterPushMode
		wantErr  bool
	}{
		{
			name:     "transactional mode",
			input:    "transactional",
			expected: qdb.WriterPushModeTransactional,
			wantErr:  false,
		},
		{
			name:     "async mode",
			input:    "async",
			expected: qdb.WriterPushModeAsync,
			wantErr:  false,
		},
		{
			name:     "fast mode",
			input:    "fast",
			expected: qdb.WriterPushModeFast,
			wantErr:  false,
		},
		{
			name:     "invalid mode",
			input:    "invalid",
			expected: qdb.WriterPushModeAsync, // default fallback
			wantErr:  true,
		},
		{
			name:     "empty string",
			input:    "",
			expected: qdb.WriterPushModeAsync, // default fallback
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mode qdb.WriterPushMode
			flag := &pushModeFlag{dst: &mode}

			err := flag.Set(tt.input)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "invalid push mode value")
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, mode)
			}
		})
	}
}

// TestPushModeFlagSetNilDst verifies pushModeFlag Set method with nil dst.
func TestPushModeFlagSetNilDst(t *testing.T) {
	flag := &pushModeFlag{dst: nil}

	// This should panic when trying to dereference nil pointer
	assert.Panics(t, func() {
		_ = flag.Set("transactional")
	})
}
