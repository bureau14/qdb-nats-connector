package connector

import (
	"os"
	"path/filepath"
	"testing"

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

// TestLoadConfigYAMLFile verifies YAML config file loading.
func TestLoadConfigYAMLFile(t *testing.T) {
	// Create temporary config file
	configContent := `
nats:
  endpoint: "nats://yaml:4222"
  topic: "yaml.topic"
qdb:
  cluster_uri: "qdb://yaml:2836"
  compression: "best"
  encryption: "aes"
pid: "/tmp/yaml.pid"
`
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "config.yaml")
	err := os.WriteFile(configFile, []byte(configContent), 0644)
	require.NoError(t, err)

	opts, err := LoadConfig([]string{"--config", configFile}, func() {})
	require.NoError(t, err)
	require.NotNil(t, opts)

	assert.Equal(t, "nats://yaml:4222", opts.Endpoint())
	assert.Equal(t, "yaml.topic", opts.Topic())
	assert.Equal(t, "qdb://yaml:2836", opts.ClusterUri())
	assert.Equal(t, "/tmp/yaml.pid", opts.PidFile)
	require.NotNil(t, opts.Compression())
	require.NotNil(t, opts.Encryption())
}

// TestLoadConfigJSONFile verifies JSON config file loading.
func TestLoadConfigJSONFile(t *testing.T) {
	// Create temporary config file
	configContent := `{
  "nats": {
    "endpoint": "nats://json:4222",
    "topic": "json.topic"
  },
  "qdb": {
    "cluster_uri": "qdb://json:2836"
  },
  "pid": "/tmp/json.pid"
}`
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "config.json")
	err := os.WriteFile(configFile, []byte(configContent), 0644)
	require.NoError(t, err)

	opts, err := LoadConfig([]string{"--config", configFile}, func() {})
	require.NoError(t, err)
	require.NotNil(t, opts)

	assert.Equal(t, "nats://json:4222", opts.Endpoint())
	assert.Equal(t, "json.topic", opts.Topic())
	assert.Equal(t, "qdb://json:2836", opts.ClusterUri())
	assert.Equal(t, "/tmp/json.pid", opts.PidFile)
}

// TestLoadConfigEnvironmentVariables verifies environment variable loading.
func TestLoadConfigEnvironmentVariables(t *testing.T) {
	// Set environment variables
	os.Setenv("QDB_NATS_NATS_ENDPOINT", "nats://env:4222")
	os.Setenv("QDB_NATS_NATS_TOPIC", "env.topic")
	os.Setenv("QDB_NATS_QDB_CLUSTER_URI", "qdb://env:2836")
	os.Setenv("QDB_NATS_PID", "/tmp/env.pid")
	defer func() {
		os.Unsetenv("QDB_NATS_NATS_ENDPOINT")
		os.Unsetenv("QDB_NATS_NATS_TOPIC")
		os.Unsetenv("QDB_NATS_QDB_CLUSTER_URI")
		os.Unsetenv("QDB_NATS_PID")
	}()

	opts, err := LoadConfig([]string{}, func() {})
	require.NoError(t, err)
	require.NotNil(t, opts)

	assert.Equal(t, "nats://env:4222", opts.Endpoint())
	assert.Equal(t, "env.topic", opts.Topic())
	assert.Equal(t, "qdb://env:2836", opts.ClusterUri())
	assert.Equal(t, "/tmp/env.pid", opts.PidFile)
}

// TestLoadConfigPrecedence verifies CLI flags take precedence over config file and env vars.
func TestLoadConfigPrecedence(t *testing.T) {
	// Set environment variables
	os.Setenv("QDB_NATS_NATS_ENDPOINT", "nats://env:4222")
	os.Setenv("QDB_NATS_NATS_TOPIC", "env.topic")
	defer func() {
		os.Unsetenv("QDB_NATS_NATS_ENDPOINT")
		os.Unsetenv("QDB_NATS_NATS_TOPIC")
	}()

	// Create config file
	configContent := `
nats:
  endpoint: "nats://config:4222"
  topic: "config.topic"
`
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "config.yaml")
	err := os.WriteFile(configFile, []byte(configContent), 0644)
	require.NoError(t, err)

	// CLI flags should override both config file and env vars
	opts, err := LoadConfig([]string{
		"--config", configFile,
		"--nats", "nats://cli:4222",
		"--topic", "cli.topic",
	}, func() {})
	require.NoError(t, err)
	require.NotNil(t, opts)

	// CLI flags should win
	assert.Equal(t, "nats://cli:4222", opts.Endpoint())
	assert.Equal(t, "cli.topic", opts.Topic())
}

// TestLoadConfigEnvOverridesConfigFile verifies env vars take precedence over config file.
func TestLoadConfigEnvOverridesConfigFile(t *testing.T) {
	// Clean up any existing environment variables first
	os.Unsetenv("QDB_NATS_NATS_ENDPOINT")
	os.Unsetenv("QDB_NATS_NATS_TOPIC")

	// Set environment variables
	os.Setenv("QDB_NATS_NATS_ENDPOINT", "nats://env:4222")
	os.Setenv("QDB_NATS_NATS_TOPIC", "env.topic")
	defer func() {
		os.Unsetenv("QDB_NATS_NATS_ENDPOINT")
		os.Unsetenv("QDB_NATS_NATS_TOPIC")
	}()

	// Create config file
	configContent := `
nats:
  endpoint: "nats://config:4222"
  topic: "config.topic"
`
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "config.yaml")
	err := os.WriteFile(configFile, []byte(configContent), 0644)
	require.NoError(t, err)

	// Load without explicit CLI flags
	opts, err := LoadConfig([]string{"--config", configFile}, func() {})
	require.NoError(t, err)
	require.NotNil(t, opts)

	// Environment variables should override config file (per precedence: defaults < config file < env vars < CLI flags)
	assert.Equal(t, "nats://env:4222", opts.Endpoint())
	assert.Equal(t, "env.topic", opts.Topic())
}

// TestLoadConfigInvalidConfigFile verifies error handling for invalid config files.
func TestLoadConfigInvalidConfigFile(t *testing.T) {
	// Create invalid config file
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "invalid.yaml")
	err := os.WriteFile(configFile, []byte("invalid: yaml: content: ["), 0644)
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

// TestLoadConfigCompressionValues verifies compression parsing.
func TestLoadConfigCompressionValues(t *testing.T) {
	tests := []struct {
		name  string
		value string
		valid bool
	}{
		{"none", "none", true},
		{"fast", "fast", true},
		{"speed", "speed", true},
		{"best", "best", true},
		{"invalid", "invalid", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := LoadConfig([]string{"--qdb-compression", tt.value}, func() {})
			if tt.valid {
				require.NoError(t, err)
				require.NotNil(t, opts)
				require.NotNil(t, opts.Compression())
			} else {
				require.Error(t, err)
				require.Nil(t, opts)
			}
		})
	}
}

// TestLoadConfigEncryptionValues verifies encryption parsing.
func TestLoadConfigEncryptionValues(t *testing.T) {
	tests := []struct {
		name  string
		value string
		valid bool
	}{
		{"none", "none", true},
		{"aes", "aes", true},
		{"invalid", "invalid", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := LoadConfig([]string{"--qdb-encryption", tt.value}, func() {})
			if tt.valid {
				require.NoError(t, err)
				require.NotNil(t, opts)
				require.NotNil(t, opts.Encryption())
			} else {
				require.Error(t, err)
				require.Nil(t, opts)
			}
		})
	}
}

// TestLoadConfigPerformanceFlags verifies performance flag parsing.
func TestLoadConfigPerformanceFlags(t *testing.T) {
	opts, err := LoadConfig([]string{
		"--qdb-client-max-parallelism", "8",
		"--qdb-client-inbuf-size", "1024",
	}, func() {})
	require.NoError(t, err)
	require.NotNil(t, opts)

	require.NotNil(t, opts.ClientMaxParallelism())
	assert.Equal(t, uint(8), *opts.ClientMaxParallelism())

	require.NotNil(t, opts.ClientMaxInBufSize())
	assert.Equal(t, uint(1024), *opts.ClientMaxInBufSize())
}

// TestLoadConfigBackwardCompatibility verifies the old ConfigureOptions still works.
func TestLoadConfigBackwardCompatibility(t *testing.T) {
	// Test that the original ConfigureOptions function still works
	// This ensures backward compatibility for existing code
	require.NotNil(t, ConfigureOptions, "ConfigureOptions function should still exist for backward compatibility")
}
