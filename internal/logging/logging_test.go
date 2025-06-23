package logging

import (
	"log/slog"
	"os"
	"testing"
)

func TestSetup(t *testing.T) {
	// Test default setup
	logger := Setup()
	if logger == nil {
		t.Error("Setup should return a non-nil logger")
	}
}

func TestSetupDefault(t *testing.T) {
	// Test default setup with metadata
	SetupDefault("test-version", "test-instance")

	// Log a test message to verify it works
	slog.Info("test message", "test_key", "test_value")
}

func TestLogLevelFromEnv(t *testing.T) {
	tests := []struct {
		envValue string
		expected slog.Level
	}{
		{"DEBUG", slog.LevelDebug},
		{"debug", slog.LevelDebug},
		{"INFO", slog.LevelInfo},
		{"WARN", slog.LevelWarn},
		{"ERROR", slog.LevelError},
		{"invalid", slog.LevelInfo}, // default
		{"", slog.LevelInfo},        // default
	}

	for _, tt := range tests {
		t.Run(tt.envValue, func(t *testing.T) {
			oldValue := os.Getenv("LOG_LEVEL")
			defer os.Setenv("LOG_LEVEL", oldValue)

			os.Setenv("LOG_LEVEL", tt.envValue)
			level := getLogLevel()
			if level != tt.expected {
				t.Errorf("Expected log level %v, got %v", tt.expected, level)
			}
		})
	}
}

func TestLogFormatFromEnv(t *testing.T) {
	tests := []struct {
		envValue string
		expected string
	}{
		{"json", "json"},
		{"JSON", "json"},
		{"text", "text"},
		{"TEXT", "text"},
		{"invalid", "invalid"},
		{"", "json"}, // default
	}

	for _, tt := range tests {
		t.Run(tt.envValue, func(t *testing.T) {
			oldValue := os.Getenv("LOG_FORMAT")
			defer os.Setenv("LOG_FORMAT", oldValue)

			os.Setenv("LOG_FORMAT", tt.envValue)
			format := getLogFormat()
			if format != tt.expected {
				t.Errorf("Expected log format %v, got %v", tt.expected, format)
			}
		})
	}
}
