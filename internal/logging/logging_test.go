package logging

import (
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoggingLogLevelFromEnv(t *testing.T) {
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
			assert.Equal(t, tt.expected, level)
		})
	}
}

func TestLoggingLogFormatFromEnv(t *testing.T) {
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
			assert.Equal(t, tt.expected, format)
		})
	}
}
