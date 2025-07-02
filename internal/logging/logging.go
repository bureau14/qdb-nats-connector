// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package logging: logger setup & configuration
// Types: none
// Ex: logging.SetupDefault("v1.0", "app") → global logger configured
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// Setup creates logger from env vars.
// Out: *slog.Logger - JSON|text based on LOG_FORMAT
// Ex: Setup() → JSONHandler logger
func Setup() *slog.Logger {
	opts := &slog.HandlerOptions{
		Level:     getLogLevel(),
		AddSource: isDevelopment(),
	}

	switch getLogFormat() {
	case "text":
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	case "json":
		return slog.New(slog.NewJSONHandler(os.Stdout, opts))
	default:
		// Default to JSON for production
		return slog.New(slog.NewJSONHandler(os.Stdout, opts))
	}
}

// SetupDefault sets global logger with metadata.
// In: version, instanceID string - service info
// Ex: SetupDefault("v1.0", "node-1") → global logger set
func SetupDefault(version, instanceID string) {
	logger := Setup()

	// Add service context to all logs
	logger = logger.With(
		"service", "qdb-nats-connector",
		"version", version,
		"instance_id", instanceID,
	)

	slog.SetDefault(logger)
}

// getLogLevel parses LOG_LEVEL env var.
// Out: slog.Level - DEBUG|INFO|WARN|ERROR
// Ex: getLogLevel() → slog.LevelInfo
func getLogLevel() slog.Level {
	switch strings.ToUpper(os.Getenv("LOG_LEVEL")) {
	case "DEBUG":
		return slog.LevelDebug
	case "WARN":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// getLogFormat reads LOG_FORMAT env var.
// Out: string - "json"|"text", default "json"
// Ex: getLogFormat() → "json"
func getLogFormat() string {
	format := os.Getenv("LOG_FORMAT")
	if format == "" {
		return "json" // default for production
	}

	return strings.ToLower(format)
}

// isDevelopment checks ENVIRONMENT=dev.
// Out: bool - true if dev|development|∅
// Ex: isDevelopment() → true
func isDevelopment() bool {
	env := strings.ToLower(os.Getenv("ENVIRONMENT"))

	return env == "development" || env == "dev" || env == ""
}
