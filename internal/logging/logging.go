package logging

import (
	"log/slog"
	"os"
	"strings"
)

// Setup configures and returns a structured logger based on environment variables.
// It supports different log levels and formats for development vs production use.
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

// SetupDefault configures the default slog logger with service metadata.
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

// getLogLevel returns the log level from environment variable or defaults to Info.
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

// getLogFormat returns the log format from environment variable or defaults to json.
func getLogFormat() string {
	format := os.Getenv("LOG_FORMAT")
	if format == "" {
		return "json" // default for production
	}
	return strings.ToLower(format)
}

// isDevelopment checks if we're running in development mode.
func isDevelopment() bool {
	env := strings.ToLower(os.Getenv("ENVIRONMENT"))
	return env == "development" || env == "dev" || env == ""
}