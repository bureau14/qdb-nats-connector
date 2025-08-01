// Package generators provides concrete implementations of field generators.
// This package contains the timestamp generator which produces time-based values
// with configurable intervals and formatting options.
package generators

import (
	"context"
	"fmt"
	"time"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal"
	"github.com/bureau14/qdb-nats-connector/tools/generator/internal/generator"
)

// timestampGenerator generates sequential timestamp values with configurable intervals.
// It maintains state for the current time and advances by a specified interval on each generation.
type timestampGenerator struct {
	currentTime time.Time
	interval    time.Duration
	format      string
}

// NewTimestampGenerator creates a timestamp generator from configuration options.
// Supported config options:
//   - start: string timestamp for initial value (defaults to current time)
//   - interval: string duration between timestamps (defaults to "1s")
//   - format: string time format (defaults to RFC3339)
func NewTimestampGenerator(config map[string]interface{}) (*timestampGenerator, error) {
	gen := &timestampGenerator{
		currentTime: time.Now(),
		interval:    time.Second,
		format:      time.RFC3339,
	}

	if startStr, ok := config["start"].(string); ok {
		startTime, err := time.Parse(time.RFC3339, startStr)
		if err != nil {
			return nil, fmt.Errorf("invalid start time format: %w", err)
		}
		gen.currentTime = startTime
	}

	if intervalStr, ok := config["interval"].(string); ok {
		interval, err := time.ParseDuration(intervalStr)
		if err != nil {
			return nil, fmt.Errorf("invalid interval format: %w", err)
		}
		gen.interval = interval
	}

	if format, ok := config["format"].(string); ok {
		gen.format = format
	}

	return gen, nil
}

// Generate returns a formatted timestamp and advances the internal time.
// Each call increments the current time by the configured interval.
func (g *timestampGenerator) Generate(ctx context.Context) (interface{}, error) {
	timestamp := g.currentTime.Format(g.format)
	g.currentTime = g.currentTime.Add(g.interval)

	return timestamp, nil
}

// init registers the timestamp generator with the global registry.
func init() {
	generator.RegisterGenerator("timestamp", func(config map[string]interface{}) (internal.FieldGenerator, error) {
		return NewTimestampGenerator(config)
	})
}
