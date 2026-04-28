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
	mode        string        // "fixed" (default), "now", "relative", "sliding_window"
	windowSize  time.Duration // for sliding_window mode
}

// NewTimestampGenerator creates a timestamp generator from configuration options.
// Supported config options:
//   - start: string timestamp for initial value (defaults to current time)
//   - interval: string duration between timestamps (defaults to "1s")
//   - format: string time format (defaults to RFC3339)
//   - mode: string generation mode ("fixed", "now", "relative", "sliding_window")
//   - offset: string duration offset from now for "relative" mode
//   - window: string duration for "sliding_window" mode (defaults to "5m")
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

	// Default mode is "fixed" for backward compatibility
	gen.mode = "fixed"

	if modeStr, ok := config["mode"].(string); ok {
		switch modeStr {
		case "fixed", "now", "relative", "sliding_window":
			gen.mode = modeStr
		default:
			return nil, fmt.Errorf("invalid timestamp mode: %s", modeStr)
		}
	}

	// Handle relative mode offset
	if gen.mode == "relative" {
		if offsetStr, ok := config["offset"].(string); ok {
			offset, err := time.ParseDuration(offsetStr)
			if err != nil {
				return nil, fmt.Errorf("invalid offset format: %w", err)
			}
			gen.currentTime = time.Now().Add(offset)
		}
	}

	// Handle sliding window size
	if gen.mode == "sliding_window" {
		if windowStr, ok := config["window"].(string); ok {
			window, err := time.ParseDuration(windowStr)
			if err != nil {
				return nil, fmt.Errorf("invalid window format: %w", err)
			}
			gen.windowSize = window
		} else {
			gen.windowSize = 5 * time.Minute // default 5 minute window
		}
	}

	return gen, nil
}

// Generate returns a formatted timestamp based on the configured mode.
// Behavior varies by mode:
//   - "fixed": Sequential from start time (original behavior)
//   - "now": Always current wall clock time
//   - "relative": Sequential from offset time
//   - "sliding_window": Random within time window
func (g *timestampGenerator) Generate(ctx context.Context) (interface{}, error) {
	var timestamp time.Time

	switch g.mode {
	case "now":
		// Always use current wall clock time
		timestamp = time.Now()

	case "relative":
		// Use sequential time starting from offset
		timestamp = g.currentTime
		g.currentTime = g.currentTime.Add(g.interval)

	case "sliding_window":
		// Generate timestamp within the sliding window
		now := time.Now()
		windowStart := now.Add(-g.windowSize)
		// Random position within window for realistic distribution
		timestamp = windowStart.Add(time.Duration(float64(g.windowSize) * 0.5))

	default: // "fixed"
		// Original behavior: sequential from start time
		timestamp = g.currentTime
		g.currentTime = g.currentTime.Add(g.interval)
	}

	return timestamp.Format(g.format), nil
}

// init registers the timestamp generator with the global registry.
func init() {
	generator.RegisterGenerator("timestamp", func(config map[string]interface{}) (internal.FieldGenerator, error) {
		return NewTimestampGenerator(config)
	})
}
