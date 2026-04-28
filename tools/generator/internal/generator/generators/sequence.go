// Package generators provides concrete implementations of field generators.
// This package contains the sequence generator which produces sequential numeric values
// with configurable start, end, step, and repeat behavior.
package generators

import (
	"context"
	"fmt"
	"math"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal"
	"github.com/bureau14/qdb-nats-connector/tools/generator/internal/generator"
)

// sequenceGenerator generates sequential integer values with configurable parameters.
// It maintains state for the current value and advances by a specified step on each generation.
type sequenceGenerator struct {
	current int64
	start   int64
	end     int64
	step    int64
	repeat  bool
}

// NewSequenceGenerator creates a sequence generator from configuration options.
// Supported config options:
//   - start: int starting value (defaults to 1)
//   - end: int ending value (defaults to math.MaxInt64)
//   - step: int increment value (defaults to 1)
//   - repeat: bool whether to reset to start when end is reached (defaults to false)
func NewSequenceGenerator(config map[string]interface{}) (*sequenceGenerator, error) {
	gen := &sequenceGenerator{
		start:  1,
		end:    math.MaxInt64,
		step:   1,
		repeat: false,
	}

	if start, ok := config["start"].(int); ok {
		gen.start = int64(start)
	}

	if end, ok := config["end"].(int); ok {
		gen.end = int64(end)
	}

	if step, ok := config["step"].(int); ok {
		gen.step = int64(step)
	}

	if repeat, ok := config["repeat"].(bool); ok {
		gen.repeat = repeat
	}

	gen.current = gen.start

	return gen, nil
}

// Generate returns the current sequence value and advances to the next value.
// Each call increments the current value by the configured step.
// If repeat is enabled and the end is reached, resets to start.
// If repeat is disabled and the end is exceeded, returns an error.
func (g *sequenceGenerator) Generate(ctx context.Context) (interface{}, error) {
	if g.current > g.end {
		if g.repeat {
			g.current = g.start
		} else {
			return nil, fmt.Errorf("sequence exceeded end value %d", g.end)
		}
	}

	value := g.current
	g.current += g.step

	return value, nil
}

// init registers the sequence generator with the global registry.
func init() {
	generator.RegisterGenerator("sequence", func(config map[string]interface{}) (internal.FieldGenerator, error) {
		return NewSequenceGenerator(config)
	})
}
