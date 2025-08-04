// Package generators provides concrete implementations of field generators.
// This package contains the constant generator which produces a fixed value
// for each generation call.
package generators

import (
	"context"
	"fmt"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal"
	"github.com/bureau14/qdb-nats-connector/tools/generator/internal/generator"
)

// constantGenerator generates a fixed value specified in configuration.
// It stores a single value and returns it on every Generate call.
type constantGenerator struct {
	value interface{}
}

// NewConstantGenerator creates a constant generator from configuration options.
// Supported config options:
//   - value: any type - the fixed value to return on each Generate call (required)
func NewConstantGenerator(config map[string]interface{}) (*constantGenerator, error) {
	value, exists := config["value"]
	if !exists {
		return nil, fmt.Errorf("constant generator requires 'value' configuration parameter")
	}

	return &constantGenerator{
		value: value,
	}, nil
}

// Generate returns the configured constant value.
// The value is returned unchanged on every call.
func (g *constantGenerator) Generate(ctx context.Context) (interface{}, error) {
	return g.value, nil
}

// init registers the constant generator with the global registry.
func init() {
	generator.RegisterGenerator("constant", func(config map[string]interface{}) (internal.FieldGenerator, error) {
		return NewConstantGenerator(config)
	})
}
