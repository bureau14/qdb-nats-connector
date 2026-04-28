// Package generators provides concrete implementations of field generators.
// This package contains the multiply calculator generator which multiplies a source field
// by a factor with optional addition.
package generators

import (
	"context"
	"fmt"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal"
	"github.com/bureau14/qdb-nats-connector/tools/generator/internal/generator"
)

// multiplyGenerator multiplies a source field by a factor with optional addition.
// It stores the source field name, multiplication factor, optional add value, and context for receiving generated values.
type multiplyGenerator struct {
	source  string
	factor  float64
	add     float64
	context map[string]interface{}
}

// NewMultiplyGenerator creates a multiply calculator generator from configuration options.
// Supported config options:
//   - source: string - field name to multiply (required)
//   - factor: float64 - multiplication factor (required)
//   - add: float64 - optional value to add after multiplication (defaults to 0.0)
func NewMultiplyGenerator(config map[string]interface{}) (*multiplyGenerator, error) {
	// Extract source (required)
	sourceRaw, exists := config["source"]
	if !exists {
		return nil, fmt.Errorf("multiply generator requires 'source' configuration parameter")
	}

	source, ok := sourceRaw.(string)
	if !ok {
		return nil, fmt.Errorf("source must be a string")
	}

	if source == "" {
		return nil, fmt.Errorf("source cannot be empty")
	}

	// Extract factor (required)
	factorRaw, exists := config["factor"]
	if !exists {
		return nil, fmt.Errorf("multiply generator requires 'factor' configuration parameter")
	}

	factor, ok := factorRaw.(float64)
	if !ok {
		return nil, fmt.Errorf("factor must be a float64")
	}

	// Extract add (optional, defaults to 0.0)
	add := 0.0
	if addVal, ok := config["add"].(float64); ok {
		add = addVal
	}

	return &multiplyGenerator{
		source:  source,
		factor:  factor,
		add:     add,
		context: make(map[string]interface{}),
	}, nil
}

// Generate returns an error indicating that multiply generator requires GenerateWithContext support.
// This is a temporary implementation until Phase 3.2 adds context support for calculator generators.
func (g *multiplyGenerator) Generate(ctx context.Context) (interface{}, error) {
	return nil, fmt.Errorf("multiply generator requires GenerateWithContext support")
}

// GenerateWithContext multiplies a source field value by a factor and optionally adds a value
func (g *multiplyGenerator) GenerateWithContext(ctx context.Context, record map[string]interface{}) (interface{}, error) {
	sourceValue, exists := record[g.source]
	if !exists {
		return nil, fmt.Errorf("source field '%s' not found in record", g.source)
	}

	// Convert to float64 for multiplication
	var numValue float64
	switch v := sourceValue.(type) {
	case float64:
		numValue = v
	case int:
		numValue = float64(v)
	case int64:
		numValue = float64(v)
	default:
		return nil, fmt.Errorf("source field '%s' has non-numeric value: %T", g.source, sourceValue)
	}

	result := numValue*g.factor + g.add

	return result, nil
}

// init registers the multiply generator with the global registry.
func init() {
	generator.RegisterGenerator("multiply", func(config map[string]interface{}) (internal.FieldGenerator, error) {
		return NewMultiplyGenerator(config)
	})
}
