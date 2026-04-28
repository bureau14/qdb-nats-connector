// Package generators provides concrete implementations of field generators.
// This package contains the min calculator generator which computes minimum values
// from specified source fields with optional multiplication.
package generators

import (
	"context"
	"fmt"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal"
	"github.com/bureau14/qdb-nats-connector/tools/generator/internal/generator"
)

// minGenerator calculates the minimum value from multiple source fields.
// It stores source field names, an optional multiplier, and context for receiving generated values.
type minGenerator struct {
	sources  []string
	multiply float64
	context  map[string]interface{}
}

// NewMinGenerator creates a min calculator generator from configuration options.
// Supported config options:
//   - sources: []string - array of field names to compare (required)
//   - multiply: float64 - optional multiplier applied to result (defaults to 1.0)
func NewMinGenerator(config map[string]interface{}) (*minGenerator, error) {
	// Extract sources array (required)
	sourcesRaw, exists := config["sources"]
	if !exists {
		return nil, fmt.Errorf("min generator requires 'sources' configuration parameter")
	}

	// Convert sources to string slice
	sourcesSlice, ok := sourcesRaw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("sources must be an array")
	}

	if len(sourcesSlice) == 0 {
		return nil, fmt.Errorf("sources array cannot be empty")
	}

	sources := make([]string, len(sourcesSlice))
	for i, source := range sourcesSlice {
		sourceStr, ok := source.(string)
		if !ok {
			return nil, fmt.Errorf("all sources must be strings")
		}
		sources[i] = sourceStr
	}

	// Extract multiply (optional, defaults to 1.0)
	multiply := 1.0
	if multiplyVal, ok := config["multiply"].(float64); ok {
		multiply = multiplyVal
	}

	return &minGenerator{
		sources:  sources,
		multiply: multiply,
		context:  make(map[string]interface{}),
	}, nil
}

// Generate returns an error indicating that min generator requires GenerateWithContext support.
// This is a temporary implementation until Phase 3.2 adds context support for calculator generators.
func (g *minGenerator) Generate(ctx context.Context) (interface{}, error) {
	return nil, fmt.Errorf("min generator requires GenerateWithContext support")
}

// GenerateWithContext computes the minimum value from multiple source fields with optional multiplication
func (g *minGenerator) GenerateWithContext(ctx context.Context, record map[string]interface{}) (interface{}, error) {
	if len(g.sources) == 0 {
		return nil, fmt.Errorf("no source fields configured for min generator")
	}

	var minValue *float64

	for _, source := range g.sources {
		sourceValue, exists := record[source]
		if !exists {
			return nil, fmt.Errorf("source field '%s' not found in record", source)
		}

		// Convert to float64 for comparison
		var numValue float64
		switch v := sourceValue.(type) {
		case float64:
			numValue = v
		case int:
			numValue = float64(v)
		case int64:
			numValue = float64(v)
		default:
			return nil, fmt.Errorf("source field '%s' has non-numeric value: %T", source, sourceValue)
		}

		if minValue == nil || numValue < *minValue {
			minValue = &numValue
		}
	}

	if minValue == nil {
		return nil, fmt.Errorf("no valid numeric values found in source fields")
	}

	result := *minValue * g.multiply

	return result, nil
}

// init registers the min generator with the global registry.
func init() {
	generator.RegisterGenerator("min", func(config map[string]interface{}) (internal.FieldGenerator, error) {
		return NewMinGenerator(config)
	})
}
