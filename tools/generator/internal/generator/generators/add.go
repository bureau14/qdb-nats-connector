// Package generators provides concrete implementations of field generators.
// This package contains the add calculator generator which sums values
// from specified source fields.
package generators

import (
	"context"
	"fmt"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal"
	"github.com/bureau14/qdb-nats-connector/tools/generator/internal/generator"
)

// addGenerator calculates the sum of values from multiple source fields.
// It stores source field names and context for receiving generated values.
type addGenerator struct {
	sources []string
	context map[string]interface{}
}

// NewAddGenerator creates an add calculator generator from configuration options.
// Supported config options:
//   - sources: []string - array of field names to add together (required)
func NewAddGenerator(config map[string]interface{}) (*addGenerator, error) {
	// Extract sources array (required)
	sourcesRaw, exists := config["sources"]
	if !exists {
		return nil, fmt.Errorf("add generator requires 'sources' configuration parameter")
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

	return &addGenerator{
		sources: sources,
		context: make(map[string]interface{}),
	}, nil
}

// Generate returns an error indicating that add generator requires GenerateWithContext support.
// This is a temporary implementation until Phase 3.2 adds context support for calculator generators.
func (g *addGenerator) Generate(ctx context.Context) (interface{}, error) {
	return nil, fmt.Errorf("add generator requires GenerateWithContext support")
}

// GenerateWithContext sums values from multiple source fields
func (g *addGenerator) GenerateWithContext(ctx context.Context, record map[string]interface{}) (interface{}, error) {
	var sum float64

	for _, source := range g.sources {
		sourceValue, exists := record[source]
		if !exists {
			return nil, fmt.Errorf("source field '%s' not found in record", source)
		}

		// Convert to float64 for addition
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

		sum += numValue
	}

	return sum, nil
}

// init registers the add generator with the global registry.
func init() {
	generator.RegisterGenerator("add", func(config map[string]interface{}) (internal.FieldGenerator, error) {
		return NewAddGenerator(config)
	})
}
