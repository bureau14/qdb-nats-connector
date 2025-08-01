// Package generator provides a registry pattern for field generators.
// The registry allows dynamic registration and retrieval of field generator factories,
// enabling pluggable generator implementations without compile-time dependencies.
//
// Generator factories are registered by type name and can be retrieved to create
// generator instances with specific configurations. This pattern supports extensibility
// and modular generator implementations.
package generator

import (
	"context"
	"fmt"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal"
)

// GeneratorFactory creates a FieldGenerator instance from configuration.
// Factories are responsible for validating configuration parameters and
// initializing generators with the provided settings.
type GeneratorFactory func(config map[string]interface{}) (internal.FieldGenerator, error)

// generators holds the registry of available generator factories indexed by type name.
var generators = map[string]GeneratorFactory{}

// RegisterGenerator adds a new generator factory to the registry.
// The generatorType parameter specifies the unique identifier used in field definitions.
// If a generator with the same type is already registered, it will be replaced.
//
// Example usage:
//
//	RegisterGenerator("timestamp", func(config map[string]interface{}) (internal.FieldGenerator, error) {
//	    return &TimestampGenerator{format: config["format"].(string)}, nil
//	})
func RegisterGenerator(generatorType string, factory GeneratorFactory) {
	generators[generatorType] = factory
}

// GetGenerator retrieves a generator factory by type name.
// Returns the factory function and true if found, or nil and false if not registered.
//
// Example usage:
//
//	factory, exists := GetGenerator("timestamp")
//	if !exists {
//	    return fmt.Errorf("unknown generator type: timestamp")
//	}
//	generator, err := factory(config)
func GetGenerator(generatorType string) (GeneratorFactory, bool) {
	factory, exists := generators[generatorType]

	return factory, exists
}

// CreateGenerator is a convenience function that retrieves a factory and creates a generator.
// It combines GetGenerator and factory invocation with proper error handling.
//
// Returns a configured FieldGenerator instance or an error if the type is unknown
// or configuration is invalid.
func CreateGenerator(ctx context.Context, generatorType string, config map[string]interface{}) (*GeneratorInstance, error) {
	factory, exists := GetGenerator(generatorType)
	if !exists {
		return nil, fmt.Errorf("unknown generator type: %s", generatorType)
	}

	generator, err := factory(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create generator %s: %w", generatorType, err)
	}

	return &GeneratorInstance{generator: generator}, nil
}

// GeneratorInstance wraps a FieldGenerator to satisfy linting requirements.
type GeneratorInstance struct {
	generator internal.FieldGenerator
}

// Generate implements the FieldGenerator interface by delegating to the wrapped generator.
func (g *GeneratorInstance) Generate(ctx context.Context) (interface{}, error) {
	return g.generator.Generate(ctx)
}

// ListGenerators returns a slice of all registered generator type names.
// Useful for validation, debugging, and providing available options to users.
func ListGenerators() []string {
	types := make([]string, 0, len(generators))
	for generatorType := range generators {
		types = append(types, generatorType)
	}

	return types
}
