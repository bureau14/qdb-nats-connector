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

// GeneratorRegistration holds factory and metadata for a generator
type GeneratorRegistration struct {
	Factory  GeneratorFactory
	Metadata GeneratorMetadata
}

// generators holds the registry of available generators with metadata
var generators = map[string]GeneratorRegistration{}

// RegisterGenerator adds a new generator factory to the registry.
// The generatorType parameter specifies the unique identifier used in field definitions.
// If a generator with the same type is already registered, it will be replaced.
//
// Example usage:
//
//	RegisterGenerator("timestamp", func(config map[string]interface{}) (internal.FieldGenerator, error) {
//	    return &TimestampGenerator{format: config["format"].(string)}, nil
//	})
//
// RegisterGeneratorWithMetadata adds generator with metadata to registry
// In: generator type, factory, metadata
// Out: none
// Ex: RegisterGeneratorWithMetadata("brownian", factory, metadata)
func RegisterGeneratorWithMetadata(generatorType string, factory GeneratorFactory, metadata GeneratorMetadata) {
	generators[generatorType] = GeneratorRegistration{
		Factory:  factory,
		Metadata: metadata,
	}
}

func RegisterGenerator(generatorType string, factory GeneratorFactory) {
	// Use default metadata for backward compatibility
	metadata := GeneratorMetadata{
		Name:         generatorType,
		Description:  fmt.Sprintf("%s generator", generatorType),
		Version:      "1.0.0",
		Capabilities: GeneratorCapabilities{},
	}
	RegisterGeneratorWithMetadata(generatorType, factory, metadata)
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
	registration, exists := generators[generatorType]
	if !exists {
		return nil, false
	}

	return registration.Factory, true
}

// CreateGenerator is a convenience function that retrieves a factory and creates a generator.
// It combines GetGenerator and factory invocation with proper error handling.
//
// Returns a configured FieldGenerator instance or an error if the type is unknown
// or configuration is invalid.
func CreateGenerator(ctx context.Context, generatorType string, config map[string]interface{}) (*GeneratorInstance, error) {
	registration, exists := generators[generatorType]
	if !exists {
		return nil, fmt.Errorf("unknown generator type: %s", generatorType)
	}

	generator, err := registration.Factory(config)
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

// GetGenerator returns the underlying FieldGenerator
// Out: wrapped generator instance
// Ex: gen := instance.GetGenerator()
//
//nolint:ireturn // Need to expose underlying interface for state management
func (g *GeneratorInstance) GetGenerator() internal.FieldGenerator {
	return g.generator
}

// GetGeneratorMetadata retrieves metadata for a generator type
// In: generator type name
// Out: metadata if found, nil otherwise
// Ex: meta := GetGeneratorMetadata("brownian")
func GetGeneratorMetadata(generatorType string) *GeneratorMetadata {
	registration, exists := generators[generatorType]
	if !exists {
		return nil
	}

	return &registration.Metadata
}

// ListGeneratorsWithMetadata returns all generators with metadata
// Out: map of type to metadata
// Ex: all := ListGeneratorsWithMetadata()
func ListGeneratorsWithMetadata() map[string]GeneratorMetadata {
	result := make(map[string]GeneratorMetadata)
	for genType, registration := range generators {
		result[genType] = registration.Metadata
	}

	return result
}

// ListGeneratorsByCapability returns generators with specific capability
// In: capability name (e.g., "IsStateful", "IsBinary")
// Out: list of generator types with that capability
// Ex: stateful := ListGeneratorsByCapability("IsStateful")
func ListGeneratorsByCapability(capability string) []string {
	var result []string
	for genType, registration := range generators {
		switch capability {
		case "IsStateful":
			if registration.Metadata.Capabilities.IsStateful {
				result = append(result, genType)
			}
		case "IsBinary":
			if registration.Metadata.Capabilities.IsBinary {
				result = append(result, genType)
			}
		case "IsContinuous":
			if registration.Metadata.Capabilities.IsContinuous {
				result = append(result, genType)
			}
		}
	}

	return result
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
