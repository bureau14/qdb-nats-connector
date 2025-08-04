// Package generators provides concrete implementations of field generators.
// This package contains the object generator which creates nested objects
// by orchestrating multiple sub-generators for each field.
package generators

import (
	"context"
	"fmt"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal"
	"github.com/bureau14/qdb-nats-connector/tools/generator/internal/generator"
)

// objectGenerator generates nested objects with multiple fields.
// It orchestrates sub-generators to create structured data objects.
type objectGenerator struct {
	fields map[string]*generator.GeneratorInstance
}

// NewObjectGenerator creates an object generator from configuration options.
// Supported config options:
//   - fields: map of field names to generator definitions (required)
//
// Example configuration:
//
//	fields:
//	  id:
//	    type: uuid
//	    config: {}
//	  name:
//	    type: constant
//	    config:
//	      value: "sensor-01"
//	  status:
//	    type: weighted_choice
//	    config:
//	      choices:
//	        - value: "online"
//	          weight: 90
//	        - value: "offline"
//	          weight: 10
func NewObjectGenerator(config map[string]interface{}) (*objectGenerator, error) {
	fieldsRaw, exists := config["fields"]
	if !exists {
		return nil, fmt.Errorf("object generator requires 'fields' configuration parameter")
	}

	fieldsMap, ok := fieldsRaw.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("fields must be a map")
	}

	if len(fieldsMap) == 0 {
		return nil, fmt.Errorf("fields map cannot be empty")
	}

	fields := make(map[string]*generator.GeneratorInstance)

	for fieldName, fieldDefRaw := range fieldsMap {
		fieldDefMap, ok := fieldDefRaw.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("field definition for '%s' must be an object", fieldName)
		}

		generatorType, hasType := fieldDefMap["type"]
		if !hasType {
			return nil, fmt.Errorf("field '%s' missing 'type' field", fieldName)
		}

		generatorTypeStr, ok := generatorType.(string)
		if !ok {
			return nil, fmt.Errorf("field '%s' type must be a string", fieldName)
		}

		var fieldConfig map[string]interface{}
		if configRaw, hasConfig := fieldDefMap["config"]; hasConfig {
			if configMap, ok := configRaw.(map[string]interface{}); ok {
				fieldConfig = configMap
			} else {
				return nil, fmt.Errorf("field '%s' config must be an object", fieldName)
			}
		} else {
			fieldConfig = make(map[string]interface{})
		}

		subGenerator, err := generator.CreateGenerator(context.Background(), generatorTypeStr, fieldConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create generator for field '%s': %w", fieldName, err)
		}

		fields[fieldName] = subGenerator
	}

	return &objectGenerator{
		fields: fields,
	}, nil
}

// Generate creates a new object by calling Generate on each sub-generator.
// Returns a map[string]interface{} containing all field values.
func (g *objectGenerator) Generate(ctx context.Context) (interface{}, error) {
	result := make(map[string]interface{})

	for fieldName, subGenerator := range g.fields {
		value, err := subGenerator.Generate(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to generate field '%s': %w", fieldName, err)
		}
		result[fieldName] = value
	}

	return result, nil
}

// init registers the object generator with the global registry.
func init() {
	generator.RegisterGenerator("object", func(config map[string]interface{}) (internal.FieldGenerator, error) {
		return NewObjectGenerator(config)
	})
}
