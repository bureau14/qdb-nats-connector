// Package generator provides the generation engine that integrates all components.
// The engine coordinates template parsing, generator creation, and record generation
// to produce structured JSON data according to template specifications.
package generator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal"
	"github.com/bureau14/qdb-nats-connector/tools/generator/internal/transformer"
)

// Engine coordinates the data generation process by managing templates and generators.
// It integrates template parsing, generator instantiation, and record generation
// into a unified interface for producing structured data.
type Engine struct {
	template   *internal.Template
	generators map[string]*GeneratorInstance
}

// NewEngine creates a new generation engine from a template file.
// It parses the template, validates field definitions using the registry, and creates generator instances
// for each field. Returns an error if template parsing fails or generators cannot be created.
func NewEngine(templatePath string) (*Engine, error) {
	template, err := internal.ParseTemplate(templatePath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse template: %w", err)
	}

	generators, err := createGenerators(template)
	if err != nil {
		return nil, fmt.Errorf("failed to create generators: %w", err)
	}

	return &Engine{
		template:   template,
		generators: generators,
	}, nil
}

// createGenerators instantiates field generators for all template fields
func createGenerators(template *internal.Template) (map[string]*GeneratorInstance, error) {
	generators := make(map[string]*GeneratorInstance)

	for _, fieldDef := range template.Fields {
		generator, err := CreateGenerator(context.Background(), fieldDef.Type, fieldDef.Config)
		if err != nil {
			return nil, fmt.Errorf("failed to create generator for field %s: %w", fieldDef.Name, err)
		}
		generators[fieldDef.Name] = generator
	}

	return generators, nil
}

// GenerateRecords generates the specified number of records and writes them as JSON to the writer.
// Each record is generated according to the template specification, with field values
// produced by their respective generators. Adds a "$table" field if template.Table is set.
func (e *Engine) GenerateRecords(ctx context.Context, count int, writer io.Writer) error {
	encoder := json.NewEncoder(writer)

	for i := range count {
		record, err := e.generateSingleRecord(ctx)
		if err != nil {
			return fmt.Errorf("failed to generate record %d: %w", i+1, err)
		}

		err = encoder.Encode(record)
		if err != nil {
			return fmt.Errorf("failed to encode record %d: %w", i+1, err)
		}
	}

	return nil
}

// GetTemplate returns the engine's template
// Out: template used for generation
// Ex: tmpl := engine.GetTemplate()
func (e *Engine) GetTemplate() *internal.Template {
	return e.template
}

// GetGenerators returns the engine's field generators
// Out: map of field generators
// Ex: gens := engine.GetGenerators()
func (e *Engine) GetGenerators() map[string]*GeneratorInstance {
	return e.generators
}

// generateSingleRecord creates a single record by calling all field generators
func (e *Engine) generateSingleRecord(ctx context.Context) (interface{}, error) {
	record := make(map[string]interface{})

	// Process fields in order to respect dependencies
	for _, fieldDef := range e.template.Fields {
		generator := e.generators[fieldDef.Name]

		var value interface{}
		var err error

		// Check if generator supports context-aware generation
		if genWithCtx, ok := generator.GetGenerator().(internal.FieldGeneratorWithContext); ok {
			value, err = genWithCtx.GenerateWithContext(ctx, record)
		} else {
			value, err = generator.Generate(ctx)
		}

		if err != nil {
			return nil, fmt.Errorf("failed to generate field %s: %w", fieldDef.Name, err)
		}

		// Skip internal fields in final output
		if !fieldDef.Internal {
			// Handle nested paths (e.g., "device.info.id")
			if strings.Contains(fieldDef.Name, ".") {
				setNestedValue(record, fieldDef.Name, value)
			} else {
				// Existing flat fields work exactly as before
				record[fieldDef.Name] = value
			}
		} else {
			// Store internal fields in record for dependencies but don't output them
			record[fieldDef.Name] = value
		}
	}

	if e.template.Table != "" {
		record["$table"] = e.template.Table
	}

	// Remove internal fields before returning
	for _, fieldDef := range e.template.Fields {
		if fieldDef.Internal {
			delete(record, fieldDef.Name)
		}
	}

	// Apply post-transformations if defined
	if len(e.template.PostTransformations) > 0 {
		result := interface{}(record)

		for _, transformDef := range e.template.PostTransformations {
			t, err := transformer.GetTransformer(transformDef.Type, transformDef.Config)
			if err != nil {
				return nil, fmt.Errorf("failed to create transformer %s: %w", transformDef.Type, err)
			}

			result, err = t.Transform(ctx, result)
			if err != nil {
				return nil, fmt.Errorf("transformation %s failed: %w", transformDef.Type, err)
			}
		}

		return result, nil
	}

	// No transformations, return record as before
	return record, nil
}

// setNestedValue sets a value at a nested path within a map structure.
// It creates intermediate maps as needed to build the nested structure.
// For example, path "device.info.id" with value "123" creates:
// {"device": {"info": {"id": "123"}}}
func setNestedValue(record map[string]interface{}, path string, value interface{}) {
	parts := strings.Split(path, ".")
	current := record

	// Navigate/create nested structure for all parts except the last
	for _, part := range parts[:len(parts)-1] {
		if _, exists := current[part]; !exists {
			current[part] = make(map[string]interface{})
		}
		current = current[part].(map[string]interface{})
	}

	// Set the value at the final key
	current[parts[len(parts)-1]] = value
}
