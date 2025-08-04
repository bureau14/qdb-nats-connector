// Package generator provides the generation engine that integrates all components.
// The engine coordinates template parsing, generator creation, and record generation
// to produce structured JSON data according to template specifications.
package generator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal"
)

// Engine coordinates the data generation process by managing templates and generators.
// It integrates template parsing, generator instantiation, and record generation
// into a unified interface for producing structured data.
type Engine struct {
	template   *internal.Template
	generators map[string]*GeneratorInstance
}

// NewEngine creates a new generation engine from a template file.
// It parses the template, validates field definitions, and creates generator instances
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

	for fieldName, fieldDef := range template.Fields {
		generator, err := CreateGenerator(context.Background(), fieldDef.Type, fieldDef.Config)
		if err != nil {
			return nil, fmt.Errorf("failed to create generator for field %s: %w", fieldName, err)
		}
		generators[fieldName] = generator
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
func (e *Engine) generateSingleRecord(ctx context.Context) (map[string]interface{}, error) {
	record := make(map[string]interface{})

	for fieldName, generator := range e.generators {
		value, err := generator.Generate(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to generate field %s: %w", fieldName, err)
		}
		record[fieldName] = value
	}

	if e.template.Table != "" {
		record["$table"] = e.template.Table
	}

	return record, nil
}
