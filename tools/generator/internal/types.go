// Package internal provides core types for the data generator.
// This package defines the template structure, field definitions, and field generator interface
// used throughout the data generation system.
package internal

import "context"

// Template defines a data generation template with metadata and field definitions.
// It represents a complete specification for generating structured data including
// the target table, naming pattern, and field-level generation rules.
type Template struct {
	// Name is the human-readable identifier for this template
	Name string `yaml:"name"`

	// Table specifies the target QuasarDB table name for generated data
	Table string `yaml:"table"`

	// Pattern defines the naming or identification pattern for generated records
	Pattern string `yaml:"pattern"`

	// Fields is an ordered list of field definitions (changed from map)
	Fields []FieldDefinition `yaml:"fields"`

	// PostTransformations defines optional transformations to apply to the entire record
	PostTransformations []TransformationDefinition `yaml:"post_transformations,omitempty"`
}

// UnmarshalYAML implements custom YAML unmarshaling to support both old and new formats.
// Old format: fields as a map where keys are field names
// New format: fields as a slice where each field has a name property
func (t *Template) UnmarshalYAML(unmarshal func(interface{}) error) error {
	// Try new format first (fields as slice)
	var newFormat struct {
		Name                string                     `yaml:"name"`
		Table               string                     `yaml:"table"`
		Pattern             string                     `yaml:"pattern"`
		Fields              []FieldDefinition          `yaml:"fields"`
		PostTransformations []TransformationDefinition `yaml:"post_transformations,omitempty"`
	}

	err := unmarshal(&newFormat)
	if err == nil {
		// Successfully parsed as new format
		*t = Template(newFormat)

		return nil
	}

	// Fall back to old format (fields as map)
	var oldFormat struct {
		Name                string                            `yaml:"name"`
		Table               string                            `yaml:"table"`
		Pattern             string                            `yaml:"pattern"`
		Fields              map[string]map[string]interface{} `yaml:"fields"`
		PostTransformations []TransformationDefinition        `yaml:"post_transformations,omitempty"`
	}

	err = unmarshal(&oldFormat)
	if err != nil {
		return err
	}

	// Convert old format to new format
	t.Name = oldFormat.Name
	t.Table = oldFormat.Table
	t.Pattern = oldFormat.Pattern
	t.PostTransformations = oldFormat.PostTransformations
	t.Fields = make([]FieldDefinition, 0, len(oldFormat.Fields))

	// Convert map to slice, preserving field names
	for name, fieldData := range oldFormat.Fields {
		fieldDef := FieldDefinition{
			Name: name,
		}

		// Extract type
		if typeVal, ok := fieldData["type"].(string); ok {
			fieldDef.Type = typeVal
			delete(fieldData, "type") // Remove type from config
		}

		// Extract internal flag if present
		if internalVal, ok := fieldData["internal"].(bool); ok {
			fieldDef.Internal = internalVal
			delete(fieldData, "internal") // Remove internal from config
		}

		// Extract config
		if configVal, ok := fieldData["config"].(map[string]interface{}); ok {
			fieldDef.Config = configVal
		} else if len(fieldData) > 0 {
			// If there's no explicit config field, treat remaining fields as config
			fieldDef.Config = fieldData
		}

		t.Fields = append(t.Fields, fieldDef)
	}

	return nil
}

// FieldDefinition specifies how a single field should be generated.
// It includes the data type and configuration parameters specific to that field's generator.
type FieldDefinition struct {
	// Name is the field name in the output record
	Name string `yaml:"name"`

	// Type specifies the field generator type (e.g., "timestamp", "random_int", "sequence")
	Type string `yaml:"type"`

	// Config contains generator-specific configuration parameters
	// The structure varies based on the Type field
	Config map[string]interface{} `yaml:"config"`

	// Internal marks fields that should not appear in the final output
	// Used for intermediate calculations
	Internal bool `yaml:"internal,omitempty"`
}

// TransformationDefinition specifies a transformation to apply to generated data
type TransformationDefinition struct {
	Type   string                 `yaml:"type"`
	Config map[string]interface{} `yaml:"config,omitempty"`
}

// FieldGenerator defines the interface for generating field values.
// Implementations provide specific generation logic for different data types and patterns.
type FieldGenerator interface {
	// Generate produces a single field value based on the generator's configuration.
	// The context can be used for cancellation and to pass request-scoped values.
	Generate(ctx context.Context) (interface{}, error)
}

// FieldGeneratorWithContext extends FieldGenerator with context-aware generation.
// Generators implementing this interface can access previously generated field values
// to create field dependencies and calculated values.
type FieldGeneratorWithContext interface {
	FieldGenerator
	// GenerateWithContext produces a field value with access to previously generated fields.
	// The record parameter contains all fields generated so far in the current record.
	GenerateWithContext(ctx context.Context, record map[string]interface{}) (interface{}, error)
}
