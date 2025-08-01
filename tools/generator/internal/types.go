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

	// Fields maps field names to their generation definitions
	Fields map[string]FieldDefinition `yaml:"fields"`
}

// FieldDefinition specifies how a single field should be generated.
// It includes the data type and configuration parameters specific to that field's generator.
type FieldDefinition struct {
	// Type specifies the field generator type (e.g., "timestamp", "random_int", "sequence")
	Type string `yaml:"type"`

	// Config contains generator-specific configuration parameters
	// The structure varies based on the Type field
	Config map[string]interface{} `yaml:"config"`
}

// FieldGenerator defines the interface for generating field values.
// Implementations provide specific generation logic for different data types and patterns.
type FieldGenerator interface {
	// Generate produces a single field value based on the generator's configuration.
	// The context can be used for cancellation and to pass request-scoped values.
	Generate(ctx context.Context) (interface{}, error)
}
