// Package internal handles YAML template parsing and validation for data generation.
// This module provides functionality to parse YAML template files into Template structs
// with comprehensive validation of required fields and field type recognition.
package internal

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// TypeValidator is a function type for validating generator types
type TypeValidator func(generatorType string) bool

// registryTypeValidator is the default type validator that can be set by the generator package
var registryTypeValidator TypeValidator

// SetDefaultTypeValidator sets the default type validator used by ParseTemplate
func SetDefaultTypeValidator(validator TypeValidator) {
	registryTypeValidator = validator
}

// ParseTemplate reads and parses a YAML template file into a Template struct.
// It validates that all required fields are present and that field types are recognized.
// Returns an error if the file cannot be read, parsed, or if validation fails.
func ParseTemplate(filePath string) (*Template, error) {
	// Use default validator that delegates to registryTypeValidator
	return ParseTemplateWithValidator(filePath, registryTypeValidator)
}

// ParseTemplateWithValidator reads and parses a YAML template file with optional type validation.
// If typeValidator is provided, it will be used to validate field types during parsing.
// If typeValidator is nil, type validation is skipped (will be done later in generator creation).
func ParseTemplateWithValidator(filePath string, typeValidator TypeValidator) (*Template, error) {
	data, err := readTemplateFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read template file %s: %w", filePath, err)
	}

	template, err := parseYAMLTemplate(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse YAML template from %s: %w", filePath, err)
	}

	err = validateTemplateWithValidator(template, typeValidator)
	if err != nil {
		return nil, fmt.Errorf("template validation failed for %s: %w", filePath, err)
	}

	return template, nil
}

// readTemplateFile reads the contents of a template file from disk
func readTemplateFile(filePath string) ([]byte, error) {
	// #nosec G304 -- filePath is controlled by the caller, not user input
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("unable to read file: %w", err)
	}

	return data, nil
}

// parseYAMLTemplate unmarshals YAML data into a Template struct
func parseYAMLTemplate(data []byte) (*Template, error) {
	var template Template
	err := yaml.Unmarshal(data, &template)
	if err != nil {
		return nil, fmt.Errorf("YAML parsing error: %w", err)
	}

	return &template, nil
}

// validateTemplateWithValidator validates template using an optional type validator
func validateTemplateWithValidator(template *Template, typeValidator TypeValidator) error {
	if template.Name == "" {
		return fmt.Errorf("required field 'name' is missing or empty")
	}

	if len(template.Fields) == 0 {
		return fmt.Errorf("at least one field is required")
	}

	for _, fieldDef := range template.Fields {
		if fieldDef.Name == "" {
			return fmt.Errorf("field name is required")
		}
		if fieldDef.Type == "" {
			return fmt.Errorf("field '%s' is missing type", fieldDef.Name)
		}

		// Use type validator if provided
		if typeValidator != nil && !typeValidator(fieldDef.Type) {
			return fmt.Errorf("field '%s' has unrecognized type '%s'", fieldDef.Name, fieldDef.Type)
		}
	}

	return nil
}
