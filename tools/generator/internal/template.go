// Package internal handles YAML template parsing and validation for data generation.
// This module provides functionality to parse YAML template files into Template structs
// with comprehensive validation of required fields and field type recognition.
package internal

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// recognizedFieldTypes contains the set of valid field generator types
var recognizedFieldTypes = map[string]bool{
	"timestamp":         true,
	"random_int":        true,
	"random_float":      true,
	"sequence":          true,
	"constant":          true,
	"uuid":              true,
	"random_string":     true,
	"brownian_motion":   true,
	"network_burst":     true,
	"pattern_composite": true,
	"signal_synthesis":  true,
	"gzipped_json":      true,
	"stress_pattern":    true,
	"chaos":             true,
}

// ParseTemplate reads and parses a YAML template file into a Template struct.
// It validates that all required fields are present and that field types are recognized.
// Returns an error if the file cannot be read, parsed, or if validation fails.
func ParseTemplate(filePath string) (*Template, error) {
	data, err := readTemplateFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read template file %s: %w", filePath, err)
	}

	template, err := parseYAMLTemplate(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse YAML template from %s: %w", filePath, err)
	}

	err = validateTemplate(template)
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

// validateTemplate ensures the template has all required fields and valid field types
func validateTemplate(template *Template) error {
	err := validateRequiredFields(template)
	if err != nil {
		return err
	}

	err = validateFieldTypes(template)
	if err != nil {
		return err
	}

	return nil
}

// validateRequiredFields checks that name and fields are present and fields is not empty
func validateRequiredFields(template *Template) error {
	if template.Name == "" {
		return fmt.Errorf("required field 'name' is missing or empty")
	}

	if len(template.Fields) == 0 {
		return fmt.Errorf("at least one field must be defined in 'fields'")
	}

	return nil
}

// validateFieldTypes ensures all field types are recognized
func validateFieldTypes(template *Template) error {
	for fieldName, fieldDef := range template.Fields {
		if fieldDef.Type == "" {
			return fmt.Errorf("field '%s' is missing required 'type'", fieldName)
		}

		if !recognizedFieldTypes[fieldDef.Type] {
			return fmt.Errorf("field '%s' has unrecognized type '%s'", fieldName, fieldDef.Type)
		}
	}

	return nil
}
