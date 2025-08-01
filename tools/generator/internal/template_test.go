package internal

import (
	"os"
	"path/filepath"
	"testing"
)

// TestParseTemplate_Valid tests successful parsing of a valid template
func TestParseTemplate_Valid(t *testing.T) {
	// Create testdata directory relative to this test file
	testdataPath := filepath.Join("..", "testdata", "basic.yaml")

	template, err := ParseTemplate(testdataPath)
	if err != nil {
		t.Fatalf("Expected successful parsing, got error: %v", err)
	}

	if template.Name != "basic_template" {
		t.Errorf("Expected name 'basic_template', got '%s'", template.Name)
	}

	if template.Table != "test_table" {
		t.Errorf("Expected table 'test_table', got '%s'", template.Table)
	}

	if len(template.Fields) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(template.Fields))
	}

	// Verify field types are recognized
	if template.Fields["timestamp_field"].Type != "timestamp" {
		t.Errorf("Expected timestamp_field type 'timestamp', got '%s'", template.Fields["timestamp_field"].Type)
	}

	if template.Fields["value_field"].Type != "random_int" {
		t.Errorf("Expected value_field type 'random_int', got '%s'", template.Fields["value_field"].Type)
	}
}

// TestParseTemplate_MissingRequiredFields tests validation failure for missing required fields
func TestParseTemplate_MissingRequiredFields(t *testing.T) {
	// Create temporary invalid template file
	tempDir := t.TempDir()
	invalidTemplatePath := filepath.Join(tempDir, "invalid.yaml")

	invalidContent := `# Missing name field
table: "test_table"
fields: {}`

	err := os.WriteFile(invalidTemplatePath, []byte(invalidContent), 0o600)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	_, err = ParseTemplate(invalidTemplatePath)
	if err == nil {
		t.Fatal("Expected validation error for missing required fields, got nil")
	}

	// Should mention missing name field
	if !containsString(err.Error(), "required field 'name' is missing") {
		t.Errorf("Expected error about missing name field, got: %v", err)
	}
}

// TestParseTemplate_UnknownFieldType tests validation failure for unrecognized field types
func TestParseTemplate_UnknownFieldType(t *testing.T) {
	// Create temporary template with unknown field type
	tempDir := t.TempDir()
	invalidTemplatePath := filepath.Join(tempDir, "unknown_type.yaml")

	invalidContent := `name: "test_template"
table: "test_table"
fields:
  bad_field:
    type: "unknown_type"
    config: {}`

	err := os.WriteFile(invalidTemplatePath, []byte(invalidContent), 0o600)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	_, err = ParseTemplate(invalidTemplatePath)
	if err == nil {
		t.Fatal("Expected validation error for unknown field type, got nil")
	}

	// Should mention unrecognized type
	if !containsString(err.Error(), "unrecognized type 'unknown_type'") {
		t.Errorf("Expected error about unrecognized type, got: %v", err)
	}
}

// containsString checks if a string contains a substring
func containsString(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr ||
			(len(s) > len(substr) &&
				(s[:len(substr)] == substr ||
					s[len(s)-len(substr):] == substr ||
					findSubstring(s, substr))))
}

// findSubstring performs a simple substring search
func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}

	return false
}
