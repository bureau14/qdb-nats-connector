package generators

import (
	"testing"
)

func TestNewWeightedChoiceGenerator_WeightValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      map[string]interface{}
		wantErr     bool
		errContains string
	}{
		{
			name: "zero weight",
			config: map[string]interface{}{
				"choices": []interface{}{
					map[string]interface{}{
						"value":  "test",
						"weight": 0,
					},
				},
			},
			wantErr:     true,
			errContains: "weight at index 0 must be positive",
		},
		{
			name: "negative weight",
			config: map[string]interface{}{
				"choices": []interface{}{
					map[string]interface{}{
						"value":  "test",
						"weight": -5,
					},
				},
			},
			wantErr:     true,
			errContains: "weight at index 0 must be positive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewWeightedChoiceGenerator(tt.config)
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewWeightedChoiceGenerator() expected error but got none")

					return
				}
				if tt.errContains != "" && !containsString(err.Error(), tt.errContains) {
					t.Errorf("NewWeightedChoiceGenerator() error = %v, want error containing %q", err, tt.errContains)
				}
			}
		})
	}
}

// containsString checks if haystack contains needle
func containsString(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (haystack == needle ||
		needle == "" ||
		(haystack != "" && (haystack[:len(needle)] == needle ||
			haystack[len(haystack)-len(needle):] == needle ||
			findSubstring(haystack, needle))))
}

func findSubstring(haystack, needle string) bool {
	for i := 0; i <= len(haystack)-len(needle); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}

	return false
}
