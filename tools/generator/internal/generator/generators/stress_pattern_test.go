package generators

import (
	"testing"
)

func TestStressPatternGenerator_MissingPattern(t *testing.T) {
	config := map[string]interface{}{
		"period": "1m",
	}

	_, err := NewStressPatternGenerator(config)
	if err == nil {
		t.Error("Expected error for missing pattern field")
	}
}

func TestStressPatternGenerator_InvalidPattern(t *testing.T) {
	config := map[string]interface{}{
		"pattern": "invalid_pattern",
	}

	_, err := NewStressPatternGenerator(config)
	if err == nil {
		t.Error("Expected error for invalid/unknown pattern type")
	}
}

func TestStressPatternGenerator_InvalidPeriod(t *testing.T) {
	config := map[string]interface{}{
		"pattern": "sine_burst",
		"period":  "invalid",
	}

	_, err := NewStressPatternGenerator(config)
	if err == nil {
		t.Error("Expected error for invalid period format")
	}
}
