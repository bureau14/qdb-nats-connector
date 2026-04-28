package generators

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewErrorInjectionGenerator_ErrorRateValidation(t *testing.T) {
	t.Run("invalid error_rate range below 0 returns error", func(t *testing.T) {
		config := map[string]interface{}{
			"normal": map[string]interface{}{
				"type": "constant",
				"config": map[string]interface{}{
					"value": 42,
				},
			},
			"error_rate": -0.1, // Invalid range
			"errors": []interface{}{
				map[string]interface{}{
					"value":  "ERROR",
					"weight": 1.0,
				},
			},
		}

		_, err := NewErrorInjectionGenerator(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "error_rate must be between 0.0 and 1.0")
	})

	t.Run("invalid error_rate range above 1 returns error", func(t *testing.T) {
		config := map[string]interface{}{
			"normal": map[string]interface{}{
				"type": "constant",
				"config": map[string]interface{}{
					"value": 42,
				},
			},
			"error_rate": 1.5, // Invalid range
			"errors": []interface{}{
				map[string]interface{}{
					"value":  "ERROR",
					"weight": 1.0,
				},
			},
		}

		_, err := NewErrorInjectionGenerator(config)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "error_rate must be between 0.0 and 1.0")
	})
}
