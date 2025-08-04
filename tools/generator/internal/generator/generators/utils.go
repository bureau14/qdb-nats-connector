// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.

package generators

// getFloat64 extracts a float64 value from config by key
func getFloat64(config map[string]interface{}, key string) (float64, bool) {
	if val, ok := config[key]; ok {
		return getFloatFromInterface(val)
	}

	return 0, false
}

// getFloatFromInterface converts various numeric types to float64
func getFloatFromInterface(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	default:
		return 0, false
	}
}
