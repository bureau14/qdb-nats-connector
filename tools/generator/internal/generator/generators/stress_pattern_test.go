package generators

import (
	"context"
	"testing"
	"time"
)

func TestStressPatternGenerator_NewGenerator(t *testing.T) {
	tests := []struct {
		name    string
		config  map[string]interface{}
		wantErr bool
	}{
		{
			name: "sine_burst pattern with defaults",
			config: map[string]interface{}{
				"pattern": "sine_burst",
			},
			wantErr: false,
		},
		{
			name: "square_wave pattern with custom config",
			config: map[string]interface{}{
				"pattern":       "square_wave",
				"period":        "30s",
				"burst_factor":  50.0,
				"baseline_rate": 500.0,
			},
			wantErr: false,
		},
		{
			name: "sawtooth pattern",
			config: map[string]interface{}{
				"pattern": "sawtooth",
				"period":  "2m",
			},
			wantErr: false,
		},
		{
			name: "missing pattern",
			config: map[string]interface{}{
				"period": "1m",
			},
			wantErr: true,
		},
		{
			name: "invalid pattern",
			config: map[string]interface{}{
				"pattern": "invalid_pattern",
			},
			wantErr: true,
		},
		{
			name: "invalid period",
			config: map[string]interface{}{
				"pattern": "sine_burst",
				"period":  "invalid",
			},
			wantErr: true,
		},
		{
			name: "burst_factor too small",
			config: map[string]interface{}{
				"pattern":      "sine_burst",
				"burst_factor": 0.5,
			},
			wantErr: true,
		},
		{
			name: "negative baseline_rate",
			config: map[string]interface{}{
				"pattern":       "sine_burst",
				"baseline_rate": -100.0,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gen, err := NewStressPatternGenerator(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewStressPatternGenerator() error = %v, wantErr %v", err, tt.wantErr)

				return
			}
			if !tt.wantErr && gen == nil {
				t.Error("Expected generator to be created, got nil")
			}
		})
	}
}

func TestStressPatternGenerator_Generate(t *testing.T) {
	config := map[string]interface{}{
		"pattern":       "sine_burst",
		"period":        "1s", // Short period for testing
		"burst_factor":  2.0,
		"baseline_rate": 100.0,
	}

	gen, err := NewStressPatternGenerator(config)
	if err != nil {
		t.Fatalf("Failed to create generator: %v", err)
	}

	ctx := context.Background()

	// Test initialization
	err = gen.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize generator: %v", err)
	}

	// Generate some values
	var values []float64
	for range 10 {
		val, err := gen.Generate(ctx)
		if err != nil {
			t.Fatalf("Generate failed: %v", err)
		}

		value, ok := val.(float64)
		if !ok {
			t.Fatalf("Expected float64, got %T", val)
		}

		values = append(values, value)

		// Values should be non-negative
		if value < 0 {
			t.Errorf("Generated negative value: %v", value)
		}

		// Sleep a bit to see pattern progression
		time.Sleep(100 * time.Millisecond)
	}

	// Test that values are changing (pattern progression)
	allSame := true
	for i := 1; i < len(values); i++ {
		if values[i] != values[0] {
			allSame = false

			break
		}
	}
	if allSame {
		t.Error("All generated values are the same, pattern not progressing")
	}
}

func TestStressPatternGenerator_SquareWave(t *testing.T) {
	config := map[string]interface{}{
		"pattern":       "square_wave",
		"period":        "100ms", // Very short period for testing
		"burst_factor":  3.0,
		"baseline_rate": 100.0,
	}

	gen, err := NewStressPatternGenerator(config)
	if err != nil {
		t.Fatalf("Failed to create generator: %v", err)
	}

	ctx := context.Background()
	err = gen.Initialize(ctx)
	if err != nil {
		t.Fatalf("Failed to initialize generator: %v", err)
	}

	// Test that we get both high and low values
	var values []float64
	for range 20 {
		val, err := gen.Generate(ctx)
		if err != nil {
			t.Fatalf("Generate failed: %v", err)
		}

		value, ok := val.(float64)
		if !ok {
			t.Fatalf("Expected float64, got %T", val)
		}

		values = append(values, value)
		time.Sleep(10 * time.Millisecond)
	}

	// Should have both baseline (100) and burst (300) values
	hasBaseline := false
	hasBurst := false
	for _, v := range values {
		if v == 100.0 {
			hasBaseline = true
		}
		if v == 300.0 {
			hasBurst = true
		}
	}

	if !hasBaseline {
		t.Error("Square wave pattern should have baseline values")
	}
	if !hasBurst {
		t.Error("Square wave pattern should have burst values")
	}
}

func TestStressPatternGenerator_StateManagement(t *testing.T) {
	config := map[string]interface{}{
		"pattern": "sine_burst",
	}

	gen, err := NewStressPatternGenerator(config)
	if err != nil {
		t.Fatalf("Failed to create generator: %v", err)
	}

	// Test GetState before initialization
	state := gen.GetState()
	stateMap, ok := state.(map[string]interface{})
	if !ok {
		t.Fatalf("Expected map[string]interface{}, got %T", state)
	}

	if stateMap["pattern"] != "sine_burst" {
		t.Errorf("Expected pattern 'sine_burst', got %v", stateMap["pattern"])
	}

	// Test Reset
	err = gen.Reset()
	if err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	// State should still be accessible after reset
	state = gen.GetState()
	stateMap, ok = state.(map[string]interface{})
	if !ok {
		t.Fatalf("Expected map[string]interface{}, got %T", state)
	}

	if stateMap["pattern"] != "sine_burst" {
		t.Errorf("Expected pattern 'sine_burst' after reset, got %v", stateMap["pattern"])
	}
}

func TestStressPatternGenerator_Metadata(t *testing.T) {
	config := map[string]interface{}{
		"pattern": "sine_burst",
	}

	gen, err := NewStressPatternGenerator(config)
	if err != nil {
		t.Fatalf("Failed to create generator: %v", err)
	}

	metadata := gen.GetMetadata()

	if metadata.Name != "stress_pattern" {
		t.Errorf("Expected name 'stress_pattern', got %v", metadata.Name)
	}

	if !metadata.Capabilities.IsStateful {
		t.Error("Expected generator to be stateful")
	}

	if metadata.Capabilities.IsBinary {
		t.Error("Expected generator to not be binary")
	}

	if !metadata.Capabilities.IsContinuous {
		t.Error("Expected generator to be continuous")
	}
}
