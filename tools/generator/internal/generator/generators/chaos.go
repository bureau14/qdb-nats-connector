// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.

package generators

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal"
	"github.com/bureau14/qdb-nats-connector/tools/generator/internal/generator"
)

// chaosMode defines a type of chaos injection
type chaosMode struct {
	Type        string  // Mode type
	Probability float64 // Probability of occurrence
}

// chaosGenerator injects failures and anomalies
// Creates null values, malformed data, extreme values
// In: modes with probabilities, base generator
// Ex: 1% null, 0.1% malformed, 5% extreme values
type chaosGenerator struct {
	// Configuration
	modes         []chaosMode                  // Chaos modes to apply
	baseGenerator *generator.GeneratorInstance // Base generator for normal values

	// State
	nullCount      int64      // Count of null injections
	malformedCount int64      // Count of malformed data
	extremeCount   int64      // Count of extreme values
	normalCount    int64      // Count of normal values
	mu             sync.Mutex // Thread safety
}

// NewChaosGenerator creates failure injection generator
// Config options:
//   - modes: array of chaos mode definitions (required)
//   - base: base generator config for normal values (optional)
//
// Ex: {"modes": [{"type": "null_injection", "probability": 0.01}]}
func NewChaosGenerator(config map[string]interface{}) (*chaosGenerator, error) {
	gen := &chaosGenerator{}

	// Parse modes (required)
	modesRaw, ok := config["modes"].([]interface{})
	if !ok || len(modesRaw) == 0 {
		return nil, fmt.Errorf("chaos requires 'modes' array")
	}

	// Parse each mode
	for i, modeRaw := range modesRaw {
		modeMap, ok := modeRaw.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("mode %d is not a map", i)
		}

		mode := chaosMode{}

		// Parse type
		modeType, ok := modeMap["type"].(string)
		if !ok {
			return nil, fmt.Errorf("mode %d requires 'type'", i)
		}

		switch modeType {
		case "null_injection", "malformed_data", "extreme_values":
			mode.Type = modeType
		default:
			return nil, fmt.Errorf("unknown chaos mode: %s", modeType)
		}

		// Parse probability
		prob, ok := getFloat64(modeMap, "probability")
		if !ok || prob < 0 || prob > 1 {
			return nil, fmt.Errorf("mode %d requires 'probability' between 0 and 1", i)
		}
		mode.Probability = prob

		gen.modes = append(gen.modes, mode)
	}

	// Parse base generator (optional)
	err := gen.parseBaseGenerator(config)
	if err != nil {
		return nil, err
	}

	return gen, nil
}

// Initialize prepares generator state
func (g *chaosGenerator) Initialize(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.nullCount = 0
	g.malformedCount = 0
	g.extremeCount = 0
	g.normalCount = 0

	// Initialize base generator if stateful
	return generator.InitializeIfStateful(g.baseGenerator.GetGenerator(), ctx)
}

// Generate produces value with chaos injection
// Randomly selects chaos mode based on probabilities
func (g *chaosGenerator) Generate(ctx context.Context) (interface{}, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Check each chaos mode
	for _, mode := range g.modes {
		//nolint:gosec // Non-crypto usage for data generation
		if rand.Float64() < mode.Probability {
			switch mode.Type {
			case "null_injection":
				g.nullCount++

				return nil, nil // Return null value

			case "malformed_data":
				g.malformedCount++

				return g.generateMalformed()

			case "extreme_values":
				g.extremeCount++

				return g.generateExtreme()
			}
		}
	}

	// Normal value from base generator
	g.normalCount++

	return g.baseGenerator.Generate(ctx)
}

// Reset returns to initial state
func (g *chaosGenerator) Reset() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.nullCount = 0
	g.malformedCount = 0
	g.extremeCount = 0
	g.normalCount = 0

	// Reset base generator if stateful
	if stateful, ok := g.baseGenerator.GetGenerator().(generator.StatefulFieldGenerator); ok {
		return stateful.Reset()
	}

	return nil
}

// GetState returns current generator state
func (g *chaosGenerator) GetState() interface{} {
	g.mu.Lock()
	defer g.mu.Unlock()

	total := g.nullCount + g.malformedCount + g.extremeCount + g.normalCount

	state := map[string]interface{}{
		"null_count":      g.nullCount,
		"malformed_count": g.malformedCount,
		"extreme_count":   g.extremeCount,
		"normal_count":    g.normalCount,
		"total_count":     total,
	}

	// Add percentages if total > 0
	if total > 0 {
		state["null_percentage"] = float64(g.nullCount) / float64(total) * 100
		state["malformed_percentage"] = float64(g.malformedCount) / float64(total) * 100
		state["extreme_percentage"] = float64(g.extremeCount) / float64(total) * 100
		state["normal_percentage"] = float64(g.normalCount) / float64(total) * 100
	}

	return state
}

// GetMetadata returns generator metadata
func (g *chaosGenerator) GetMetadata() generator.GeneratorMetadata {
	return generator.GeneratorMetadata{
		Name:        "chaos",
		Description: "Failure injection generator for resilience testing",
		Version:     "1.0.0",
		Capabilities: generator.GeneratorCapabilities{
			IsStateful:   true,
			IsBinary:     false,
			IsContinuous: true,
		},
	}
}

// parseBaseGenerator parses the base generator configuration
func (g *chaosGenerator) parseBaseGenerator(config map[string]interface{}) error {
	baseConfig, ok := config["base"].(map[string]interface{})
	if !ok {
		// Default base generator
		return g.createDefaultBaseGenerator()
	}

	baseType, ok := baseConfig["type"].(string)
	if !ok {
		baseType = "random_float" // Default base type
	}

	baseConfigMap, _ := baseConfig["config"].(map[string]interface{})
	if baseConfigMap == nil {
		baseConfigMap = map[string]interface{}{
			"min": 0.0,
			"max": 100.0,
		}
	}

	baseGen, err := generator.CreateGenerator(context.Background(), baseType, baseConfigMap)
	if err != nil {
		return fmt.Errorf("failed to create base generator: %w", err)
	}
	g.baseGenerator = baseGen

	return nil
}

// createDefaultBaseGenerator creates a default base generator
func (g *chaosGenerator) createDefaultBaseGenerator() error {
	baseGen, err := generator.CreateGenerator(context.Background(), "random_float", map[string]interface{}{
		"min": 0.0,
		"max": 100.0,
	})
	if err != nil {
		return fmt.Errorf("failed to create default base generator: %w", err)
	}
	g.baseGenerator = baseGen

	return nil
}

// generateMalformed creates malformed data
func (g *chaosGenerator) generateMalformed() (interface{}, error) {
	// Various types of malformed data
	malformedTypes := []interface{}{
		"NaN",                          // Not a number string
		math.NaN(),                     // Actual NaN
		math.Inf(1),                    // Positive infinity
		math.Inf(-1),                   // Negative infinity
		"undefined",                    // Undefined string
		"null",                         // Null string (not actual null)
		"",                             // Empty string
		"�����",                        // Invalid UTF-8
		map[string]interface{}{},       // Empty object
		[]interface{}{},                // Empty array
		"\x00\x01\x02",                 // Binary data
		"9999999999999999999999999999", // Number too large
	}

	// Pick random malformed value
	//nolint:gosec // Non-crypto usage for data generation
	idx := rand.Intn(len(malformedTypes))

	return malformedTypes[idx], nil
}

// generateExtreme creates extreme values
func (g *chaosGenerator) generateExtreme() (interface{}, error) {
	// Various extreme values
	extremeValues := []interface{}{
		math.MaxFloat64,             // Maximum float
		-math.MaxFloat64,            // Minimum float
		math.SmallestNonzeroFloat64, // Smallest positive
		1e308,                       // Very large
		-1e308,                      // Very negative
		0.0,                         // Zero
		math.Copysign(0, -1),        // Negative zero
		1e-308,                      // Very small positive
		-1e-308,                     // Very small negative
		int64(math.MaxInt64),        // Maximum integer
		int64(math.MinInt64),        // Minimum integer
	}

	// Pick random extreme value
	//nolint:gosec // Non-crypto usage for data generation
	idx := rand.Intn(len(extremeValues))

	return extremeValues[idx], nil
}

// Register the generator
func init() {
	generator.RegisterGenerator("chaos", func(config map[string]interface{}) (internal.FieldGenerator, error) {
		return NewChaosGenerator(config)
	})
}
