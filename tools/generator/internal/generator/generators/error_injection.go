// Package generators provides concrete implementations of field generators.
// This package contains the error injection generator which combines normal
// value generation with configurable error injection based on probability.
package generators

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal"
	"github.com/bureau14/qdb-nats-connector/tools/generator/internal/generator"
)

// errorChoice represents a single error option with its value and weight.
type errorChoice struct {
	Value  interface{}
	Weight float64
}

// errorInjectionGenerator generates values that are either normal or error values
// based on a configured error rate probability. It uses weighted selection for
// error values when errors occur.
type errorInjectionGenerator struct {
	// normalGenerator produces normal values when no error is injected
	normalGenerator internal.FieldGenerator
	// errorRate is the probability (0.0 to 1.0) of generating an error value
	errorRate float64
	// errors contains the weighted error values to select from
	errors []errorChoice
	// cumulativeWeights for efficient O(log n) error selection
	cumulativeWeights []float64
	// totalWeight is the sum of all error weights
	totalWeight float64
	// rng provides random number generation for error probability
	rng *rand.Rand
}

// NewErrorInjectionGenerator creates an error injection generator from configuration.
// Supported config options:
//   - normal: configuration for the normal value generator (required)
//   - error_rate: float64 probability of error (0.0 to 1.0, required)
//   - errors: array of error choice objects, each with "value" and "weight" fields (required)
//
// Example configuration:
//
//	normal:
//	  type: random_float
//	  config:
//	    min: 20.0
//	    max: 25.0
//	error_rate: 0.1
//	errors:
//	  - value: "ERROR"
//	    weight: 0.7
//	  - value: "N/A"
//	    weight: 0.3
func NewErrorInjectionGenerator(config map[string]interface{}) (*errorInjectionGenerator, error) {
	// Parse normal generator configuration
	normalConfig, hasNormal := config["normal"]
	if !hasNormal {
		return nil, fmt.Errorf("error_injection generator requires 'normal' configuration parameter")
	}

	normalConfigMap, ok := normalConfig.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("normal configuration must be an object")
	}

	normalType, hasType := normalConfigMap["type"].(string)
	if !hasType {
		return nil, fmt.Errorf("normal generator configuration must specify 'type'")
	}

	normalGenConfig, hasConfig := normalConfigMap["config"]
	if !hasConfig {
		normalGenConfig = map[string]interface{}{}
	}
	normalGenConfigMap, ok := normalGenConfig.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("normal generator config must be an object")
	}

	// Create the normal generator
	normalGenInstance, err := generator.CreateGenerator(context.Background(), normalType, normalGenConfigMap)
	if err != nil {
		return nil, fmt.Errorf("failed to create normal generator: %w", err)
	}

	// Parse error rate
	errorRateRaw, hasErrorRate := config["error_rate"]
	if !hasErrorRate {
		return nil, fmt.Errorf("error_injection generator requires 'error_rate' configuration parameter")
	}

	errorRate, ok := getFloatFromInterface(errorRateRaw)
	if !ok {
		return nil, fmt.Errorf("error_rate must be a number")
	}

	if errorRate < 0.0 || errorRate > 1.0 {
		return nil, fmt.Errorf("error_rate must be between 0.0 and 1.0")
	}

	// Parse error choices
	errorsRaw, hasErrors := config["errors"]
	if !hasErrors {
		return nil, fmt.Errorf("error_injection generator requires 'errors' configuration parameter")
	}

	errorsSlice, ok := errorsRaw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("errors must be an array")
	}

	if len(errorsSlice) == 0 {
		return nil, fmt.Errorf("errors array cannot be empty")
	}

	errors := make([]errorChoice, 0, len(errorsSlice))
	cumulativeWeights := make([]float64, 0, len(errorsSlice))
	totalWeight := 0.0

	for i, errorRaw := range errorsSlice {
		errorMap, ok := errorRaw.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("error at index %d must be an object", i)
		}

		value, hasValue := errorMap["value"]
		if !hasValue {
			return nil, fmt.Errorf("error at index %d missing 'value' field", i)
		}

		weightRaw, hasWeight := errorMap["weight"]
		if !hasWeight {
			return nil, fmt.Errorf("error at index %d missing 'weight' field", i)
		}

		weight, ok := getFloatFromInterface(weightRaw)
		if !ok {
			return nil, fmt.Errorf("weight at index %d must be a number", i)
		}

		if weight <= 0 {
			return nil, fmt.Errorf("weight at index %d must be positive", i)
		}

		errors = append(errors, errorChoice{
			Value:  value,
			Weight: weight,
		})

		totalWeight += weight
		cumulativeWeights = append(cumulativeWeights, totalWeight)
	}

	return &errorInjectionGenerator{
		normalGenerator:   normalGenInstance.GetGenerator(),
		errorRate:         errorRate,
		errors:            errors,
		cumulativeWeights: cumulativeWeights,
		totalWeight:       totalWeight,
		rng:               rand.New(rand.NewSource(time.Now().UnixNano())), //nolint:gosec // Non-crypto usage for data generation
	}, nil
}

// Generate returns either a normal value or an error value based on the configured
// error rate probability. When an error is generated, it selects from the weighted
// error choices.
func (g *errorInjectionGenerator) Generate(ctx context.Context) (interface{}, error) {
	// Decide whether to generate error or normal value
	if g.rng.Float64() < g.errorRate {
		// Generate error value using weighted selection
		target := g.rng.Float64() * g.totalWeight

		// Find the first cumulative weight that is greater than target
		for i, cumulativeWeight := range g.cumulativeWeights {
			if target < cumulativeWeight {
				return g.errors[i].Value, nil
			}
		}

		// Fallback to last error choice (should not happen due to floating point precision)
		return g.errors[len(g.errors)-1].Value, nil
	}

	// Generate normal value
	return g.normalGenerator.Generate(ctx)
}

// init registers the error_injection generator with the global registry.
func init() {
	generator.RegisterGenerator("error_injection", func(config map[string]interface{}) (internal.FieldGenerator, error) {
		return NewErrorInjectionGenerator(config)
	})
}
