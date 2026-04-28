// Package generators provides concrete implementations of field generators.
// This package contains the weighted_choice generator which selects from
// a set of values based on configured weights.
package generators

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal"
	"github.com/bureau14/qdb-nats-connector/tools/generator/internal/generator"
)

// Choice represents a single choice option with its value and weight.
type Choice struct {
	Value  interface{} `yaml:"value"`
	Weight float64     `yaml:"weight"`
}

// weightedChoiceGenerator generates values from a weighted set of choices.
// It uses cumulative weights for efficient O(log n) selection.
type weightedChoiceGenerator struct {
	choices           []Choice
	cumulativeWeights []float64
	totalWeight       float64
	rng               *rand.Rand
}

// NewWeightedChoiceGenerator creates a weighted choice generator from configuration.
// Supported config options:
//   - choices: array of choice objects, each with "value" and "weight" fields (required)
//
// Example configuration:
//
//	choices:
//	  - value: "DC1"
//	    weight: 60
//	  - value: "DC2"
//	    weight: 40
func NewWeightedChoiceGenerator(config map[string]interface{}) (*weightedChoiceGenerator, error) {
	choicesRaw, exists := config["choices"]
	if !exists {
		return nil, fmt.Errorf("weighted_choice generator requires 'choices' configuration parameter")
	}

	choicesSlice, ok := choicesRaw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("choices must be an array")
	}

	if len(choicesSlice) == 0 {
		return nil, fmt.Errorf("choices array cannot be empty")
	}

	choices := make([]Choice, 0, len(choicesSlice))
	cumulativeWeights := make([]float64, 0, len(choicesSlice))
	totalWeight := 0.0

	for i, choiceRaw := range choicesSlice {
		choiceMap, ok := choiceRaw.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("choice at index %d must be an object", i)
		}

		value, hasValue := choiceMap["value"]
		if !hasValue {
			return nil, fmt.Errorf("choice at index %d missing 'value' field", i)
		}

		weightRaw, hasWeight := choiceMap["weight"]
		if !hasWeight {
			return nil, fmt.Errorf("choice at index %d missing 'weight' field", i)
		}

		var weight float64
		switch w := weightRaw.(type) {
		case int:
			weight = float64(w)
		case float64:
			weight = w
		default:
			return nil, fmt.Errorf("weight at index %d must be a number", i)
		}

		if weight <= 0 {
			return nil, fmt.Errorf("weight at index %d must be positive", i)
		}

		choices = append(choices, Choice{
			Value:  value,
			Weight: weight,
		})

		totalWeight += weight
		cumulativeWeights = append(cumulativeWeights, totalWeight)
	}

	return &weightedChoiceGenerator{
		choices:           choices,
		cumulativeWeights: cumulativeWeights,
		totalWeight:       totalWeight,
		rng:               rand.New(rand.NewSource(time.Now().UnixNano())), //nolint:gosec // Non-crypto usage for data generation
	}, nil
}

// Generate returns a randomly selected value based on the configured weights.
// Values with higher weights are more likely to be selected.
func (g *weightedChoiceGenerator) Generate(ctx context.Context) (interface{}, error) {
	// Generate random value in [0, totalWeight)
	target := g.rng.Float64() * g.totalWeight

	// Find the first cumulative weight that is greater than target
	// This is equivalent to binary search but using linear scan for simplicity
	// since most use cases will have small numbers of choices
	for i, cumulativeWeight := range g.cumulativeWeights {
		if target < cumulativeWeight {
			return g.choices[i].Value, nil
		}
	}

	// Fallback to last choice (should not happen due to floating point precision)
	return g.choices[len(g.choices)-1].Value, nil
}

// init registers the weighted_choice generator with the global registry.
func init() {
	generator.RegisterGenerator("weighted_choice", func(config map[string]interface{}) (internal.FieldGenerator, error) {
		return NewWeightedChoiceGenerator(config)
	})
}
