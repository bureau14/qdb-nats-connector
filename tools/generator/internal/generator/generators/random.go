// Package generators provides concrete implementations of field generators.
// This package contains random generators for integers, floats, and strings
// with configurable ranges and characteristics.
package generators

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal"
	"github.com/bureau14/qdb-nats-connector/tools/generator/internal/generator"
)

// randomIntGenerator generates random integer values within a specified range.
// It maintains its own random number generator with seeded state.
type randomIntGenerator struct {
	min int64
	max int64
	rng *rand.Rand
}

// randomFloatGenerator generates random float values within a specified range.
// It maintains its own random number generator with seeded state.
type randomFloatGenerator struct {
	min float64
	max float64
	rng *rand.Rand
}

// randomStringGenerator generates random strings of specified length from a character set.
// It maintains its own random number generator with seeded state.
type randomStringGenerator struct {
	length  int
	charset string
	rng     *rand.Rand
}

// NewRandomIntGenerator creates a random integer generator from configuration.
// Supported config options:
//   - min: int minimum value (required)
//   - max: int maximum value (required)
func NewRandomIntGenerator(config map[string]interface{}) (*randomIntGenerator, error) {
	minVal, hasMin := getFloat64(config, "min")
	if !hasMin {
		return nil, fmt.Errorf("min value is required")
	}

	maxVal, hasMax := getFloat64(config, "max")
	if !hasMax {
		return nil, fmt.Errorf("max value is required")
	}

	minInt := int64(minVal)
	maxInt := int64(maxVal)

	if minInt >= maxInt {
		return nil, fmt.Errorf("min value must be less than max value")
	}

	return &randomIntGenerator{
		min: minInt,
		max: maxInt,
		rng: rand.New(rand.NewSource(time.Now().UnixNano())), //nolint:gosec // Non-crypto usage for data generation
	}, nil
}

// Generate returns a random int64 value in the range [min, max).
func (g *randomIntGenerator) Generate(ctx context.Context) (interface{}, error) {
	return g.min + g.rng.Int63n(g.max-g.min), nil
}

// NewRandomFloatGenerator creates a random float generator from configuration.
// Supported config options:
//   - min: float minimum value (required)
//   - max: float maximum value (required)
func NewRandomFloatGenerator(config map[string]interface{}) (*randomFloatGenerator, error) {
	minVal, hasMin := getFloat64(config, "min")
	if !hasMin {
		return nil, fmt.Errorf("min value is required")
	}

	maxVal, hasMax := getFloat64(config, "max")
	if !hasMax {
		return nil, fmt.Errorf("max value is required")
	}

	if minVal >= maxVal {
		return nil, fmt.Errorf("min value must be less than max value")
	}

	return &randomFloatGenerator{
		min: minVal,
		max: maxVal,
		rng: rand.New(rand.NewSource(time.Now().UnixNano())), //nolint:gosec // Non-crypto usage for data generation
	}, nil
}

// Generate returns a random float64 value in the range [min, max).
func (g *randomFloatGenerator) Generate(ctx context.Context) (interface{}, error) {
	return g.min + g.rng.Float64()*(g.max-g.min), nil
}

// NewRandomStringGenerator creates a random string generator from configuration.
// Supported config options:
//   - length: int string length (defaults to 10)
//   - charset: string character set to use (defaults to alphanumeric)
func NewRandomStringGenerator(config map[string]interface{}) (*randomStringGenerator, error) {
	length := 10
	if lengthVal, ok := config["length"].(int); ok {
		if lengthVal <= 0 {
			return nil, fmt.Errorf("length must be positive")
		}
		length = lengthVal
	}

	charset := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	if cs, ok := config["charset"].(string); ok {
		if cs == "" {
			return nil, fmt.Errorf("charset cannot be empty")
		}
		charset = cs
	}

	return &randomStringGenerator{
		length:  length,
		charset: charset,
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())), //nolint:gosec // Non-crypto usage for data generation
	}, nil
}

// Generate returns a random string of the configured length from the charset.
func (g *randomStringGenerator) Generate(ctx context.Context) (interface{}, error) {
	result := make([]byte, g.length)
	for i := range result {
		result[i] = g.charset[g.rng.Intn(len(g.charset))]
	}

	return string(result), nil
}

// init registers all random generators with the global registry.
func init() {
	generator.RegisterGenerator("random_int", func(config map[string]interface{}) (internal.FieldGenerator, error) {
		return NewRandomIntGenerator(config)
	})

	generator.RegisterGenerator("random_float", func(config map[string]interface{}) (internal.FieldGenerator, error) {
		return NewRandomFloatGenerator(config)
	})

	generator.RegisterGenerator("random_string", func(config map[string]interface{}) (internal.FieldGenerator, error) {
		return NewRandomStringGenerator(config)
	})
}
