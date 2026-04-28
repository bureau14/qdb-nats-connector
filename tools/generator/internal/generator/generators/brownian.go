// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.

// Package generators: Brownian motion generator for realistic sensor/financial data patterns
// Types: brownianMotionGenerator
// Ex: config{"base": 22.5, "volatility": 0.5, "bounds": [20, 25]}
package generators

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal"
	"github.com/bureau14/qdb-nats-connector/tools/generator/internal/generator"
)

// brownianMotionGenerator simulates Wiener process with bounds
// Generates realistic drifting values for sensor/financial data
// In: base value, volatility, bounds, optional drift
// Ex: temperature sensor drifting around 22.5°C
type brownianMotionGenerator struct {
	// Configuration
	base       float64 // Starting/equilibrium value
	volatility float64 // Standard deviation of changes
	minBound   float64 // Lower bound (reflection)
	maxBound   float64 // Upper bound (reflection)
	drift      float64 // Optional drift coefficient

	// State
	currentValue float64    // Current position
	mu           sync.Mutex // Thread safety
	initialized  bool       // Initialization flag
	rng          *rand.Rand // Random number generator
}

// NewBrownianMotionGenerator creates Brownian motion generator
// Config options:
//   - base: starting value (required)
//   - volatility: change magnitude (default 0.1)
//   - bounds: [min, max] array (optional)
//   - drift: trend coefficient (default 0.0)
//
// Ex: {"base": 22.5, "volatility": 0.5, "bounds": [20, 25]}
func NewBrownianMotionGenerator(config map[string]interface{}) (*brownianMotionGenerator, error) {
	gen := &brownianMotionGenerator{
		volatility: 0.1, // Default volatility
		drift:      0.0, // No drift by default
		minBound:   math.Inf(-1),
		maxBound:   math.Inf(1),
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())), //nolint:gosec // Non-crypto usage for data generation
	}

	// Parse base value (required)
	base, ok := getFloat64(config, "base")
	if !ok {
		return nil, fmt.Errorf("brownian_motion requires 'base' value")
	}
	gen.base = base
	gen.currentValue = base

	// Parse volatility
	if vol, ok := getFloat64(config, "volatility"); ok {
		if vol <= 0 {
			return nil, fmt.Errorf("volatility must be positive")
		}
		gen.volatility = vol
	}

	// Parse bounds
	if bounds, ok := config["bounds"].([]interface{}); ok && len(bounds) == 2 {
		if minVal, ok := getFloatFromInterface(bounds[0]); ok {
			gen.minBound = minVal
		}
		if maxVal, ok := getFloatFromInterface(bounds[1]); ok {
			gen.maxBound = maxVal
		}

		if gen.minBound >= gen.maxBound {
			return nil, fmt.Errorf("invalid bounds: min must be less than max")
		}

		// Ensure base is within bounds
		if gen.base < gen.minBound || gen.base > gen.maxBound {
			return nil, fmt.Errorf("base value %.2f outside bounds [%.2f, %.2f]",
				gen.base, gen.minBound, gen.maxBound)
		}
	}

	// Parse drift
	if drift, ok := getFloat64(config, "drift"); ok {
		gen.drift = drift
	}

	return gen, nil
}

// Initialize prepares generator state
func (g *brownianMotionGenerator) Initialize(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.currentValue = g.base
	g.initialized = true

	return nil
}

// Generate produces next value using Wiener process
// Uses Box-Muller transform for normal distribution
// Reflects at boundaries to maintain bounds
func (g *brownianMotionGenerator) Generate(ctx context.Context) (interface{}, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.initialized {
		g.currentValue = g.base
		g.initialized = true
	}

	// Generate normal random variable
	z := g.rng.NormFloat64()

	// Calculate change: dW = volatility * sqrt(dt) * Z + drift * dt
	// Using dt = 1 for simplicity
	change := g.volatility*z + g.drift

	// Update value
	newValue := g.currentValue + change

	// Apply boundary reflection
	if newValue < g.minBound {
		newValue = g.minBound + (g.minBound - newValue)
	} else if newValue > g.maxBound {
		newValue = g.maxBound - (newValue - g.maxBound)
	}

	// Ensure still within bounds after reflection
	newValue = math.Max(g.minBound, math.Min(g.maxBound, newValue))

	g.currentValue = newValue

	return newValue, nil
}

// Reset returns to base value
func (g *brownianMotionGenerator) Reset() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.currentValue = g.base

	return nil
}

// GetState returns current value
func (g *brownianMotionGenerator) GetState() interface{} {
	g.mu.Lock()
	defer g.mu.Unlock()

	return map[string]interface{}{
		"current_value": g.currentValue,
		"base":          g.base,
		"volatility":    g.volatility,
		"drift":         g.drift,
	}
}

// Register the generator
func init() {
	generator.RegisterGenerator("brownian_motion", func(config map[string]interface{}) (internal.FieldGenerator, error) {
		return NewBrownianMotionGenerator(config)
	})
}
