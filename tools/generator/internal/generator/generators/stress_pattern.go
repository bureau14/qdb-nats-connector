// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.

package generators

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal"
	"github.com/bureau14/qdb-nats-connector/tools/generator/internal/generator"
)

// stressPatternGenerator creates load testing patterns
// Generates sine burst, square wave, sawtooth patterns
// In: pattern type, period, burst factor, baseline rate
// Ex: sine burst with 100x spikes every minute
type stressPatternGenerator struct {
	// Configuration
	pattern      string        // Pattern type: sine_burst, square_wave, sawtooth
	period       time.Duration // Pattern period
	burstFactor  float64       // Multiplier during burst
	baselineRate float64       // Normal rate value

	// State
	startTime   time.Time  // Pattern start time
	mu          sync.Mutex // Thread safety
	initialized bool       // Initialization flag
}

// NewStressPatternGenerator creates load pattern generator
// Config options:
//   - pattern: "sine_burst", "square_wave", "sawtooth" (required)
//   - period: duration string (default "1m")
//   - burst_factor: multiplier during peak (default 100)
//   - baseline_rate: normal load value (default 1000)
//
// Ex: {"pattern": "sine_burst", "period": "30s", "burst_factor": 50}
func NewStressPatternGenerator(config map[string]interface{}) (*stressPatternGenerator, error) {
	gen := &stressPatternGenerator{
		period:       time.Minute, // 1 minute default
		burstFactor:  100,         // 100x default
		baselineRate: 1000,        // 1000 msgs/sec default
	}

	// Parse pattern type (required)
	pattern, ok := config["pattern"].(string)
	if !ok {
		return nil, fmt.Errorf("stress_pattern requires 'pattern' type")
	}

	switch pattern {
	case "sine_burst", "square_wave", "sawtooth":
		gen.pattern = pattern
	default:
		return nil, fmt.Errorf("unknown pattern type: %s (valid: sine_burst, square_wave, sawtooth)", pattern)
	}

	// Parse period
	if periodStr, ok := config["period"].(string); ok {
		period, err := time.ParseDuration(periodStr)
		if err != nil {
			return nil, fmt.Errorf("invalid period: %w", err)
		}
		if period <= 0 {
			return nil, fmt.Errorf("period must be positive")
		}
		gen.period = period
	}

	// Parse burst factor
	if factor, ok := getFloat64(config, "burst_factor"); ok {
		if factor <= 1 {
			return nil, fmt.Errorf("burst_factor must be greater than 1")
		}
		gen.burstFactor = factor
	}

	// Parse baseline rate
	if rate, ok := getFloat64(config, "baseline_rate"); ok {
		if rate <= 0 {
			return nil, fmt.Errorf("baseline_rate must be positive")
		}
		gen.baselineRate = rate
	}

	return gen, nil
}

// Initialize prepares generator state
func (g *stressPatternGenerator) Initialize(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.startTime = time.Now()
	g.initialized = true

	return nil
}

// Generate produces load pattern value
// Calculates position in pattern cycle and returns appropriate value
func (g *stressPatternGenerator) Generate(ctx context.Context) (interface{}, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.initialized {
		g.startTime = time.Now()
		g.initialized = true
	}

	// Calculate position in pattern cycle (0.0 to 1.0)
	elapsed := time.Since(g.startTime)
	position := math.Mod(elapsed.Seconds(), g.period.Seconds()) / g.period.Seconds()

	var value float64

	switch g.pattern {
	case "sine_burst":
		// Sine wave with bursts at peaks
		// Use squared sine for sharper peaks
		sineValue := math.Sin(2 * math.Pi * position)
		if sineValue > 0 {
			// Positive half: create burst
			squaredSine := sineValue * sineValue
			value = g.baselineRate + (g.burstFactor-1)*g.baselineRate*squaredSine
		} else {
			// Negative half: stay at baseline
			value = g.baselineRate
		}

	case "square_wave":
		// Square wave: alternating high/low
		if position < 0.5 {
			// First half: burst
			value = g.baselineRate * g.burstFactor
		} else {
			// Second half: baseline
			value = g.baselineRate
		}

	case "sawtooth":
		// Linear ramp up, then drop
		if position < 0.9 {
			// Ramp up over 90% of period
			rampFactor := position / 0.9
			value = g.baselineRate + (g.burstFactor-1)*g.baselineRate*rampFactor
		} else {
			// Quick drop in last 10%
			dropFactor := (1.0 - position) / 0.1
			value = g.baselineRate + (g.burstFactor-1)*g.baselineRate*dropFactor
		}

	default:
		value = g.baselineRate
	}

	// Ensure non-negative
	value = math.Max(0, value)

	return value, nil
}

// Reset returns to initial state
func (g *stressPatternGenerator) Reset() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.startTime = time.Now()

	return nil
}

// GetState returns current generator state
func (g *stressPatternGenerator) GetState() interface{} {
	g.mu.Lock()
	defer g.mu.Unlock()

	elapsed := time.Since(g.startTime)
	position := math.Mod(elapsed.Seconds(), g.period.Seconds()) / g.period.Seconds()

	return map[string]interface{}{
		"pattern":       g.pattern,
		"period":        g.period.String(),
		"burst_factor":  g.burstFactor,
		"baseline_rate": g.baselineRate,
		"position":      position,
		"elapsed":       elapsed.String(),
	}
}

// GetMetadata returns generator metadata
func (g *stressPatternGenerator) GetMetadata() generator.GeneratorMetadata {
	return generator.GeneratorMetadata{
		Name:        "stress_pattern",
		Description: "Load testing pattern generator",
		Version:     "1.0.0",
		Capabilities: generator.GeneratorCapabilities{
			IsStateful:   true,
			IsBinary:     false,
			IsContinuous: true,
		},
	}
}

// Register the generator
func init() {
	generator.RegisterGenerator("stress_pattern", func(config map[string]interface{}) (internal.FieldGenerator, error) {
		return NewStressPatternGenerator(config)
	})
}
