// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.

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

// networkBurstGenerator simulates network traffic with bursts
// Generates baseline traffic with periodic spikes
// In: baseline, burst probability, multiplier, duration
// Ex: network bytes/sec with occasional 10x spikes
type networkBurstGenerator struct {
	// Configuration
	baseline         float64       // Normal traffic level
	burstProbability float64       // Chance of burst per generation
	burstMultiplier  float64       // Burst size factor
	burstDuration    time.Duration // How long bursts last

	// State
	inBurst      bool       // Currently in burst mode
	burstEndTime time.Time  // When current burst ends
	lastValue    float64    // Last generated value
	mu           sync.Mutex // Thread safety
	initialized  bool       // Initialization flag
	rng          *rand.Rand // Random number generator
}

// NewNetworkBurstGenerator creates network burst traffic generator
// Config options:
//   - baseline: normal traffic level (required)
//   - burst_probability: chance of burst [0-1] (default 0.05)
//   - burst_multiplier: burst size factor (default 10)
//   - burst_duration: burst length as duration string (default "30s")
//
// Ex: {"baseline": 1000000, "burst_probability": 0.1, "burst_multiplier": 50}
func NewNetworkBurstGenerator(config map[string]interface{}) (*networkBurstGenerator, error) {
	gen := &networkBurstGenerator{
		burstProbability: 0.05,                                            // 5% chance by default
		burstMultiplier:  10,                                              // 10x traffic by default
		burstDuration:    30 * time.Second,                                // 30 second bursts
		rng:              rand.New(rand.NewSource(time.Now().UnixNano())), //nolint:gosec // Non-crypto usage for data generation
	}

	// Parse baseline (required)
	baseline, ok := getFloat64(config, "baseline")
	if !ok || baseline <= 0 {
		return nil, fmt.Errorf("network_burst requires positive 'baseline' value")
	}
	gen.baseline = baseline
	gen.lastValue = baseline

	// Parse burst probability
	if prob, ok := getFloat64(config, "burst_probability"); ok {
		if prob < 0 || prob > 1 {
			return nil, fmt.Errorf("burst_probability must be between 0 and 1")
		}
		gen.burstProbability = prob
	}

	// Parse burst multiplier
	if mult, ok := getFloat64(config, "burst_multiplier"); ok {
		if mult <= 1 {
			return nil, fmt.Errorf("burst_multiplier must be greater than 1")
		}
		gen.burstMultiplier = mult
	}

	// Parse burst duration
	if durStr, ok := config["burst_duration"].(string); ok {
		dur, err := time.ParseDuration(durStr)
		if err != nil {
			return nil, fmt.Errorf("invalid burst_duration: %w", err)
		}
		if dur <= 0 {
			return nil, fmt.Errorf("burst_duration must be positive")
		}
		gen.burstDuration = dur
	}

	return gen, nil
}

// Initialize prepares generator state
func (g *networkBurstGenerator) Initialize(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.inBurst = false
	g.burstEndTime = time.Time{}
	g.lastValue = g.baseline
	g.initialized = true

	return nil
}

// Generate produces network traffic value with burst patterns
// Uses Poisson process for burst arrivals
// Adds noise to baseline for realistic variation
func (g *networkBurstGenerator) Generate(ctx context.Context) (interface{}, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.initialized {
		g.inBurst = false
		g.lastValue = g.baseline
		g.initialized = true
	}

	now := time.Now()

	// Check if burst should end
	if g.inBurst && now.After(g.burstEndTime) {
		g.inBurst = false
	}

	// Check if new burst should start (Poisson process)
	if !g.inBurst && g.rng.Float64() < g.burstProbability {
		g.inBurst = true
		g.burstEndTime = now.Add(g.burstDuration)
	}

	// Calculate base value
	var baseValue float64
	if g.inBurst {
		// During burst: elevated traffic with some variation
		baseValue = g.baseline * g.burstMultiplier
	} else {
		// Normal traffic
		baseValue = g.baseline
	}

	// Add realistic noise (±10% variation)
	noise := (g.rng.Float64() - 0.5) * 0.2 * baseValue
	value := baseValue + noise

	// Ensure non-negative
	value = math.Max(0, value)

	// Smooth transitions using exponential moving average
	alpha := 0.3 // Smoothing factor
	g.lastValue = alpha*value + (1-alpha)*g.lastValue

	return g.lastValue, nil
}

// Reset returns to baseline state
func (g *networkBurstGenerator) Reset() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.inBurst = false
	g.burstEndTime = time.Time{}
	g.lastValue = g.baseline

	return nil
}

// GetState returns current burst state
func (g *networkBurstGenerator) GetState() interface{} {
	g.mu.Lock()
	defer g.mu.Unlock()

	state := map[string]interface{}{
		"current_value": g.lastValue,
		"baseline":      g.baseline,
		"in_burst":      g.inBurst,
	}

	if g.inBurst {
		state["burst_remaining"] = time.Until(g.burstEndTime).Seconds()
	}

	return state
}

// Register the generator
func init() {
	generator.RegisterGenerator("network_burst", func(config map[string]interface{}) (internal.FieldGenerator, error) {
		return NewNetworkBurstGenerator(config)
	})
}
