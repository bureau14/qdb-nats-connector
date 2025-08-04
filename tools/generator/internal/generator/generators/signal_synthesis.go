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

// signalSynthesisGenerator creates waveforms using Fourier synthesis
// Generates realistic signal data for electrical/mechanical sensors
// In: frequency, harmonics, amplitudes, noise level, sample rate
// Ex: 50Hz power signal with harmonics at 150, 250, 350Hz
type signalSynthesisGenerator struct {
	// Configuration
	frequency  float64   // Base frequency in Hz
	harmonics  []float64 // Harmonic frequencies (multipliers)
	amplitudes []float64 // Amplitude for each harmonic
	noiseLevel float64   // Noise amplitude (0-1)
	sampleRate float64   // Samples per second

	// State
	currentTime float64    // Current time position
	timeStep    float64    // Time increment per sample
	mu          sync.Mutex // Thread safety
	initialized bool       // Initialization flag
	rng         *rand.Rand // Random number generator
}

// NewSignalSynthesisGenerator creates waveform generator
// Config options:
//   - frequency: base frequency in Hz (required)
//   - harmonics: array of harmonic multipliers (default: none)
//   - amplitudes: array of amplitudes for each harmonic (default: 1.0)
//   - noise_level: noise amplitude 0-1 (default: 0.05)
//   - sample_rate: samples per second (default: 1000)
//
// Ex: {"frequency": 50, "harmonics": [3, 5, 7], "amplitudes": [1.0, 0.3, 0.1]}
func NewSignalSynthesisGenerator(config map[string]interface{}) (*signalSynthesisGenerator, error) {
	gen := &signalSynthesisGenerator{
		noiseLevel: 0.05,                                            // 5% noise by default
		sampleRate: 1000.0,                                          // 1kHz default sample rate
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())), //nolint:gosec // Non-crypto usage for data generation
	}

	// Parse base frequency (required)
	freq, ok := getFloat64(config, "frequency")
	if !ok || freq <= 0 {
		return nil, fmt.Errorf("signal_synthesis requires positive 'frequency'")
	}
	gen.frequency = freq

	// Parse harmonics
	if harmonicsRaw, ok := config["harmonics"].([]interface{}); ok {
		for _, h := range harmonicsRaw {
			if harmonic, ok := getFloatFromInterface(h); ok && harmonic > 0 {
				gen.harmonics = append(gen.harmonics, harmonic)
			}
		}
	}

	// Parse amplitudes
	if amplitudesRaw, ok := config["amplitudes"].([]interface{}); ok {
		for _, a := range amplitudesRaw {
			if amplitude, ok := getFloatFromInterface(a); ok {
				gen.amplitudes = append(gen.amplitudes, amplitude)
			}
		}
	}

	// Ensure amplitudes match harmonics
	if len(gen.harmonics) > 0 {
		// Pad or trim amplitudes to match harmonics
		for len(gen.amplitudes) < len(gen.harmonics)+1 {
			gen.amplitudes = append(gen.amplitudes, 1.0)
		}
		gen.amplitudes = gen.amplitudes[:len(gen.harmonics)+1]
	} else if len(gen.amplitudes) == 0 {
		// No harmonics, just fundamental
		gen.amplitudes = []float64{1.0}
	}

	// Parse noise level
	if noise, ok := getFloat64(config, "noise_level"); ok {
		if noise < 0 || noise > 1 {
			return nil, fmt.Errorf("noise_level must be between 0 and 1")
		}
		gen.noiseLevel = noise
	}

	// Parse sample rate
	if rate, ok := getFloat64(config, "sample_rate"); ok && rate > 0 {
		gen.sampleRate = rate
	}

	// Calculate time step
	gen.timeStep = 1.0 / gen.sampleRate

	return gen, nil
}

// Initialize prepares generator state
func (g *signalSynthesisGenerator) Initialize(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.currentTime = 0
	g.initialized = true

	return nil
}

// Generate produces next waveform sample
// Uses Fourier synthesis: sum of sinusoids at different frequencies
// Adds white noise for realism
func (g *signalSynthesisGenerator) Generate(ctx context.Context) (interface{}, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.initialized {
		g.currentTime = 0
		g.initialized = true
	}

	// Start with fundamental frequency
	signal := g.amplitudes[0] * math.Sin(2*math.Pi*g.frequency*g.currentTime)

	// Add harmonics
	for i, harmonic := range g.harmonics {
		freq := g.frequency * harmonic
		amplitude := g.amplitudes[i+1]
		signal += amplitude * math.Sin(2*math.Pi*freq*g.currentTime)
	}

	// Add noise
	if g.noiseLevel > 0 {
		noise := (g.rng.Float64() - 0.5) * 2 * g.noiseLevel
		signal += noise
	}

	// Apply anti-aliasing (simple low-pass filter)
	// Nyquist frequency is sampleRate/2
	nyquist := g.sampleRate / 2
	maxFreq := g.frequency
	if len(g.harmonics) > 0 {
		maxFreq = g.frequency * g.harmonics[len(g.harmonics)-1]
	}

	// If signal contains frequencies above Nyquist, attenuate
	if maxFreq > nyquist {
		// Simple attenuation factor
		attenuation := nyquist / maxFreq
		signal *= attenuation
	}

	// Advance time
	g.currentTime += g.timeStep

	// Prevent time overflow (wrap around after 1 hour)
	if g.currentTime > 3600 {
		g.currentTime -= 3600
	}

	return signal, nil
}

// Reset returns to initial state
func (g *signalSynthesisGenerator) Reset() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.currentTime = 0

	return nil
}

// GetState returns current generator state
func (g *signalSynthesisGenerator) GetState() interface{} {
	g.mu.Lock()
	defer g.mu.Unlock()

	return map[string]interface{}{
		"current_time": g.currentTime,
		"frequency":    g.frequency,
		"sample_rate":  g.sampleRate,
		"harmonics":    g.harmonics,
	}
}

// GetMetadata returns generator metadata
func (g *signalSynthesisGenerator) GetMetadata() generator.GeneratorMetadata {
	return generator.GeneratorMetadata{
		Name:        "signal_synthesis",
		Description: "Fourier synthesis waveform generator",
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
	generator.RegisterGenerator("signal_synthesis", func(config map[string]interface{}) (internal.FieldGenerator, error) {
		return NewSignalSynthesisGenerator(config)
	})
}
