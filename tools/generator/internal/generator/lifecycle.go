// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.

// Package generator: lifecycle interfaces for stateful generators
// Types: StatefulFieldGenerator, GeneratorState
// Ex: gen.Initialize(ctx) → prepares generator state
package generator

import (
	"context"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal"
)

// StatefulFieldGenerator extends FieldGenerator with lifecycle methods
// Provides state management for generators that maintain internal state
// In: context for all operations
// Ex: brownian motion, network burst patterns
type StatefulFieldGenerator interface {
	internal.FieldGenerator

	// Initialize prepares generator for use
	// Called once before first Generate call
	// In: context for initialization
	// Out: error if initialization fails
	// Ex: gen.Initialize(ctx) → loads initial state
	Initialize(ctx context.Context) error

	// Reset clears generator state
	// Returns generator to initial conditions
	// Out: error if reset fails
	// Ex: gen.Reset() → clears accumulated values
	Reset() error

	// GetState returns current generator state
	// Used for debugging and persistence
	// Out: state as interface{} (generator-specific)
	// Ex: state := gen.GetState() → current brownian value
	GetState() interface{}
}

// GeneratorCapabilities describes optional generator features
// Used by registry for feature detection
// Ex: caps.IsStateful → true for stateful generators
type GeneratorCapabilities struct {
	IsStateful   bool // Implements StatefulFieldGenerator
	IsBinary     bool // Produces binary data
	IsContinuous bool // Optimized for continuous mode
}

// GeneratorMetadata provides generator information
// Used for discovery and documentation
// Ex: meta.Description → "Generates random values"
type GeneratorMetadata struct {
	Name         string                // Generator type name
	Description  string                // Human-readable description
	Version      string                // Generator version
	Capabilities GeneratorCapabilities // Feature flags
}

// GeneratorState provides thread-safe state storage
// Base type for generator-specific state
// Ex: &GeneratorState{Data: map[string]float64{}}
type GeneratorState struct {
	Data interface{} // Generator-specific state data
}

// IsStatefulGenerator checks if generator implements state management
// In: generator instance
// Out: true if stateful, false otherwise
// Ex: if IsStatefulGenerator(gen) { gen.Initialize(ctx) }
func IsStatefulGenerator(gen internal.FieldGenerator) bool {
	_, ok := gen.(StatefulFieldGenerator)

	return ok
}

// InitializeIfStateful initializes generator if it supports state
// In: generator instance, context
// Out: error if initialization fails, nil for non-stateful
// Ex: InitializeIfStateful(gen, ctx) → initializes if needed
func InitializeIfStateful(gen internal.FieldGenerator, ctx context.Context) error {
	if stateful, ok := gen.(StatefulFieldGenerator); ok {
		return stateful.Initialize(ctx)
	}

	return nil
}
