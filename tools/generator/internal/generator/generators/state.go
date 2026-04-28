// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.

package generators

import (
	"context"
	"sync"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal"
	"github.com/bureau14/qdb-nats-connector/tools/generator/internal/generator"
)

// StateManager coordinates state across multiple generators
// Provides centralized state management for complex scenarios
// In: map of generators to manage
// Ex: manager := NewStateManager(generators)
type StateManager struct {
	generators map[string]*generator.GeneratorInstance
	states     map[string]interface{}
	mu         sync.RWMutex
}

// NewStateManager creates state management coordinator
// In: generators to manage
// Out: initialized StateManager
// Ex: NewStateManager(gens) → manager for state coordination
func NewStateManager(generators map[string]*generator.GeneratorInstance) *StateManager {
	sm := &StateManager{
		generators: make(map[string]*generator.GeneratorInstance),
		states:     make(map[string]interface{}),
	}

	// Copy generator instances
	for name, genInstance := range generators {
		sm.generators[name] = genInstance
	}

	return sm
}

// InitializeAll initializes all stateful generators
// In: context for initialization
// Out: error if any initialization fails
// Ex: sm.InitializeAll(ctx) → prepares all generators
func (sm *StateManager) InitializeAll(ctx context.Context) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for name, genInstance := range sm.generators {
		// Get underlying generator and check if it's stateful
		gen := genInstance.GetGenerator()
		if stateful, ok := gen.(generator.StatefulFieldGenerator); ok {
			err := stateful.Initialize(ctx)
			if err != nil {
				return err
			}
			// Capture initial state
			sm.states[name] = stateful.GetState()
		}
	}

	return nil
}

// ResetAll resets all stateful generators
// Out: error if any reset fails
// Ex: sm.ResetAll() → returns all to initial state
func (sm *StateManager) ResetAll() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for name, genInstance := range sm.generators {
		gen := genInstance.GetGenerator()
		if stateful, ok := gen.(generator.StatefulFieldGenerator); ok {
			err := stateful.Reset()
			if err != nil {
				return err
			}
			// Update tracked state
			sm.states[name] = stateful.GetState()
		}
	}

	return nil
}

// GetAllStates returns current state snapshot
// Out: map of generator states
// Ex: states := sm.GetAllStates() → {"temp": {...}, "burst": {...}}
func (sm *StateManager) GetAllStates() map[string]interface{} {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	snapshot := make(map[string]interface{})
	for name, genInstance := range sm.generators {
		gen := genInstance.GetGenerator()
		if stateful, ok := gen.(generator.StatefulFieldGenerator); ok {
			snapshot[name] = stateful.GetState()
		}
	}

	return snapshot
}

// GetGeneratorState returns state for specific generator
// In: generator name
// Out: state if stateful, nil otherwise
// Ex: state := sm.GetGeneratorState("temperature")
func (sm *StateManager) GetGeneratorState(name string) interface{} {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if genInstance, ok := sm.generators[name]; ok {
		gen := genInstance.GetGenerator()
		if stateful, ok := gen.(generator.StatefulFieldGenerator); ok {
			return stateful.GetState()
		}
	}

	return nil
}

// UpdateState updates tracked state after generation
// In: generator name
// Out: none
// Ex: sm.UpdateState("temperature") → refreshes state cache
func (sm *StateManager) UpdateState(name string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if genInstance, ok := sm.generators[name]; ok {
		gen := genInstance.GetGenerator()
		if stateful, ok := gen.(generator.StatefulFieldGenerator); ok {
			sm.states[name] = stateful.GetState()
		}
	}
}

// StatefulGeneratorWrapper wraps any generator with state tracking
// Useful for adding state to stateless generators
// Ex: wrapped := NewStatefulWrapper(gen, initialState)
type StatefulGeneratorWrapper struct {
	inner internal.FieldGenerator
	state interface{}
	mu    sync.RWMutex
}

// NewStatefulWrapper creates wrapper with state tracking
// In: generator to wrap, initial state
// Out: wrapped generator with state
// Ex: NewStatefulWrapper(gen, 0) → adds counter state
func NewStatefulWrapper(gen internal.FieldGenerator, initialState interface{}) *StatefulGeneratorWrapper {
	return &StatefulGeneratorWrapper{
		inner: gen,
		state: initialState,
	}
}

// Generate delegates to inner generator
func (w *StatefulGeneratorWrapper) Generate(ctx context.Context) (interface{}, error) {
	return w.inner.Generate(ctx)
}

// Initialize prepares wrapper state
func (w *StatefulGeneratorWrapper) Initialize(ctx context.Context) error {
	// Initialize inner if stateful
	return generator.InitializeIfStateful(w.inner, ctx)
}

// Reset clears wrapper state
func (w *StatefulGeneratorWrapper) Reset() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Reset inner if stateful
	if stateful, ok := w.inner.(generator.StatefulFieldGenerator); ok {
		return stateful.Reset()
	}

	return nil
}

// GetState returns wrapper state
func (w *StatefulGeneratorWrapper) GetState() interface{} {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.state
}

// SetState updates wrapper state
// In: new state value
// Ex: wrapper.SetState(42) → updates tracked state
func (w *StatefulGeneratorWrapper) SetState(state interface{}) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.state = state
}
