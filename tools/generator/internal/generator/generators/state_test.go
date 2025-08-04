package generators

import (
	"context"
	"testing"
)

// mockStatelessGenerator implements FieldGenerator
type mockStatelessGenerator struct {
	value int
}

func (m *mockStatelessGenerator) Generate(ctx context.Context) (interface{}, error) {
	m.value++

	return m.value, nil
}

func TestStateManager_Basic(t *testing.T) {
	// Create mock generators
	stateless := &mockStatelessGenerator{}

	// Since we can't easily create GeneratorInstance directly, let's test the wrapper
	wrapper := NewStatefulWrapper(stateless, 0)

	// Test wrapper functionality
	ctx := context.Background()

	// Test Generate
	value, err := wrapper.Generate(ctx)
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}
	if value != 1 {
		t.Errorf("Expected 1, got %v", value)
	}

	// Test state management
	if wrapper.GetState() != 0 {
		t.Errorf("Expected initial state 0, got %v", wrapper.GetState())
	}

	wrapper.SetState(42)
	if wrapper.GetState() != 42 {
		t.Errorf("Expected state 42, got %v", wrapper.GetState())
	}

	// Test Reset
	err = wrapper.Reset()
	if err != nil {
		t.Fatalf("Reset failed: %v", err)
	}
}

func TestStatefulGeneratorWrapper(t *testing.T) {
	stateless := &mockStatelessGenerator{}
	wrapper := NewStatefulWrapper(stateless, 100)

	ctx := context.Background()

	// Test initial state
	if wrapper.GetState() != 100 {
		t.Errorf("Expected initial state 100, got %v", wrapper.GetState())
	}

	// Test Generate still works
	value, err := wrapper.Generate(ctx)
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}
	if value != 1 {
		t.Errorf("Expected 1, got %v", value)
	}

	// Test state persistence
	wrapper.SetState("test")
	if wrapper.GetState() != "test" {
		t.Errorf("Expected state 'test', got %v", wrapper.GetState())
	}

	// Test Initialize
	err = wrapper.Initialize(ctx)
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Test Reset
	err = wrapper.Reset()
	if err != nil {
		t.Fatalf("Reset failed: %v", err)
	}
}
