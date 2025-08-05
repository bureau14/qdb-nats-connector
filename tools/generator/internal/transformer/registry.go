package transformer

import (
	"fmt"
	"sync"
)

var (
	registry = make(map[string]Factory)
	mu       sync.RWMutex
)

// RegisterTransformer registers a transformer factory with the given name
func RegisterTransformer(name string, factory Factory) {
	mu.Lock()
	defer mu.Unlock()
	registry[name] = factory
}

// GetTransformer creates a transformer instance from a registered factory
//
//nolint:ireturn // Factory pattern requires returning interface
func GetTransformer(name string, config map[string]interface{}) (Transformer, error) {
	mu.RLock()
	factory, exists := registry[name]
	mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("unknown transformer: %s", name)
	}

	return factory(config)
}
