package transformer

import "context"

// Transformer applies a transformation to a value
type Transformer interface {
	Transform(ctx context.Context, input interface{}) (interface{}, error)
}

// Factory creates a transformer from configuration
type Factory func(config map[string]interface{}) (Transformer, error)
