package transformers

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal/transformer"
)

// jsonEncoder transforms a map[string]interface{} to a JSON string
type jsonEncoder struct{}

// Transform converts a map[string]interface{} record to a JSON string
func (e *jsonEncoder) Transform(ctx context.Context, input interface{}) (interface{}, error) {
	// Type check: expect map[string]interface{}
	record, ok := input.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("encode_json expects map[string]interface{}, got %T", input)
	}

	data, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("json encoding failed: %w", err)
	}

	return string(data), nil
}

// NewJSONEncoder creates a new JSON encoder transformer
//
//nolint:ireturn // Factory pattern requires returning interface
func NewJSONEncoder(config map[string]interface{}) (transformer.Transformer, error) {
	return &jsonEncoder{}, nil
}

func init() {
	transformer.RegisterTransformer("encode_json", NewJSONEncoder)
}
