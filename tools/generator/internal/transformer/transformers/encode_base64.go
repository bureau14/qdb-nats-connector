package transformers

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal/transformer"
)

// base64Encoder transforms binary data to base64 string
type base64Encoder struct{}

// Transform converts []byte input to a base64 encoded string
func (e *base64Encoder) Transform(ctx context.Context, input interface{}) (interface{}, error) {
	// Type check: expect []byte
	data, ok := input.([]byte)
	if !ok {
		return nil, fmt.Errorf("encode_base64 expects []byte, got %T", input)
	}

	return base64.StdEncoding.EncodeToString(data), nil
}

// NewBase64Encoder creates a new base64 encoder transformer
//
//nolint:ireturn // Factory pattern requires returning interface
func NewBase64Encoder(config map[string]interface{}) (transformer.Transformer, error) {
	return &base64Encoder{}, nil
}

func init() {
	transformer.RegisterTransformer("encode_base64", NewBase64Encoder)
}
