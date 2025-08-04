// Package generators provides concrete implementations of field generators.
// This package contains the UUID generator which produces UUID v4 values
// for each generation call.
package generators

import (
	"context"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal"
	"github.com/bureau14/qdb-nats-connector/tools/generator/internal/generator"
	"github.com/google/uuid"
)

// uuidGenerator generates UUID v4 strings.
// Since UUIDs are stateless, this struct has no fields.
type uuidGenerator struct{}

// NewUUIDGenerator creates a UUID generator from configuration options.
// No configuration options are needed for UUID generation.
func NewUUIDGenerator(config map[string]interface{}) (*uuidGenerator, error) {
	return &uuidGenerator{}, nil
}

// Generate returns a new UUID v4 string.
// Each call generates a fresh random UUID.
func (g *uuidGenerator) Generate(ctx context.Context) (interface{}, error) {
	return uuid.New().String(), nil
}

// init registers the UUID generator with the global registry.
func init() {
	generator.RegisterGenerator("uuid", func(config map[string]interface{}) (internal.FieldGenerator, error) {
		return NewUUIDGenerator(config)
	})
}
