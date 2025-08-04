// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.

package generators

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal"
	"github.com/bureau14/qdb-nats-connector/tools/generator/internal/generator"
)

// gzippedJSONGenerator creates compressed JSON data
// Generates nested JSON, compresses with gzip, encodes as base64
// In: content field definitions, compression level
// Ex: complex sensor data compressed for transport
type gzippedJSONGenerator struct {
	contentDef       map[string]interface{}                  // Content field definitions
	compressionLevel int                                     // Gzip compression level (1-9)
	subGenerators    map[string]*generator.GeneratorInstance // Nested generators
}

// NewGzippedJSONGenerator creates compressed JSON generator
// Config options:
//   - content: map of field definitions (required)
//   - compression_level: 1-9, default 6
//
// Ex: {"content": {"timestamp": {...}, "data": {...}}, "compression_level": 6}
func NewGzippedJSONGenerator(config map[string]interface{}) (*gzippedJSONGenerator, error) {
	gen := &gzippedJSONGenerator{
		compressionLevel: gzip.DefaultCompression, // 6 by default
	}

	// Parse content definition (required)
	content, ok := config["content"].(map[string]interface{})
	if !ok || len(content) == 0 {
		return nil, fmt.Errorf("gzipped_json requires 'content' map")
	}
	gen.contentDef = content

	// Parse compression level
	if levelVal, ok := config["compression_level"]; ok {
		var level float64
		switch v := levelVal.(type) {
		case float64:
			level = v
		case int:
			level = float64(v)
		case int64:
			level = float64(v)
		default:
			return nil, fmt.Errorf("compression_level must be a number")
		}

		intLevel := int(level)
		if intLevel < 1 || intLevel > 9 {
			return nil, fmt.Errorf("compression_level must be between 1 and 9")
		}
		gen.compressionLevel = intLevel
	}

	// Create sub-generators for content fields
	gen.subGenerators = make(map[string]*generator.GeneratorInstance)
	for fieldName, fieldDef := range content {
		fieldMap, ok := fieldDef.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("content field %s must be a map", fieldName)
		}

		// Extract type and config
		fieldType, ok := fieldMap["type"].(string)
		if !ok {
			return nil, fmt.Errorf("content field %s requires 'type'", fieldName)
		}

		// Get config or use empty map
		fieldConfig, _ := fieldMap["config"].(map[string]interface{})
		if fieldConfig == nil {
			fieldConfig = make(map[string]interface{})
		}

		// Create generator
		subGen, err := generator.CreateGenerator(context.Background(), fieldType, fieldConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create generator for field %s: %w", fieldName, err)
		}

		gen.subGenerators[fieldName] = subGen
	}

	return gen, nil
}

// Generate produces compressed JSON data
// Generates nested JSON → Gzip → Base64
func (g *gzippedJSONGenerator) Generate(ctx context.Context) (interface{}, error) {
	// Generate content using sub-generators
	content := make(map[string]interface{})

	for fieldName, subGen := range g.subGenerators {
		value, err := subGen.Generate(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to generate field %s: %w", fieldName, err)
		}
		content[fieldName] = value
	}

	// Convert to JSON
	jsonData, err := json.Marshal(content)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON: %w", err)
	}

	// Compress with gzip
	var compressedBuf bytes.Buffer
	gzWriter, err := gzip.NewWriterLevel(&compressedBuf, g.compressionLevel)
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip writer: %w", err)
	}

	_, err = gzWriter.Write(jsonData)
	if err != nil {
		_ = gzWriter.Close() // Best effort cleanup

		return nil, fmt.Errorf("failed to compress data: %w", err)
	}

	err = gzWriter.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to close gzip writer: %w", err)
	}

	// Encode as base64
	encoded := base64.StdEncoding.EncodeToString(compressedBuf.Bytes())

	return encoded, nil
}

// Initialize prepares sub-generators if stateful
func (g *gzippedJSONGenerator) Initialize(ctx context.Context) error {
	for fieldName, subGen := range g.subGenerators {
		err := generator.InitializeIfStateful(subGen.GetGenerator(), ctx)
		if err != nil {
			return fmt.Errorf("failed to initialize field %s: %w", fieldName, err)
		}
	}

	return nil
}

// Reset resets all sub-generators if stateful
func (g *gzippedJSONGenerator) Reset() error {
	for fieldName, subGen := range g.subGenerators {
		if stateful, ok := subGen.GetGenerator().(generator.StatefulFieldGenerator); ok {
			err := stateful.Reset()
			if err != nil {
				return fmt.Errorf("failed to reset field %s: %w", fieldName, err)
			}
		}
	}

	return nil
}

// GetState returns state of all sub-generators
func (g *gzippedJSONGenerator) GetState() interface{} {
	states := make(map[string]interface{})

	for fieldName, subGen := range g.subGenerators {
		if stateful, ok := subGen.GetGenerator().(generator.StatefulFieldGenerator); ok {
			states[fieldName] = stateful.GetState()
		}
	}

	return map[string]interface{}{
		"compression_level": g.compressionLevel,
		"field_states":      states,
	}
}

// GetMetadata returns generator metadata
func (g *gzippedJSONGenerator) GetMetadata() generator.GeneratorMetadata {
	return generator.GeneratorMetadata{
		Name:        "gzipped_json",
		Description: "Compressed JSON generator for binary-safe transport",
		Version:     "1.0.0",
		Capabilities: generator.GeneratorCapabilities{
			IsStateful:   true, // If any sub-generator is stateful
			IsBinary:     true, // Produces binary data
			IsContinuous: true, // Works well in continuous mode
		},
	}
}

// Register the generator
func init() {
	generator.RegisterGenerator("gzipped_json", func(config map[string]interface{}) (internal.FieldGenerator, error) {
		return NewGzippedJSONGenerator(config)
	})
}
