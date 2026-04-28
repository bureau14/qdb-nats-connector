package transformers

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal/transformer"
)

// compressor transforms input data using gzip compression
type compressor struct {
	algorithm string
	level     int
}

// Transform compresses input data using the configured algorithm and level
func (c *compressor) Transform(ctx context.Context, input interface{}) (interface{}, error) {
	// Accept string or []byte
	var data []byte
	switch v := input.(type) {
	case string:
		data = []byte(v)
	case []byte:
		data = v
	default:
		return nil, fmt.Errorf("compress expects string or []byte, got %T", input)
	}

	var buf bytes.Buffer

	switch c.algorithm {
	case "gzip":
		w, err := gzip.NewWriterLevel(&buf, c.level)
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip writer: %w", err)
		}
		_, err = w.Write(data)
		if err != nil {
			_ = w.Close() // Best effort cleanup

			return nil, fmt.Errorf("failed to write data: %w", err)
		}
		err = w.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to close gzip writer: %w", err)
		}
	default:
		return nil, fmt.Errorf("unknown compression algorithm: %s", c.algorithm)
	}

	return buf.Bytes(), nil
}

// NewCompressor creates a new compressor transformer
//
//nolint:ireturn // Factory pattern requires returning interface
func NewCompressor(config map[string]interface{}) (transformer.Transformer, error) {
	algorithm := "gzip"
	level := gzip.DefaultCompression

	// Read algorithm from config
	if alg, ok := config["algorithm"]; ok {
		if algStr, ok := alg.(string); ok {
			algorithm = algStr
		} else {
			return nil, fmt.Errorf("algorithm must be a string, got %T", alg)
		}
	}

	// Read level from config
	if lvl, ok := config["level"]; ok {
		switch v := lvl.(type) {
		case int:
			level = v
		case float64:
			level = int(v)
		default:
			return nil, fmt.Errorf("level must be an integer, got %T", lvl)
		}
	}

	// Validate algorithm
	if algorithm != "gzip" {
		return nil, fmt.Errorf("unsupported compression algorithm: %s", algorithm)
	}

	// Validate compression level
	if level < gzip.HuffmanOnly || level > gzip.BestCompression {
		return nil, fmt.Errorf("invalid gzip compression level: %d (must be between %d and %d)", level, gzip.HuffmanOnly, gzip.BestCompression)
	}

	return &compressor{
		algorithm: algorithm,
		level:     level,
	}, nil
}

func init() {
	transformer.RegisterTransformer("compress", NewCompressor)
}
