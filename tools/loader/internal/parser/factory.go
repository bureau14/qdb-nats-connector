package parser

import (
	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/tools/loader/internal"
)

// GetParser returns the appropriate parser based on the format
//
//nolint:ireturn // Factory function legitimately returns interface
func GetParser(format int) (Parser, error) {
	// First check registry for registered parsers
	parser, err := GetRegisteredParser(format)
	if err == nil {
		return parser, nil
	}

	// Fall back to legacy implementations for backward compatibility
	switch format {
	case internal.FormatGzipJSON:
		return nil, connectorErrors.NewInvalidConfigError("factory", "Gzip format not yet implemented")
	case internal.FormatParquet:
		return nil, connectorErrors.NewInvalidConfigError("factory", "Parquet format not yet implemented")
	default:
		return nil, connectorErrors.NewInvalidConfigError("factory", "Unknown data format")
	}
}
