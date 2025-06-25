// Package parser implements the message transformation pipeline for the connector.
// This internal package provides pluggable parsers that transform raw NATS messages
// into structured data suitable for QuasarDB time series storage.
// Decision rationale:
// - Plugin architecture enables runtime parser loading
// - Chain of responsibility pattern for complex transformations
// - Internal package prevents external dependencies
package parser

import (
	"fmt"
	"log/slog"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/internal/errors"
)

// Parser orchestrates message transformation through configured parser plugins.
// Key assumptions:
// - Parser plugins are loaded at initialization time
// - Parse operations are thread-safe
// - Failed parsing returns error without side effects
type Parser struct {
	// TODO: Add parser configuration fields here.
}

// NewParser creates a parser instance ready for message transformation.
// Decision rationale:
// - Initialization validates parser configuration early
// - Returns error for invalid plugin paths or configurations
// - Empty parser for now, plugin loading to be implemented
// Performance trade-offs:
// - Plugin loading happens once at startup
// - Runtime parsing avoids plugin lookup overhead
func NewParser() (*Parser, error) {
	slog.Info("Initializing new parser")
	return &Parser{}, nil
}

// Parse transforms raw message data into QuasarDB writer tables.
// Decision rationale:
// - Returns slice of WriterTable to support multiple table writes per message
// - Plugin architecture allows for extensible parsing logic
// Performance trade-offs:
// - Memory allocation for slice and tables on each parse
// - Trade-off between memory usage and flexibility for multi-table writes
func (p *Parser) Parse(data []byte) ([]*qdb.WriterTable, error) {
	if len(data) == 0 {
		return nil, errors.NewParsingFailedError("parser", fmt.Errorf("empty message data"))
	}

	// TODO: Implement actual parsing logic with plugins
	slog.Debug("Parsing message data", "data_len", len(data))

	// Placeholder - return empty slice for now
	// In real implementation, this would parse the message and create appropriate WriterTable instances
	return []*qdb.WriterTable{}, nil
}
