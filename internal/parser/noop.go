package parser

import (
	"fmt"
	"log/slog"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/nats-io/nats.go"
)

// NoopParser provides basic parser orchestration functionality.
//
// NoopParser serves as a simple implementation that validates messages
// but does not perform actual parsing. It's primarily used for testing
// and as a placeholder until parser plugins are implemented.
//
// Key assumptions:
// - Used when no specific parser is configured
// - Returns empty result for valid messages
// - Validates message structure only
type NoopParser struct {
}

// NewNoopParser creates a noop parser instance.
//
// NewNoopParser initializes a basic parser that validates messages
// but returns empty results. This is primarily used for testing and
// as a placeholder implementation.
//
// Decision rationale:
// - Provides a simple parser for testing connector integration
// - Validates basic message structure without transformation
// - Serves as reference implementation for custom parsers
//
// Example usage:
//
//	parser, err := NewNoopParser()
//	if err != nil {
//	    return fmt.Errorf("failed to initialize parser: %w", err)
//	}
//
// Future enhancements:
// - Accept ParserConfig for plugin paths and settings
// - Load and validate plugins during initialization
// - Return specific errors for plugin loading failures
func NewNoopParser() (*NoopParser, error) {
	slog.Info("Initializing noop parser")
	return &NoopParser{}, nil
}

// Parse implements the Parser interface with basic validation.
//
// This implementation validates the message structure but does not
// perform actual parsing. It serves as a reference implementation
// and testing placeholder.
//
// Decision rationale:
// - Provides minimal parser implementation for testing
// - Validates message structure consistently
// - Returns empty result to indicate successful validation
//
// Error handling:
// - Returns ConnectorError with ErrCodeParsingFailed for invalid messages
// - Nil message returns error immediately
// - Empty message data is considered an error
func (p *NoopParser) Parse(msg *nats.Msg) ([]qdb.WriterTable, error) {
	if msg == nil {
		return nil, errors.NewParsingFailedError("noop_parser", fmt.Errorf("nil message"))
	}
	if len(msg.Data) == 0 {
		return nil, errors.NewParsingFailedError("noop_parser", fmt.Errorf("empty message data"))
	}

	// TODO: Implement actual parsing logic with plugins
	// Future implementation:
	// 1. Look up parser configuration for msg.Subject
	// 2. If no specific parser, use noop parser
	// 3. Execute parser chain if multiple parsers configured
	// 4. Aggregate WriterTable results from all parsers
	slog.Debug("Parsing NATS message", "subject", msg.Subject, "data_len", len(msg.Data))

	// Placeholder - return empty slice for now
	// In real implementation, this would parse the message and create appropriate WriterTable instances
	return []qdb.WriterTable{}, nil
}
