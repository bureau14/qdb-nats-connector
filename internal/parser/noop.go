// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package parser: message transformation pipeline
// Types: Parser, JsonParser, NoopParser
// Ex: parser.NewJsonParser().Parse(msg) → []WriterTable
package parser

import (
	"fmt"
	"log/slog"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/nats-io/nats.go"
)

// NoopParser: validation-only parser for testing, returns empty tables
type NoopParser struct {
}

// NewNoopParser creates validation-only parser.
// Returns:
//
//	*NoopParser: test parser, returns ∅
//	error: never fails (interface consistency)
//
// Example:
//
//	NewNoopParser() // → parser, nil
func NewNoopParser() (*NoopParser, error) {
	slog.Info("Initializing noop parser")
	return &NoopParser{}, nil
}

// Parse validates msg, returns ∅.
// Args:
//
//	msg: *nats.Msg - NATS message for validation
//
// Returns:
//
//	[]qdb.WriterTable: empty slice (testing only)
//	error: ParsingFailed on nil/empty msg
//
// Example:
//
//	Parse(msg) // → [], nil
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
