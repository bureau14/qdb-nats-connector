// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package parser: NATS→QuasarDB message transformation
// Types: Parser, JsonParser, ParseResult
// Ex: parser.Parse(msg) → []WriterTable
package parser

import (
	"fmt"
	"log/slog"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/nats-io/nats.go"
)

// NoopParser: validation-only parser for testing, returns empty tables
type NoopParser struct{}

// NewNoopParser creates validation-only parser.
// In: none
// Out: *NoopParser, error - test parser, always nil
// Ex: NewNoopParser() → &NoopParser{}, nil
func NewNoopParser() (*NoopParser, error) {
	slog.Info("Initializing noop parser")

	return &NoopParser{}, nil
}

// Parse validates message, returns empty tables.
// In: msg *nats.Msg - message to validate
// Out: []WriterTable, error - ∅ or nil/empty error
// Ex: Parse(msg) → [], nil
func (p *NoopParser) Parse(msg *nats.Msg) ([]qdb.WriterTable, error) {
	if msg == nil {
		return nil, errors.NewParsingFailedError("noop_parser", fmt.Errorf("nil message"))
	}
	if len(msg.Data) == 0 {
		return nil, errors.NewParsingFailedError("noop_parser", fmt.Errorf("empty message data"))
	}

	slog.Debug("Parsing NATS message", "subject", msg.Subject, "data_len", len(msg.Data))

	return []qdb.WriterTable{}, nil
}

// ParseBatch delegates to DefaultParseBatch for validation.
// In: msgs []*nats.Msg - message batch
// Out: []ParseResult - empty tables or errors
// Ex: ParseBatch(msgs) → [{[],nil},{[],err}]
func (p *NoopParser) ParseBatch(msgs []*nats.Msg) []ParseResult {
	return DefaultParseBatch(p, msgs)
}
