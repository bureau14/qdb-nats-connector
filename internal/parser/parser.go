// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package parser: NATS→QuasarDB message transformation
// Types: Parser, JsonParser, ParseResult
// Ex: parser.Parse(msg) → []WriterTable
package parser

import (
	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/nats-io/nats.go"
)

// ParseResult: parse outcome with tables/error & sequence
type ParseResult struct {
	// Tables contains the QuasarDB WriterTable structures created from the parsed message.
	// This slice is empty if parsing failed (Error will be non-nil).
	// Currently, parsers return a single table per message, but the slice format
	// allows for future extensions.
	Tables []qdb.WriterTable

	// Error contains parsing failure information if the message could not be processed.
	// This is nil when parsing succeeds (Tables will contain data).
	// Common errors include invalid JSON, missing required fields, or unsupported data types.
	Error *errors.ConnectorError

	// Sequence is the JetStream consumer sequence number of the message.
	// This is used for selective acknowledgment and negative acknowledgment of messages
	// based on processing results.
	Sequence uint64
}

// Parser: NATS msg→QuasarDB tables transformer, goroutine-safe
type Parser interface {
	// Parse transforms single NATS message to QuasarDB tables
	Parse(msg *nats.Msg) ([]qdb.WriterTable, error)

	// ParseBatch processes messages independently, returns results per message
	ParseBatch(msgs []*nats.Msg) []ParseResult
}

// DefaultParseBatch parses messages individually, tracks sequences.
// In: parser Parser, msgs []*nats.Msg - messages to parse
// Out: []ParseResult - results with tables/errors per message
// Ex: DefaultParseBatch(p, msgs) → [{Tables,nil,seq1},{nil,err,seq2}]
func DefaultParseBatch(parser Parser, msgs []*nats.Msg) []ParseResult {
	results := make([]ParseResult, len(msgs))

	for i, msg := range msgs {
		// Get message metadata for sequence tracking
		var sequence uint64
		if msg != nil {
			meta, err := msg.Metadata()
			if err == nil {
				sequence = meta.Sequence.Consumer
			}
		}

		// Parse individual message
		tables, err := parser.Parse(msg)
		var connErr *errors.ConnectorError
		if err != nil {
			// Convert error to ConnectorError if it isn't already
			ce, ok := err.(*errors.ConnectorError)
			if ok {
				connErr = ce
			} else {
				connErr = errors.NewParsingFailedError("parser", err)
			}
		}

		results[i] = ParseResult{
			Tables:   tables,
			Error:    connErr,
			Sequence: sequence,
		}
	}

	return results
}
