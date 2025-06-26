// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package parser: message transformation pipeline
// Types: Parser, JsonParser, NoopParser
// Ex: parser.NewJsonParser().Parse(msg) → []WriterTable
package parser

import (
	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/nats-io/nats.go"
)

// Parser: interface for NATS msg→QDB table transformation, thread-safe
type Parser interface {
	// Parse transforms NATS msg→QDB tables.
	// Args:
	//   msg: *nats.Msg - message with subject/data/headers
	// Returns:
	//   []qdb.WriterTable: parsed tables (may be ∅)
	//   error: ParsingFailed on invalid format
	// Example:
	//   Parse(jsonMsg) // → [table], nil
	Parse(msg *nats.Msg) ([]qdb.WriterTable, error)
}
