// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package parser: NATS→QuasarDB message transformation
// Types: Parser, NoopParser, YAMLParser
// Ex: parser.Parse(msg) → []WriterTable
package parser

import (
	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/nats-io/nats.go"
)

// ParserOptions: parser factory configuration
type ParserOptions struct {
	ParserType string // noop, yaml
	ConfigPath string // Path to parser config file (for yaml parser)
}

// Outcome classifies what happened to a message during parsing. The parser
// only reports; policy (drop vs fail) is applied by the caller (worker).
type Outcome int

const (
	// OutcomeOK: all pipeline steps succeeded.
	OutcomeOK Outcome = iota
	// OutcomePartial: structure intact, some field steps failed; the row is
	// sentinel-filled for the missing columns (ADR-005 partial extraction).
	OutcomePartial
	// OutcomeUnusable: structural failure -- the message cannot become a
	// valid row (undecodable payload, no $timestamp despite a configured
	// extract_index step, or missing/invalid $table routing).
	OutcomeUnusable
)

// ParseResult: classified result of parsing one message.
// Tables is populated for OK and Partial, empty for Unusable.
// Errors holds step errors in pipeline order; Errors[0] is representative.
type ParseResult struct {
	Tables  []qdb.WriterTable
	Outcome Outcome
	Errors  []error
}

// Parser: transforms NATS messages into QuasarDB timeseries tables. Needed for pluggable data transformations.
// Who: connector uses, NoopParser/YAMLParser implement.
// Parse: converts single message to tables
//
// CRITICAL MEMORY PINNING CONTRACT:
// =================================
// ALL Parser implementations MUST ensure that strings in WriterTable are compatible
// with Go's runtime.Pinner. This is a HARD REQUIREMENT due to QuasarDB's zero-copy
// architecture using unsafe.StringData() and direct memory access from C++.
//
// IMPLEMENTATION RULES FOR PARSER AUTHORS:
// 1. NEVER use strings.Builder for any string that will be written to QuasarDB
// 2. ALWAYS use direct concatenation (+) or fmt.Sprintf() for string construction
// 3. String literals and simple conversions are safe
// 4. Strings from buffers or pools may NOT be contiguous - verify before use
//
// TESTING YOUR PARSER:
// - Run with large batches to trigger memory pressure
// - Look for segfaults in qdb_batch_push_columns
// - Use race detector to find memory access issues
// - Test with concurrent writes to expose pinning problems
type Parser interface {
	// Parse transforms single NATS message to a classified ParseResult.
	// The returned error is reserved for internal invariant violations
	// (nil/empty message, table construction failure); step-level failures
	// are reported via ParseResult.Outcome and ParseResult.Errors so the
	// caller can apply its drop/fail policy.
	// All strings in ParseResult.Tables MUST be pinnable by runtime.Pinner.
	Parse(msg *nats.Msg) (ParseResult, error)
}
