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
	ParserType  string // noop, yaml
	ConfigPath  string // Path to parser config file (for yaml parser)
	ErrorAction string // drop, fail
}

// Parser: transforms NATS messages into QuasarDB timeseries tables. Needed for pluggable data transformations.
// Who: connector uses, NoopParser/YAMLParser implement.
// Parse: converts single message to tables
type Parser interface {
	// Parse transforms single NATS message to QuasarDB tables
	Parse(msg *nats.Msg) ([]qdb.WriterTable, error)
}
