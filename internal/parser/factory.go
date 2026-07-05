// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package parser: YAML-based message transformation pipelines
// Types: YAMLParser, YAMLConfig, ParseState
// Ex: NewYAMLParser(opts).Parse(msg) → []WriterTable
package parser

import (
	"fmt"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/internal/filter"
)

// NewParser creates a parser and its row filter by type.
// Args:
//
//	parserType: "noop", "yaml"
//	opts: config path
//
// Returns:
//
//	Parser: configured parser instance
//	*filter.RowFilter: pre-merge row filter (nil = pass-through; always nil for noop)
//	error: if invalid type or config
//
// Example:
//
//	p, f, err := NewParser("yaml", opts) // → YAML parser + its row filter
func NewParser(parserType string, opts ParserOptions) (Parser, *filter.RowFilter, error) { //nolint:ireturn // factory returns Parser interface by design
	switch parserType {
	case "yaml":
		// YAML parser requires configuration file per ADR-005 - contains
		// transformation pipeline specs and output schema definition
		if opts.ConfigPath == "" {
			return nil, nil, connectorErrors.NewInvalidConfigError("parser",
				"yaml parser requires --parser-config flag pointing to a YAML configuration file\n"+
					"Example: --parser=yaml --parser-config=examples/simple-yaml-parser.yaml")
		}

		// Load config and apply options
		config, err := loadYAMLConfig(opts.ConfigPath)
		if err != nil {
			return nil, nil, err
		}

		// Create YAML parser with building block pipeline compiled from config
		p, err := NewYAMLParserFromConfig(config)
		if err != nil {
			return nil, nil, err
		}

		return p, p.rowFilter, nil
	case "noop":
		p, err := NewNoopParser()
		if err != nil {
			return nil, nil, err
		}

		return p, nil, nil
	default:
		return nil, nil, connectorErrors.NewInvalidConfigError("parser", fmt.Sprintf("unknown parser: %s (valid options: yaml, noop)", parserType))
	}
}

// NewParserWithOptions creates a parser and its row filter from options.
// In: opts ParserOptions - full config
// Out: Parser - configured instance, *filter.RowFilter - row filter (nil = pass-through), error
// Ex: NewParserWithOptions(opts) → parser, filter, nil
func NewParserWithOptions(opts ParserOptions) (Parser, *filter.RowFilter, error) { //nolint:ireturn // factory returns Parser interface by design
	return NewParser(opts.ParserType, opts)
}
