// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package parser: YAML-based message transformation pipelines
// Types: YAMLParser, YAMLConfig, ParseState
// Ex: NewYAMLParser(opts).Parse(msg) → []WriterTable
package parser

import (
	"fmt"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
)

// NewParser creates parser by type.
// Args:
//
//	parserType: "noop", "yaml"
//	opts: config path, error mode, parallel
//
// Returns:
//
//	Parser: configured parser instance
//	error: if invalid type or config
//
// Example:
//
//	p := NewParser("yaml", opts) // → YAML parser with building blocks
func NewParser(parserType string, opts ParserOptions) (Parser, error) { //nolint:ireturn // factory returns Parser interface by design
	switch parserType {
	case "yaml":
		// YAML parser requires configuration file per ADR-005 - contains
		// transformation pipeline specs and output schema definition
		if opts.ConfigPath == "" {
			return nil, connectorErrors.NewInvalidConfigError("parser",
				"yaml parser requires --parser-config flag pointing to a YAML configuration file\n"+
					"Example: --parser=yaml --parser-config=examples/simple-yaml-parser.yaml")
		}

		// Load config and apply options
		config, err := loadYAMLConfig(opts.ConfigPath)
		if err != nil {
			return nil, err
		}

		// Create YAML parser with building block pipeline compiled from config
		return NewYAMLParserFromConfig(config, opts)
	case "noop":
		return NewNoopParser()
	default:
		return nil, connectorErrors.NewInvalidConfigError("parser", fmt.Sprintf("unknown parser: %s (valid options: yaml, noop)", parserType))
	}
}

// NewParserWithOptions creates parser from options.
// In: opts ParserOptions - full config
// Out: Parser - configured instance
// Ex: NewParserWithOptions(opts) → parser
func NewParserWithOptions(opts ParserOptions) (Parser, error) { //nolint:ireturn // factory returns Parser interface by design
	return NewParser(opts.ParserType, opts)
}
