// Package parser implements the message transformation pipeline for the connector.
// This internal package provides pluggable parsers that transform raw NATS messages
// into structured data suitable for QuasarDB time series storage.
//
// Parser Architecture Overview:
// The parser system follows a plugin-based architecture where parsers can be:
// - Built-in parsers (JSON, CSV, Protobuf) compiled into the binary
// - External plugins loaded dynamically at runtime via Go's plugin package
// - Chained together to form transformation pipelines
//
// Each parser implements a common interface that accepts NATS messages and
// produces QuasarDB WriterTable structures. The parser selection and chaining
// is configured per NATS subject, allowing flexible routing and transformation.
//
// Decision rationale:
// - Plugin architecture enables runtime parser loading
// - Chain of responsibility pattern for complex transformations
// - Internal package prevents external dependencies
//
// Example configuration (future implementation):
//
//	subjects:
//	  "sensors.temperature":
//	    parsers:
//	      - type: json
//	        table: "temperature_readings"
//	  "logs.app":
//	    parsers:
//	      - type: plugin
//	        path: "/opt/parsers/log_parser.so"
//	      - type: json
package parser

import (
	"fmt"
	"log/slog"

	qdb "github.com/bureau14/qdb-api-go/v3"
	"github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/nats-io/nats.go"
)

// Parser defines the interface for message transformation implementations.
//
// Parser is the core abstraction that all message parsers must implement.
// It transforms raw NATS messages into structured data suitable for QuasarDB
// time series storage.
//
// Key assumptions:
// - Implementations are thread-safe for concurrent parsing
// - Parse operations are stateless and side-effect free
// - Failed parsing returns error without partial writes
//
// Implementation requirements:
// - Must validate message format before processing
// - Should return appropriate ConnectorError with ParsingFailed code
// - Can return multiple tables for fan-out scenarios
// - Should handle nil messages gracefully
type Parser interface {
	// Parse transforms a NATS message into QuasarDB writer tables.
	//
	// Parse is the main entry point for message transformation. It examines the
	// message content and metadata to produce one or more WriterTable instances
	// ready for QuasarDB insertion.
	//
	// Parameters:
	// - msg: The NATS message containing subject, data, and headers
	//
	// Returns:
	// - []qdb.WriterTable: Parsed tables ready for writing (may be empty)
	// - error: ConnectorError with ParsingFailed code on failure
	//
	// Error handling:
	// - Nil message returns ParsingFailed error
	// - Empty message data returns ParsingFailed error
	// - Invalid format returns ParsingFailed error with details
	// - Parse errors should not cause connector shutdown
	Parse(msg *nats.Msg) ([]qdb.WriterTable, error)
}

