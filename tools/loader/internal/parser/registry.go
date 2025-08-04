package parser

import (
	"sync"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/tools/loader/internal"
)

// ParserRegistry manages registered parser factories
type ParserRegistry struct {
	mu      sync.RWMutex
	parsers map[int]func() Parser
}

// NewParserRegistry creates a new ParserRegistry
func NewParserRegistry() *ParserRegistry {
	return &ParserRegistry{
		parsers: make(map[int]func() Parser),
	}
}

// Register registers a parser factory for a given format
func (r *ParserRegistry) Register(format int, factory func() Parser) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.parsers[format] = factory
}

// GetParser returns a parser for the given format
//
//nolint:ireturn // Registry function legitimately returns interface
func (r *ParserRegistry) GetParser(format int) (Parser, error) {
	r.mu.RLock()
	factory, exists := r.parsers[format]
	r.mu.RUnlock()

	if !exists {
		return nil, connectorErrors.NewInvalidConfigError("registry", "Format not registered")
	}

	return factory(), nil
}

// SupportedFormats returns a slice of all registered format IDs
func (r *ParserRegistry) SupportedFormats() []int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	formats := make([]int, 0, len(r.parsers))
	for format := range r.parsers {
		formats = append(formats, format)
	}

	return formats
}

// Package-level default registry
var defaultRegistry = NewParserRegistry()

// init registers default parsers
func init() {
	// Register JSONLinesParser
	defaultRegistry.Register(internal.FormatJSONLines, func() Parser {
		return NewJSONLinesParser()
	})

	// Register Base64Parser - Note: this wraps JSONLines as the decoded format
	defaultRegistry.Register(internal.FormatBase64, func() Parser {
		return NewBase64Parser(internal.FormatJSONLines)
	})
}

// RegisterParser registers a parser factory in the default registry
func RegisterParser(format int, factory func() Parser) {
	defaultRegistry.Register(format, factory)
}

// GetRegisteredParser returns a parser from the default registry
//
//nolint:ireturn // Registry function legitimately returns interface
func GetRegisteredParser(format int) (Parser, error) {
	return defaultRegistry.GetParser(format)
}

// GetSupportedFormats returns all formats supported by the default registry
func GetSupportedFormats() []int {
	return defaultRegistry.SupportedFormats()
}
