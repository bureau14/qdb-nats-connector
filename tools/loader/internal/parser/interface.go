package parser

import (
	"io"

	"github.com/bureau14/qdb-nats-connector/tools/loader/internal"
)

// Parser defines the interface for parsing different data formats
type Parser interface {
	Parse(reader io.Reader) (<-chan internal.Message, <-chan error)
}
