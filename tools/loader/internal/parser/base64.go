package parser

import (
	"bufio"
	"encoding/base64"
	"io"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/tools/loader/internal"
)

// Base64Parser parses base64 encoded lines
type Base64Parser struct {
	originalFormat int
}

// NewBase64Parser creates a new Base64Parser
func NewBase64Parser(originalFormat int) *Base64Parser {
	return &Base64Parser{
		originalFormat: originalFormat,
	}
}

// Parse reads base64 encoded lines and decodes them
func (p *Base64Parser) Parse(reader io.Reader) (messages <-chan internal.Message, errors <-chan error) {
	messageChan := make(chan internal.Message)
	errorChan := make(chan error)

	go func() {
		defer close(messageChan)
		defer close(errorChan)

		scanner := bufio.NewScanner(reader)
		lineNum := 0

		for scanner.Scan() {
			lineNum++
			line := scanner.Text()

			// Skip empty lines
			if line == "" {
				continue
			}

			// Decode base64
			decoded, err := base64.StdEncoding.DecodeString(line)
			if err != nil {
				errorChan <- connectorErrors.NewParsingFailedError("base64", err)

				continue
			}

			messageChan <- internal.Message{
				Data:   decoded,
				Format: p.originalFormat,
				Type:   internal.MessageTypeData,
			}
		}

		// Send tombstone message to signal end of data
		messageChan <- internal.Message{
			Data:   nil,
			Format: p.originalFormat,
			Type:   internal.MessageTypeTombstone,
		}

		err := scanner.Err()
		if err != nil {
			errorChan <- connectorErrors.NewParsingFailedError("base64", err)
		}
	}()

	return messageChan, errorChan
}
