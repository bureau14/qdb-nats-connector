package parser

import (
	"bufio"
	"encoding/json"
	"io"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/tools/loader/internal"
)

// JSONLinesParser parses JSON Lines format (one JSON object per line)
type JSONLinesParser struct{}

// NewJSONLinesParser creates a new JSONLinesParser
func NewJSONLinesParser() *JSONLinesParser {
	return &JSONLinesParser{}
}

// Parse reads JSON Lines from the reader and streams messages
func (p *JSONLinesParser) Parse(reader io.Reader) (messages <-chan internal.Message, errors <-chan error) {
	messageChan := make(chan internal.Message)
	errorChan := make(chan error)

	go func() {
		defer close(messageChan)
		defer close(errorChan)

		scanner := bufio.NewScanner(reader)
		lineNum := 0

		for scanner.Scan() {
			lineNum++
			line := scanner.Bytes()

			// Skip empty lines
			if len(line) == 0 {
				continue
			}

			// Optional JSON validation
			if !json.Valid(line) {
				errorChan <- connectorErrors.NewParsingFailedError("jsonlines",
					connectorErrors.NewInvalidConfigError("json", "invalid JSON"))

				continue
			}

			// Create a copy of the line data since scanner reuses the buffer
			data := make([]byte, len(line))
			copy(data, line)

			messageChan <- internal.Message{
				Data:   data,
				Format: internal.FormatJSONLines,
			}
		}

		err := scanner.Err()
		if err != nil {
			errorChan <- connectorErrors.NewParsingFailedError("jsonlines", err)
		}
	}()

	return messageChan, errorChan
}
