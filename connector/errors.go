package connector

import (
	"fmt"
)

type (
	ConnectorError interface {
		APIError() *APIError
		error
	}

	connectorError struct {
		apiErr  *APIError
		message string
	}

	APIError struct {
		ErrorCode   ErrorCode `json:"error_code"`
		Description string    `json:"description"`
	}

	ErrorCode uint16
)

const (
	ErrCodeNoTopicProvided ErrorCode = 13731
)

// Error prints the JetStream API error code and description.
func (e *APIError) Error() string {
	return fmt.Sprintf("qdb-nats-connector: error_code=%d description=%s", e.ErrorCode, e.Description)
}

// APIError implements the JetStreamError interface.
func (e *APIError) APIError() *APIError {
	return e
}

func (err *connectorError) APIError() *APIError {
	return err.apiErr
}

func (err *connectorError) Error() string {
	if err.apiErr != nil && err.apiErr.Description != "" {
		return err.apiErr.Error()
	}
	return fmt.Sprintf("qdb-nats-connector: %s", err.message)
}

var (
	ErrNoTopicProvided ConnectorError = &connectorError{message: "no topic provided"}
)
