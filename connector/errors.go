package connector

import (
	"fmt"
)

// ConnectorError provides structured error information with API error codes.
// Decision rationale:
// - Interface allows both simple and API errors
// - Enables type switching for error handling
// - Compatible with standard error interface
type (
	ConnectorError interface {
		APIError() *APIError
		error
	}

	// connectorError implements ConnectorError for internal errors.
	// Key assumptions:
	// - API error takes precedence when present
	// - Message provides fallback description
	connectorError struct {
		apiErr  *APIError
		message string
	}

	// APIError represents a structured error with code and description.
	// Decision rationale:
	// - JSON tags enable API error responses
	// - ErrorCode type provides type safety
	// - Description provides human-readable context
	APIError struct {
		ErrorCode   ErrorCode `json:"error_code"`
		Description string    `json:"description"`
	}

	// ErrorCode represents connector-specific error codes.
	// Decision rationale:
	// - uint16 provides sufficient range for error codes
	// - Type alias improves code clarity
	ErrorCode uint16
)

// Pre-defined connector errors for common failure scenarios.
// Decision rationale:
// - Package-level errors enable error comparison
// - Simple errors don't need full ConnectorError complexity
var (
	ErrNoTopicProvided = fmt.Errorf("no topic provided")
)


// Error formats the API error for logging and display.
// Decision rationale:
// - Consistent prefix identifies connector errors
// - Includes both code and description for debugging
func (e *APIError) Error() string {
	return fmt.Sprintf("qdb-nats-connector: error_code=%d description=%s", e.ErrorCode, e.Description)
}

// APIError returns itself to implement the ConnectorError interface.
// Decision rationale:
// - Self-return pattern enables interface compliance
// - Allows APIError to be used directly as ConnectorError
func (e *APIError) APIError() *APIError {
	return e
}

// APIError returns the embedded API error if present.
// Decision rationale:
// - Nil return indicates non-API error
// - Enables error type inspection
func (err *connectorError) APIError() *APIError {
	return err.apiErr
}

// Error returns the error description, preferring API error when available.
// Key assumptions:
// - API error with description takes precedence
// - Falls back to message field if no API error
// Decision rationale:
// - Consistent "qdb-nats-connector:" prefix for identification
// - API errors provide more structured information when available
func (err *connectorError) Error() string {
	if err.apiErr != nil && err.apiErr.Description != "" {
		return err.apiErr.Error()
	}
	return fmt.Sprintf("qdb-nats-connector: %s", err.message)
}

