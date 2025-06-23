// Package errors provides structured error handling for the qdb-nats-connector.
// This internal package centralizes error creation and management to avoid
// circular dependencies between connector and internal packages.
// Decision rationale:
// - Structured errors enable better debugging and monitoring
// - Component tagging helps identify error sources in distributed logs
// - Error codes allow programmatic error classification
// - Metadata field supports context-specific debugging information
package errors

import (
	"fmt"
)

// ErrorCode represents a unique identifier for different error categories.
// Starting at 1000 to avoid conflicts with standard HTTP status codes.
type ErrorCode uint16

// ConnectorError provides structured error information with component context.
// Key assumptions:
// - Component field identifies the source module (source, parser, sink, etc.)
// - Code field enables programmatic error handling and classification
// - Metadata field stores error-specific debugging context
// Performance trade-offs:
// - Additional memory overhead per error for structured information
// - Faster debugging and monitoring through consistent error format
type ConnectorError struct {
	Code      ErrorCode
	Message   string
	Component string
	Wrapped   error
	Metadata  map[string]interface{}
}

const (
	// ErrCodeNoTopicProvided indicates missing topic configuration
	ErrCodeNoTopicProvided ErrorCode = iota + 1000
	// ErrCodeInvalidConfig indicates malformed or invalid configuration
	ErrCodeInvalidConfig
	// ErrCodeConnectionFailed indicates network connectivity issues
	ErrCodeConnectionFailed
	// ErrCodeSubscriptionFailed indicates NATS subscription setup failure
	ErrCodeSubscriptionFailed
	// ErrCodeParsingFailed indicates message transformation errors
	ErrCodeParsingFailed
	// ErrCodeWriteFailed indicates QuasarDB write operation failure
	ErrCodeWriteFailed
	// ErrCodeUnexpectedError indicates unhandled system-level errors
	ErrCodeUnexpectedError
)

// Error implements the error interface with structured formatting.
// Returns format: "[component] message (code: N)" for consistent log parsing.
func (e *ConnectorError) Error() string {
	return fmt.Sprintf("[%s] %s (code: %d)", e.Component, e.Message, e.Code)
}

// Unwrap implements the errors.Unwrap interface for error chain traversal.
// Enables errors.Is() and errors.As() to work with wrapped underlying errors.
func (e *ConnectorError) Unwrap() error {
	return e.Wrapped
}

// NewNoTopicProvidedError creates an error for missing topic configuration.
// Decision rationale:
// - Topic is mandatory for NATS subscription setup
// - Early validation prevents runtime subscription failures
// Key assumptions:
// - Component parameter identifies the calling module for debugging
func NewNoTopicProvidedError(component string) *ConnectorError {
	return &ConnectorError{
		Code:      ErrCodeNoTopicProvided,
		Component: component,
		Message:   "no topic provided",
		Metadata:  map[string]interface{}{},
	}
}

// NewInvalidConfigError creates an error for malformed configuration values.
// Decision rationale:
// - Configuration errors should fail fast during startup
// - Specific error message helps identify the invalid setting
// Key assumptions:
// - Message parameter contains user-readable description of the issue
func NewInvalidConfigError(component string, message string) *ConnectorError {
	return &ConnectorError{
		Code:      ErrCodeInvalidConfig,
		Component: component,
		Message:   fmt.Sprintf("invalid configuration: %s", message),
		Metadata:  map[string]interface{}{},
	}
}

// NewConnectionFailedError creates an error for network connectivity failures.
// Decision rationale:
// - Network errors should include endpoint for debugging connectivity issues
// - Original error wrapped to preserve low-level diagnostic information
// Key assumptions:
// - Endpoint parameter contains the target address (host:port format)
// - Underlying error contains OS-level connection failure details
func NewConnectionFailedError(component string, endpoint string, err error) *ConnectorError {
	return &ConnectorError{
		Code:      ErrCodeConnectionFailed,
		Component: component,
		Message:   fmt.Sprintf("failed to connect to %s", endpoint),
		Wrapped:   err,
		Metadata:  map[string]interface{}{"endpoint": endpoint},
	}
}

// NewSubscriptionFailedError creates an error for NATS subscription failures.
// Decision rationale:
// - Topic information critical for debugging subscription issues
// - NATS-specific error details preserved through wrapping
// Key assumptions:
// - Topic parameter contains the exact NATS subject pattern
// - Underlying error contains NATS client-specific failure reason
func NewSubscriptionFailedError(component string, topic string, err error) *ConnectorError {
	return &ConnectorError{
		Code:      ErrCodeSubscriptionFailed,
		Component: component,
		Message:   fmt.Sprintf("failed to subscribe to topic %s", topic),
		Wrapped:   err,
		Metadata:  map[string]interface{}{"topic": topic},
	}
}

// NewParsingFailedError creates an error for message transformation failures.
// Decision rationale:
// - Parser errors indicate malformed input data or plugin issues
// - Underlying error preserves parser-specific diagnostic information
// Key assumptions:
// - Component identifies the specific parser that failed
// - Underlying error contains details about the parsing failure
func NewParsingFailedError(component string, err error) *ConnectorError {
	return &ConnectorError{
		Code:      ErrCodeParsingFailed,
		Component: component,
		Message:   "failed to parse message",
		Wrapped:   err,
		Metadata:  map[string]interface{}{},
	}
}

// NewWriteFailedError creates an error for data persistence failures.
// Decision rationale:
// - Write errors indicate QuasarDB connectivity or data format issues
// - Underlying error preserves database-specific failure information
// Key assumptions:
// - Component identifies the sink module attempting the write
// - Underlying error contains QuasarDB client error details
func NewWriteFailedError(component string, err error) *ConnectorError {
	return &ConnectorError{
		Code:      ErrCodeWriteFailed,
		Component: component,
		Message:   "failed to write data",
		Wrapped:   err,
		Metadata:  map[string]interface{}{},
	}
}

// NewUnexpectedError creates an error for unhandled system-level failures.
// Decision rationale:
// - Catch-all for errors that don't fit specific categories
// - Custom message provides context for debugging unexpected conditions
// Key assumptions:
// - Message parameter describes the unexpected condition or operation
// - Underlying error contains system-level failure details
func NewUnexpectedError(component string, message string, err error) *ConnectorError {
	return &ConnectorError{
		Code:      ErrCodeUnexpectedError,
		Component: component,
		Message:   fmt.Sprintf("unexpected error: %s", message),
		Wrapped:   err,
		Metadata:  map[string]interface{}{},
	}
}
