package connector

// This file provides a public API facade for the internal errors package.
// Decision rationale:
// - Maintains backward compatibility for external consumers
// - Avoids exposing internal package structure to public API
// - Centralizes error type definitions while preventing circular dependencies
import (
	"github.com/bureau14/qdb-nats-connector/internal/errors"
)

// ErrorCode represents error categories for programmatic handling.
// External consumers can use these codes to implement retry logic
// or specific error handling strategies.
//
// Usage example:
//
//	if connErr, ok := err.(*ConnectorError); ok {
//	    switch connErr.Code {
//	    case ErrCodeConnectionFailed:
//	        // Implement connection retry logic
//	    case ErrCodeParsingFailed:
//	        // Log malformed message and continue
//	    }
//	}
type ErrorCode = errors.ErrorCode

// ConnectorError provides structured error information for debugging and monitoring.
// All connector operations return errors of this type for consistent handling.
//
// Usage example:
//
//	conn, err := NewConnector(opts)
//	if err != nil {
//	    if connErr, ok := err.(*ConnectorError); ok {
//	        log.Printf("Component: %s, Code: %d, Message: %s",
//	            connErr.Component, connErr.Code, connErr.Message)
//	    }
//	}
type ConnectorError = errors.ConnectorError

// Error codes for different failure categories
const (
	ErrCodeNoTopicProvided    = errors.ErrCodeNoTopicProvided
	ErrCodeInvalidConfig      = errors.ErrCodeInvalidConfig
	ErrCodeConnectionFailed   = errors.ErrCodeConnectionFailed
	ErrCodeSubscriptionFailed = errors.ErrCodeSubscriptionFailed
	ErrCodeParsingFailed      = errors.ErrCodeParsingFailed
	ErrCodeWriteFailed        = errors.ErrCodeWriteFailed
	ErrCodeUnexpectedError    = errors.ErrCodeUnexpectedError
)

// Error constructor functions for creating structured connector errors
var (
	// NewNoTopicProvidedError creates an error when topic configuration is missing
	NewNoTopicProvidedError = errors.NewNoTopicProvidedError
	// NewInvalidConfigError creates an error for malformed configuration values
	NewInvalidConfigError = errors.NewInvalidConfigError
	// NewConnectionFailedError creates an error for network connectivity failures
	NewConnectionFailedError = errors.NewConnectionFailedError
	// NewSubscriptionFailedError creates an error for NATS subscription failures
	NewSubscriptionFailedError = errors.NewSubscriptionFailedError
	// NewParsingFailedError creates an error for message transformation failures
	NewParsingFailedError = errors.NewParsingFailedError
	// NewWriteFailedError creates an error for data persistence failures
	NewWriteFailedError = errors.NewWriteFailedError
	// NewUnexpectedError creates an error for unhandled system-level failures
	NewUnexpectedError = errors.NewUnexpectedError
)
