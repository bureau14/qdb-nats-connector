// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package errors: structured error handling
// Types: ConnectorError, ErrorCode
// Ex: errors.NewParsingFailedError("json", err) → structured error
package errors

import (
	"fmt"
)

// ErrorCode: unique identifier for error categories, starts at 1000
type ErrorCode uint16

// ConnectorError: structured error with component context & metadata
type ConnectorError struct {
	Code      ErrorCode
	Message   string
	Component string
	Wrapped   error
	Metadata  map[string]interface{}
}

const (
	// ErrCodeNoTopicProvided: missing topic config
	ErrCodeNoTopicProvided ErrorCode = iota + 1000
	// ErrCodeInvalidConfig: malformed config
	ErrCodeInvalidConfig
	// ErrCodeConnectionFailed: network issues
	ErrCodeConnectionFailed
	// ErrCodeSubscriptionFailed: NATS sub failure
	ErrCodeSubscriptionFailed
	// ErrCodeParsingFailed: message transform errors
	ErrCodeParsingFailed
	// ErrCodeWriteFailed: QDB write failure
	ErrCodeWriteFailed
	// ErrCodeQueueFull: sink queue at capacity
	ErrCodeQueueFull
	// ErrCodeMaxRetriesExceeded: retry limit reached
	ErrCodeMaxRetriesExceeded
	// ErrCodeUnexpectedError: unhandled system errors
	ErrCodeUnexpectedError
)

// Error formats error as "[component] message (code: N)".
// Out: string - formatted error message
// Ex: Error() → "[sink] failed to write (code: 1005)"
func (e *ConnectorError) Error() string {
	return fmt.Sprintf("[%s] %s (code: %d)", e.Component, e.Message, e.Code)
}

// Unwrap returns wrapped error for errors.Is().
// Out: error - underlying cause
// Ex: Unwrap() → original error
func (e *ConnectorError) Unwrap() error {
	return e.Wrapped
}

// NewNoTopicProvidedError creates missing topic error.
// Args:
//
//	component: string - caller module name
//
// Returns:
//
//	*ConnectorError: ErrCodeNoTopicProvided
//
// Example:
//
//	NewNoTopicProvidedError("source") // → error
func NewNoTopicProvidedError(component string) *ConnectorError {
	return &ConnectorError{
		Code:      ErrCodeNoTopicProvided,
		Component: component,
		Message:   "no topic provided",
		Metadata:  make(map[string]interface{}),
	}
}

// NewInvalidConfigError creates config validation error.
// Args:
//
//	component: string - caller module name
//	message: string - specific validation issue
//
// Returns:
//
//	*ConnectorError: ErrCodeInvalidConfig
//
// Example:
//
//	NewInvalidConfigError("sink", "missing URI") // → error
func NewInvalidConfigError(component string, message string) *ConnectorError {
	return &ConnectorError{
		Code:      ErrCodeInvalidConfig,
		Component: component,
		Message:   fmt.Sprintf("invalid configuration: %s", message),
		Metadata:  make(map[string]interface{}),
	}
}

// NewConnectionFailedError creates network failure error.
// In: component, endpoint string, err error
// Out: *ConnectorError with endpoint metadata
// Ex: NewConnectionFailedError("sink", "qdb://host:2836", err) → err
func NewConnectionFailedError(component string, endpoint string, err error) *ConnectorError {
	return &ConnectorError{
		Code:      ErrCodeConnectionFailed,
		Component: component,
		Message:   fmt.Sprintf("failed to connect to %s", endpoint),
		Wrapped:   err,
		Metadata:  map[string]interface{}{"endpoint": endpoint},
	}
}

// NewSubscriptionFailedError creates NATS sub error.
// In: component, topic string, err error
// Out: *ConnectorError with topic metadata
// Ex: NewSubscriptionFailedError("source", "data.*", err) → err
func NewSubscriptionFailedError(component string, topic string, err error) *ConnectorError {
	return &ConnectorError{
		Code:      ErrCodeSubscriptionFailed,
		Component: component,
		Message:   fmt.Sprintf("failed to subscribe to topic %s", topic),
		Wrapped:   err,
		Metadata:  map[string]interface{}{"topic": topic},
	}
}

// NewParsingFailedError creates parser failure error.
// In: component string, err error
// Out: *ConnectorError
// Ex: NewParsingFailedError("json-parser", err) → err
func NewParsingFailedError(component string, err error) *ConnectorError {
	return &ConnectorError{
		Code:      ErrCodeParsingFailed,
		Component: component,
		Message:   "failed to parse message",
		Wrapped:   err,
		Metadata:  make(map[string]interface{}),
	}
}

// NewWriteFailedError creates QDB write error.
// In: component string, err error
// Out: *ConnectorError
// Ex: NewWriteFailedError("sink", err) → err
func NewWriteFailedError(component string, err error) *ConnectorError {
	return &ConnectorError{
		Code:      ErrCodeWriteFailed,
		Component: component,
		Message:   "failed to write data",
		Wrapped:   err,
		Metadata:  make(map[string]interface{}),
	}
}

// NewQueueFullError creates queue capacity error.
// In: component string, queueDepth int
// Out: *ConnectorError with depth metadata
// Ex: NewQueueFullError("sink", 1000) → err
func NewQueueFullError(component string, queueDepth int) *ConnectorError {
	return &ConnectorError{
		Code:      ErrCodeQueueFull,
		Component: component,
		Message:   "sink queue is full",
		Metadata:  map[string]interface{}{"queue_depth": queueDepth},
	}
}

// NewMaxRetriesExceededError creates retry exhaustion error.
// In: component string, maxAttempts int
// Out: *ConnectorError with attempts metadata
// Ex: NewMaxRetriesExceededError("sink", 3) → err
func NewMaxRetriesExceededError(component string, maxAttempts int) *ConnectorError {
	return &ConnectorError{
		Code:      ErrCodeMaxRetriesExceeded,
		Component: component,
		Message:   fmt.Sprintf("maximum retry attempts exceeded (%d)", maxAttempts),
		Metadata:  map[string]interface{}{"max_attempts": maxAttempts},
	}
}

// NewUnexpectedError creates catch-all error.
// In: component, message string, err error
// Out: *ConnectorError
// Ex: NewUnexpectedError("sink", "panic recovery", err) → err
func NewUnexpectedError(component string, message string, err error) *ConnectorError {
	return &ConnectorError{
		Code:      ErrCodeUnexpectedError,
		Component: component,
		Message:   fmt.Sprintf("unexpected error: %s", message),
		Wrapped:   err,
		Metadata:  make(map[string]interface{}),
	}
}
