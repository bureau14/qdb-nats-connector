package errors

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestErrorsConnectorErrorWhenFormattingShouldReturnStructuredMessage verifies the structured error message formatting.
// Ensures consistent format for log parsing and monitoring systems.
func TestErrorsConnectorErrorWhenFormattingShouldReturnStructuredMessage(t *testing.T) {
	err := NewConnectionFailedError("test-component", "localhost:4222", errors.New("connection refused"))

	expected := "[test-component] failed to connect to localhost:4222: connection refused (code: 1002)"
	assert.Equal(t, expected, err.Error())
}

// TestErrorsConnectorErrorWhenUnwrappingShouldReturnOriginalError verifies error chain traversal functionality.
// Critical for errors.Is() and errors.As() standard library compatibility.
func TestErrorsConnectorErrorWhenUnwrappingShouldReturnOriginalError(t *testing.T) {
	originalErr := errors.New("original error")
	connErr := NewConnectionFailedError("test", "localhost", originalErr)

	assert.Equal(t, originalErr, connErr.Unwrap())
}

// TestErrorsConstructorsWhenCreatingErrorsShouldHaveCorrectCodes validates that all error constructor functions
// create properly formatted ConnectorError instances with correct codes.
// This ensures error codes remain stable for programmatic error handling.
func TestErrorsConstructorsWhenCreatingErrorsShouldHaveCorrectCodes(t *testing.T) {
	tests := []struct {
		name         string
		constructor  func() *ConnectorError
		expectedCode ErrorCode
	}{
		{
			name:         "NoTopicProvided",
			constructor:  func() *ConnectorError { return NewNoTopicProvidedError("test") },
			expectedCode: ErrCodeNoTopicProvided,
		},
		{
			name:         "InvalidConfig",
			constructor:  func() *ConnectorError { return NewInvalidConfigError("test", "invalid setting") },
			expectedCode: ErrCodeInvalidConfig,
		},
		{
			name:         "ParsingFailed",
			constructor:  func() *ConnectorError { return NewParsingFailedError("test", errors.New("parse error")) },
			expectedCode: ErrCodeParsingFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.constructor()
			assert.Equal(t, tt.expectedCode, err.Code)
			assert.Equal(t, "test", err.Component)
		})
	}
}
