package errors

import (
	"errors"
	"testing"
)

// TestConnectorError_Error verifies the structured error message formatting.
// Ensures consistent format for log parsing and monitoring systems.
func TestConnectorError_Error(t *testing.T) {
	err := NewConnectionFailedError("test-component", "localhost:4222", errors.New("connection refused"))

	expected := "[test-component] failed to connect to localhost:4222 (code: 1002)"
	if err.Error() != expected {
		t.Errorf("Expected error message %q, got %q", expected, err.Error())
	}
}

// TestConnectorError_Unwrap verifies error chain traversal functionality.
// Critical for errors.Is() and errors.As() standard library compatibility.
func TestConnectorError_Unwrap(t *testing.T) {
	originalErr := errors.New("original error")
	connErr := NewConnectionFailedError("test", "localhost", originalErr)

	if connErr.Unwrap() != originalErr {
		t.Error("Unwrap() should return the original wrapped error")
	}
}

// TestErrorConstructors validates that all error constructor functions
// create properly formatted ConnectorError instances with correct codes.
// This ensures error codes remain stable for programmatic error handling.
func TestErrorConstructors(t *testing.T) {
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
			if err.Code != tt.expectedCode {
				t.Errorf("Expected error code %d, got %d", tt.expectedCode, err.Code)
			}
			if err.Component != "test" {
				t.Errorf("Expected component 'test', got '%s'", err.Component)
			}
		})
	}
}
