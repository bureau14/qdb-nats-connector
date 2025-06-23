// Package util provides internal utilities for testing and development.
// This internal package contains helper functions that are not part of
// the public API but are shared across internal packages.
// Decision rationale:
// - Internal package prevents external dependencies
// - Centralized utilities reduce code duplication
// - Test helpers isolated from production code
package util

import (
	"bytes"
	"math/rand"
)

// Latin / ASCII characters that are "safe" to use everywhere
// Decision rationale:
// - Alphanumeric only avoids special character issues
// - No ambiguous characters (0/O, 1/l)
// - Safe for filenames, topics, and identifiers
const latin = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"

// RandomAlias generates a 16-character random string suitable for identifiers.
// Key assumptions:
// - Uses global rand source (not cryptographically secure)
// - Fixed length of 16 characters
// - Only alphanumeric characters
// Decision rationale:
// - Buffer pre-allocation avoids repeated allocations
// - 16 chars provides sufficient uniqueness for testing
// Performance trade-offs:
// - bytes.Buffer is efficient for small strings
// - Single allocation for entire string
func RandomAlias() string {
	const size = 16
	var buffer bytes.Buffer
	for i := 0; i < size; i++ {
		buffer.WriteString(string(latin[rand.Intn(len(latin))]))
	}

	return buffer.String()
}

// RandomTopicName generates a random NATS topic name for testing.
// Decision rationale:
// - Delegates to RandomAlias for consistency
// - Topic names don't need special formatting
// - Avoids collision in concurrent tests
func RandomTopicName() string {
	return RandomAlias()
}

// GenerateTestData creates sample messages for parser testing.
// Decision rationale:
// - Placeholder for future test data generation
// - Will support multiple formats (JSON, CSV, etc)
func GenerateTestData() {
}
