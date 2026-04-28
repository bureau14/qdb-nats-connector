package detector

import (
	"bytes"
)

const maxPeekSize = 512

// checkMagicBytes checks if header starts with the given magic bytes
func checkMagicBytes(header, magic []byte) bool {
	if len(header) < len(magic) {
		return false
	}

	return bytes.Equal(header[:len(magic)], magic)
}

// isJSONContent checks if the content looks like JSON
func isJSONContent(header []byte) bool {
	// Skip whitespace to find first non-whitespace character
	for _, b := range header {
		if b == ' ' || b == '\t' || b == '\n' || b == '\r' {
			continue
		}
		// Check if first non-whitespace char is '{' or '['
		return b == '{' || b == '['
	}

	return false
}
