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

// isBase64Content checks if content looks like base64 encoded data
func isBase64Content(header []byte) bool {
	if len(header) == 0 {
		return false
	}

	// Find the first line
	var line []byte
	for i, b := range header {
		if b == '\n' || b == '\r' {
			line = header[:i]

			break
		}
	}

	// If no newline found, use entire header
	if len(line) == 0 {
		line = header
	}

	return isBase64Line(line)
}

// isBase64Line checks if a line contains only base64 characters
func isBase64Line(line []byte) bool {
	if len(line) == 0 {
		return false
	}

	// Remove trailing whitespace
	for len(line) > 0 && (line[len(line)-1] == ' ' || line[len(line)-1] == '\t') {
		line = line[:len(line)-1]
	}

	// Check if length is reasonable for base64 (multiple of 4 with padding)
	if len(line) < 4 || len(line)%4 != 0 {
		return false
	}

	// Check each character
	for _, b := range line {
		if !isBase64Char(b) {
			return false
		}
	}

	// Additional check: should not be all padding
	paddingCount := 0
	for _, b := range line {
		if b == '=' {
			paddingCount++
		}
	}

	// Too much padding is suspicious
	return paddingCount <= 2
}

// isBase64Char checks if a byte is a valid base64 character
func isBase64Char(b byte) bool {
	return (b >= 'A' && b <= 'Z') ||
		(b >= 'a' && b <= 'z') ||
		(b >= '0' && b <= '9') ||
		b == '+' || b == '/' || b == '='
}
