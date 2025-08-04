package detector

import (
	"bufio"
	"io"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/tools/loader/internal"
)

// Detector interface for format detection
type Detector interface {
	Detect(reader io.Reader) (int, error)
}

// DefaultDetector implements format detection using magic bytes
type DefaultDetector struct {
	magicBytes map[int][]byte
}

// NewDetector creates a new DefaultDetector with predefined magic bytes
func NewDetector() *DefaultDetector {
	return &DefaultDetector{
		magicBytes: map[int][]byte{
			internal.FormatParquet:  []byte("PAR1"),
			internal.FormatGzipJSON: {0x1f, 0x8b},
		},
	}
}

// Detect determines the format of data from a reader
func (d *DefaultDetector) Detect(reader io.Reader) (int, error) {
	bufReader := bufio.NewReader(reader)

	// Peek at header without consuming data
	header, err := bufReader.Peek(maxPeekSize)
	if err != nil && err != io.EOF {
		return 0, connectorErrors.NewParsingFailedError("detector", err)
	}

	// Try registry-based detection first
	format, regErr := DetectFormat(header)
	if regErr == nil {
		return format, nil
	}

	// Fallback to original implementation for backward compatibility
	// Check magic bytes first
	for format, magic := range d.magicBytes {
		if checkMagicBytes(header, magic) {
			return format, nil
		}
	}

	// Check if content looks like JSON
	if isJSONContent(header) {
		return internal.FormatJSONLines, nil
	}

	// Check if content looks like base64
	if isBase64Content(header) {
		return internal.FormatBase64, nil
	}

	return 0, connectorErrors.NewParsingFailedError("detector", nil)
}
