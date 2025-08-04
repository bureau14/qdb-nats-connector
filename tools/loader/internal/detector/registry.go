package detector

import (
	"sort"
	"sync"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/tools/loader/internal"
)

// DetectorEntry represents a registered detector with priority
type DetectorEntry struct {
	Priority int
	Detector func([]byte) (int, bool)
}

// DetectorRegistry manages registered format detectors
type DetectorRegistry struct {
	mu        sync.RWMutex
	detectors []DetectorEntry
}

// NewDetectorRegistry creates a new DetectorRegistry
func NewDetectorRegistry() *DetectorRegistry {
	return &DetectorRegistry{
		detectors: make([]DetectorEntry, 0),
	}
}

// Register registers a detector with the given priority
// Lower priority numbers have higher precedence
func (r *DetectorRegistry) Register(priority int, detector func([]byte) (int, bool)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.detectors = append(r.detectors, DetectorEntry{
		Priority: priority,
		Detector: detector,
	})

	// Keep detectors sorted by priority (lower number = higher priority)
	sort.Slice(r.detectors, func(i, j int) bool {
		return r.detectors[i].Priority < r.detectors[j].Priority
	})
}

// Detect runs all registered detectors in priority order
func (r *DetectorRegistry) Detect(header []byte) (int, error) {
	r.mu.RLock()
	detectors := make([]DetectorEntry, len(r.detectors))
	copy(detectors, r.detectors)
	r.mu.RUnlock()

	for _, entry := range detectors {
		if format, detected := entry.Detector(header); detected {
			return format, nil
		}
	}

	return 0, connectorErrors.NewParsingFailedError("detector-registry", nil)
}

// Package-level default registry
var defaultDetectorRegistry = NewDetectorRegistry()

// init registers default detectors with priorities
func init() {
	// Priority 10: Magic bytes (Parquet, Gzip) - highest priority
	defaultDetectorRegistry.Register(10, func(header []byte) (int, bool) {
		// Check Parquet magic bytes
		if checkMagicBytes(header, []byte("PAR1")) {
			return internal.FormatParquet, true
		}
		// Check Gzip magic bytes
		if checkMagicBytes(header, []byte{0x1f, 0x8b}) {
			return internal.FormatGzipJSON, true
		}

		return 0, false
	})

	// Priority 20: JSON detection
	defaultDetectorRegistry.Register(20, func(header []byte) (int, bool) {
		if isJSONContent(header) {
			return internal.FormatJSONLines, true
		}

		return 0, false
	})

	// Priority 30: Base64 detection - lowest priority
	defaultDetectorRegistry.Register(30, func(header []byte) (int, bool) {
		if isBase64Content(header) {
			return internal.FormatBase64, true
		}

		return 0, false
	})
}

// RegisterDetector registers a detector in the default registry
func RegisterDetector(priority int, detector func([]byte) (int, bool)) {
	defaultDetectorRegistry.Register(priority, detector)
}

// DetectFormat runs detection using the default registry
func DetectFormat(header []byte) (int, error) {
	return defaultDetectorRegistry.Detect(header)
}
