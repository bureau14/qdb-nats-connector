package internal

// Message represents a data message with format information
type Message struct {
	Data   []byte
	Format int
}

// Format constants for different data formats
const (
	FormatJSONLines = iota
	FormatParquet
	FormatGzipJSON
	FormatBase64
)
