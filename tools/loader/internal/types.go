package internal

// MessageType represents the type of message (data or control)
type MessageType int

const (
	MessageTypeData MessageType = iota
	MessageTypeTombstone
)

// Message represents a data message with format information
type Message struct {
	Data   []byte
	Format int
	Type   MessageType // Default is MessageTypeData (0)
}

// Format constants for different data formats
const (
	FormatJSONLines = iota
	FormatParquet
	FormatGzipJSON
	FormatBase64
)
