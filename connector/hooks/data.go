package hooks

import (
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
)

// PreReadData contains data passed to pre-read hooks
type PreReadData struct {
	WorkerID  string
	Topic     string
	Timestamp time.Time
}

// PostReadData contains data passed to post-read hooks
type PostReadData struct {
	WorkerID     string
	Topic        string
	MessageCount int
	BatchSize    int
	Duration     time.Duration
	Error        error
	Timestamp    time.Time
}

// PreWriteData contains data passed to pre-write hooks
type PreWriteData struct {
	WorkerID   string
	Topic      string
	TableCount int
	RowCount   int
	Tables     []qdb.WriterTable
	Timestamp  time.Time
}

// PostWriteData contains data passed to post-write hooks
type PostWriteData struct {
	WorkerID      string
	Topic         string
	Duration      time.Duration
	RowsWritten   int
	TablesWritten int
	Error         error
	Timestamp     time.Time
}

// PreAckData contains data passed to pre-ack hooks
type PreAckData struct {
	WorkerID  string
	Topic     string
	Sequences []uint64
	IsNack    bool
	Count     int
	Timestamp time.Time
}

// PostAckData contains data passed to post-ack hooks
type PostAckData struct {
	WorkerID    string
	Topic       string
	AckedCount  int
	NackedCount int
	Error       error
	Timestamp   time.Time
}

// PreCircuitBreakerStateChange contains data passed to pre-circuit breaker state change hooks
type PreCircuitBreakerStateChange struct {
	WorkerID     string
	Resource     string
	CurrentState string
	NextState    string
	Reason       string
}

// PostCircuitBreakerStateChange contains data passed to post-circuit breaker state change hooks
type PostCircuitBreakerStateChange struct {
	WorkerID           string
	Resource           string
	OldState           string
	NewState           string
	Reason             string
	TransitionDuration time.Duration
	FailureCount       int
	SuccessCount       int
	Error              string
}

// PostCircuitBreakerRequestRejected contains data passed to post-circuit breaker request rejection hooks
type PostCircuitBreakerRequestRejected struct {
	WorkerID string
	Resource string
	State    string
	Reason   string
}
