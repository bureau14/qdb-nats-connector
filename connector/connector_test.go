package connector

import (
	"testing"

	"github.com/bureau14/qdb-nats-connector/internal/source"
	"github.com/bureau14/qdb-nats-connector/internal/util"
	"github.com/nats-io/nats.go"
)

// DefaultOptions creates Options suitable for testing.
// Decision rationale:
// - Uses NATS default URL for local testing
// - Random topic names prevent test interference
// - Empty PidFile avoids filesystem side effects
func DefaultOptions() *Options {
	return &Options{
		sourceOptions: source.Options{
			Endpoint: nats.DefaultURL,
			Topic:    util.RandomTopicName(),
		},
		PidFile: "",
	}
}

// TestNewConnector ensures connector initialization succeeds with valid options.
// Key assumptions:
// - NATS server is running on default port
// - No authentication required for testing
// Decision rationale:
// - Tests the happy path of connector creation
// - Verifies cleanup with defer Close()
func TestNewConnector(t *testing.T) {

	c, err := NewConnector(DefaultOptions())

	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()
}
