package connector

import (

	"testing"

	"github.com/bureau14/qdb-nats-connector/internal/util"
	"github.com/nats-io/nats.go"
)

func DefaultOptions() *Options {
	return &Options{
		NatsEndpoint: nats.DefaultURL,
		NatsTopic:    util.RandomTopicName(),
		PidFile:      "",
	}
}

// Ensures
func TestNewConnector(t *testing.T) {

	c, err := NewConnector(DefaultOptions())

	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()
}

// Tests our testing utilities
func TestWriteData(t *testing.T) {

	c, err := NewConnector(DefaultOptions())

	if err != nil {
		t.Fatal(err)
	}

	defer c.Close()
}
