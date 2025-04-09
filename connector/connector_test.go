package connector

import (
	"bytes"
	"math/rand"

	"testing"

	"github.com/nats-io/nats.go"
)

const latin = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz01233456789"

// Generates a random alias, which consists only of "normal" ASCII characters
func randomAlias() string {
	const size = 16
	var buffer bytes.Buffer
	for i := 0; i < size; i++ {
		buffer.WriteString(string(latin[rand.Intn(len(latin))]))
	}

	return buffer.String()
}

func randomTopicName() string {
	return randomAlias()
}

func DefaultOptions() *Options {
	return &Options{
		NatsEndpoint: nats.DefaultURL,
		NatsTopic:    randomTopicName(),
		PidFile:      "",
	}
}

func TestNewConnector(t *testing.T) {

	c, err := NewConnector(DefaultOptions())

	if err != nil {
		t.Error(err)
	}

	defer c.Close()
}
