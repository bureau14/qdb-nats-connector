package connector

import (
	"bytes"
	"reflect"
	"math/rand"

	"testing"
	"testing/quick"

	"github.com/nats-io/nats.go"
)

type TopicString string
const latin = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz01233456789"

func (l TopicString) Generate(rand *rand.Rand, size int) reflect.Value {
	var buffer bytes.Buffer
	for i := 0; i < size; i++ {
		buffer.WriteString(string(latin[rand.Intn(len(latin))]))
	}
	s := TopicString(buffer.String())
	return reflect.ValueOf(s)
}


func DefaultOptions() *Options {
	return &Options {
		NatsEndpoint: nats.DefaultURL,
		NatsTopic: "",
		PidFile: "",
	}
}

func TestTopicNames(t *testing.T) {

	f := func(topic TopicString) bool {
		opts := DefaultOptions()
		opts.NatsTopic = string(topic)

		c, err := NewConnector(opts)

		if err == nil {
			defer c.Close()
		}

		return err == nil
	}


	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}

}
