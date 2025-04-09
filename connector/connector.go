package connector

import (
	"github.com/nats-io/nats.go"

	log "github.com/sirupsen/logrus"
)

type Connector struct {
	Options  Options
	NatsConn *nats.Conn
}

func NewConnector(opts *Options) (*Connector, error) {

	err := ValidateOptions(opts)
	if err != nil {
		log.WithFields(log.Fields{"Options": opts, "err": err}).Fatal("Options not valid")
		return nil, err
	}

	log.WithFields(log.Fields{"NatsEndpoint": opts.NatsEndpoint}).Info("Establishing connection with NATS endpoint")

	nc, err := nats.Connect(opts.NatsEndpoint)

	if err != nil {
		log.WithFields(log.Fields{"NatsEndpoint": opts.NatsEndpoint, "err": err}).Error("Error while establishing connection")
		return nil, err
	}

	log.WithFields(log.Fields{"Topic": opts.NatsTopic}).Info("Subscribing to topic")

	nc.Subscribe("foo", processMessage)

	return &Connector{*opts, nc}, nil
}

func (c *Connector) Close() {
	log.Info("Draining NATS connector")
	c.NatsConn.Drain()

	log.Info("Closing NATS connector")
	c.NatsConn.Close()
}

func processMessage(m *nats.Msg) {
	log.WithFields(log.Fields{"Message": m}).Info("Received a message")
}
