package connector

import (
	"github.com/bureau14/qdb-nats-connector/internal/parser"
	"github.com/bureau14/qdb-nats-connector/internal/sink"
	"github.com/bureau14/qdb-nats-connector/internal/source"
	log "github.com/sirupsen/logrus"
)

// Connector orchestrates the source, parser, and sink.
type Connector struct {
	Source *source.Source
	Parser *parser.Parser
	Sink   *sink.Sink
}

// NewConnector creates and initializes a new Connector.
//
// This function orchestrates the creation of source, parser, and sink components,
// handling proper resource cleanup on any initialization failure.
//
// Decision rationale:
// - Options are validated first to fail fast on invalid configuration
// - Components are created in dependency order: source -> parser -> sink
// - Each component failure triggers cleanup of previously created components
//
// Key assumptions:
// - The provided Options have been populated with valid endpoints
// - Network connectivity to NATS and QuasarDB endpoints is available
// - Component initialization is synchronous and blocking
//
// Usage example:
//
//	opts := &Options{
//	    NatsEndpoint: "nats://localhost:4222",
//	    NatsTopic:    "my.topic",
//	    QdbEndpoint:  "qdb://localhost:2836",
//	}
//	conn, err := NewConnector(opts)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer conn.Close()
func NewConnector(opts *Options) (*Connector, error) {
	// Validate options before attempting to create components
	err := ValidateOptions(opts)
	if err != nil {
		log.WithFields(log.Fields{"Options": opts, "err": err}).Error("Options not valid")
		return nil, err
	}

	// Create source using a provider that builds options from the connector's config
	srcOpts := source.FromOptionsProvider(opts)
	src, err := source.NewSource(srcOpts)
	if err != nil {
		log.WithFields(log.Fields{"Options": opts, "err": err}).Error("Failed to create source")
		return nil, err
	}

	par, err := parser.NewParser()
	if err != nil {
		log.WithFields(log.Fields{"err": err}).Error("Failed to create parser")
		src.Close()
		return nil, err
	}

	// Create sink using a provider that builds options from the connector's config
	snkOpts := sink.FromOptionsProvider(opts)
	snk, err := sink.NewSink(snkOpts)
	if err != nil {
		log.WithFields(log.Fields{"Options": snkOpts, "err": err}).Error("Failed to create sink")
		src.Close()
		return nil, err
	}

	return &Connector{
		Source: src,
		Parser: par,
		Sink:   snk,
	}, nil
}

// Close gracefully shuts down the connector's components.
//
// Components are closed in reverse initialization order to ensure
// clean shutdown without data loss.
//
// Key assumptions:
// - Component Close() methods are idempotent
// - Close() methods handle nil receivers gracefully
func (c *Connector) Close() {
	log.Info("Closing connector")
	c.Source.Close()
	c.Sink.Close()
}
