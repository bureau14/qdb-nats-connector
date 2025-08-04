package internal

import (
	"context"
	"log/slog"
	"time"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	nats "github.com/nats-io/nats.go"
)

// Connection represents a NATS connection with JetStream support
type Connection struct {
	nc         *nats.Conn
	js         nats.JetStreamContext
	url        string
	maxRetries int
}

// NewConnection creates a new NATS connection instance
func NewConnection(url string) *Connection {
	return &Connection{
		url:        url,
		maxRetries: 10,
	}
}

// Connect establishes a connection to NATS with exponential backoff retry logic
func (c *Connection) Connect(ctx context.Context) error {
	backoffDurations := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		32 * time.Second,
	}

	for attempt := range c.maxRetries {
		if attempt > 0 {
			slog.Info("Retrying NATS connection", "attempt", attempt+1, "max_retries", c.maxRetries)
		}

		// Attempt to connect
		nc, err := nats.Connect(c.url)
		if err == nil {
			// Create JetStream context
			js, jsErr := nc.JetStream()
			if jsErr != nil {
				nc.Close()
				if attempt == c.maxRetries-1 {
					return connectorErrors.NewMaxRetriesExceededError("jetstream", c.maxRetries)
				}
			} else {
				c.nc = nc
				c.js = js
				slog.Info("Successfully connected to NATS with JetStream", "url", c.url)

				return nil
			}
		}

		// If this was the last attempt, return max retries exceeded error
		if attempt == c.maxRetries-1 {
			return connectorErrors.NewMaxRetriesExceededError("nats", c.maxRetries)
		}

		// Calculate backoff duration
		var backoffDuration time.Duration
		if attempt < len(backoffDurations) {
			backoffDuration = backoffDurations[attempt]
		} else {
			backoffDuration = 32 * time.Second // Constant 32s after exhausting exponential sequence
		}

		// Wait for backoff duration or context cancellation
		select {
		case <-ctx.Done():
			return connectorErrors.NewConnectionFailedError("nats", c.url, ctx.Err())
		case <-time.After(backoffDuration):
			// Continue to next retry
		}
	}

	// This should never be reached due to the loop logic above
	return connectorErrors.NewMaxRetriesExceededError("nats", c.maxRetries)
}

// Close closes the NATS connection
func (c *Connection) Close() {
	if c.nc != nil {
		c.nc.Close()
		c.nc = nil
		c.js = nil
		slog.Info("NATS connection closed")
	}
}

// Publish publishes data to a NATS topic using JetStream
func (c *Connection) Publish(topic string, data []byte) error {
	if c.js == nil {
		return connectorErrors.NewConnectionFailedError("jetstream", c.url, nil)
	}

	_, err := c.js.Publish(topic, data)
	if err != nil {
		return connectorErrors.NewWriteFailedError("jetstream", err)
	}

	return nil
}
