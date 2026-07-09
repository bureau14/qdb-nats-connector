// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package telemetry: HTTP operational surface -- liveness/readiness
// probes and Prometheus metrics. Owned by main, never imported by
// connector; consumes narrow interfaces satisfied by
// *connector.Connector.
// Types: Server, HealthSource, StatsSource, Collector
// Ex: telemetry.NewServer(":9100", conn, reg, slog.Default()) → server
package telemetry

import (
	"context"
	stderrors "errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/bureau14/qdb-nats-connector/connector"
	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// readHeaderTimeout: bounds header reads so a slow client cannot pin a
// connection open indefinitely (slowloris).
const readHeaderTimeout = 5 * time.Second

// HealthSource: connector-side surface the probes consume.
// Satisfied by *connector.Connector.
type HealthSource interface {
	Health() connector.HealthSummary
	LastHealthEvaluate() time.Time
	NatsConnected() bool
}

// Server: probe HTTP server; listener bound at construction (fail fast
// on a taken port), served on Start, stopped via Shutdown.
type Server struct {
	httpServer *http.Server
	listener   net.Listener
	logger     *slog.Logger
}

// NewServer binds addr and builds the probe + metrics mux.
// In: addr string - listen address (":9100"), src HealthSource - probe
// inputs, gatherer prometheus.Gatherer - /metrics source (nil disables
// the route), logger *slog.Logger - server-error log destination
// Out: *Server, error - bound server or ConnectionFailed on bind error
// Ex: NewServer(":9100", conn, reg, slog.Default()) → srv, nil
func NewServer(addr string, src HealthSource, gatherer prometheus.Gatherer, logger *slog.Logger) (*Server, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, connectorErrors.NewConnectionFailedError("telemetry", addr, err)
	}

	s := &Server{
		listener: listener,
		logger:   logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/livez", handleLivez(src))
	mux.HandleFunc("/readyz", handleReadyz(src))
	if gatherer != nil {
		mux.Handle("/metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	}

	s.httpServer = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
	}

	return s, nil
}

// Addr returns the bound listen address (resolves ":0" for tests).
// In: none
// Out: string - host:port the listener is bound to
// Ex: srv.Addr() → "127.0.0.1:9100"
func (s *Server) Addr() string {
	return s.listener.Addr().String()
}

// Start serves in a background goroutine; returns immediately.
// In: none
// Out: none - serve errors (other than clean shutdown) are logged
// Ex: srv.Start() → probes reachable
func (s *Server) Start() {
	go func() {
		err := s.httpServer.Serve(s.listener)
		if err != nil && !stderrors.Is(err, http.ErrServerClosed) {
			s.logger.Error("Telemetry server failed", "error", err)
		}
	}()
}

// Shutdown gracefully stops the server.
// In: ctx context.Context - shutdown deadline
// Out: error - nil or Unexpected wrapping the shutdown failure
// Ex: srv.Shutdown(ctx) → nil
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.httpServer.Shutdown(ctx)
	if err != nil {
		return connectorErrors.NewUnexpectedError("telemetry",
			"http server shutdown failed", err)
	}

	return nil
}
