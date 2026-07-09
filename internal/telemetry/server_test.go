// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Unit tests for the probe server lifecycle: bind failures surface as
// config-time errors, Start/Shutdown round-trips serve real requests.
package telemetry

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewServerBindConflict(t *testing.T) {
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = blocker.Close() }()

	_, err = NewServer(blocker.Addr().String(), greenSource(), slog.Default())
	require.Error(t, err)

	var connErr *connectorErrors.ConnectorError
	require.ErrorAs(t, err, &connErr)
}

func TestServerStartServeShutdown(t *testing.T) {
	srv, err := NewServer("127.0.0.1:0", greenSource(), slog.Default())
	require.NoError(t, err)
	srv.Start()

	client := http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://%s/livez", srv.Addr()))
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, srv.Shutdown(ctx))
}
