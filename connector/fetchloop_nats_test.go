// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Real-NATS fetch-loop tests: a durable deleted mid-run triggers the
// re-bind path (recovery once the durable is re-created, fatal
// MaxRetriesExceeded when it never is). Drives fetchLoop directly with a
// test goroutine draining workCh -- no workers, no QuasarDB sink. The
// nats-server harness mirrors test/integration/source_consumer_test.go
// (package integration is not importable from here).
package connector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/internal/source"
	"github.com/nats-io/nats.go"
)

// natsSkipMissing skips locally; fails loudly when CI requires NATS.
func natsSkipMissing(t *testing.T) {
	t.Helper()

	if os.Getenv("QDB_TEST_REQUIRE_NATS") == "1" {
		t.Fatal("nats-server required in this environment but not found in PATH")
	}

	t.Skip("nats-server binary not found in PATH")
}

// natsFreePort allocates a free TCP port for the test server.
func natsFreePort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()

	return port
}

// natsStartServer spawns a plain nats-server with JetStream enabled.
func natsStartServer(t *testing.T, dir string) string {
	t.Helper()

	bin, err := exec.LookPath("nats-server")
	if err != nil {
		natsSkipMissing(t)
	}

	port := natsFreePort(t)
	conf := fmt.Sprintf(`
listen: 127.0.0.1:%d
jetstream { store_dir: %q }
`, port, filepath.Join(dir, "js"))

	confFile := filepath.Join(dir, "nats.conf")
	err = os.WriteFile(confFile, []byte(conf), 0o600)
	if err != nil {
		t.Fatalf("failed to write nats-server config: %v", err)
	}

	cmd := exec.Command(bin, "-c", confFile) //nolint:gosec // bin from LookPath, confFile from t.TempDir()
	err = cmd.Start()
	if err != nil {
		t.Fatalf("failed to start nats-server: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})

	url := fmt.Sprintf("nats://127.0.0.1:%d", port)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		nc, err := nats.Connect(url)
		if err == nil {
			nc.Close()

			return url
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("nats-server did not become ready within 10s")

	return ""
}

// natsJetStream connects and returns a JetStream context for test setup.
//
//nolint:ireturn // nats.go exposes the JetStream API as an interface
func natsJetStream(t *testing.T, url string) nats.JetStreamContext {
	t.Helper()

	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("failed to connect for setup: %v", err)
	}
	t.Cleanup(nc.Close)

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("failed to create JetStream context: %v", err)
	}

	return js
}

// natsAddDurable pre-creates a durable pull consumer.
func natsAddDurable(t *testing.T, js nats.JetStreamContext, stream, consumer, filter string) {
	t.Helper()

	_, err := js.AddConsumer(stream, &nats.ConsumerConfig{
		Durable:       consumer,
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: filter,
	})
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}
}

// fetchLoopHarness: fetchLoop wired to a real NATS source with a capturing
// health logger and a drain goroutine ACKing everything off workCh.
type fetchLoopHarness struct {
	js       nats.JetStreamContext
	capture  *captureHandler
	errCh    chan error
	received chan string   // subject per delivered message
	drained  chan struct{} // closed when workCh closes
	cancel   context.CancelFunc

	stream   string
	consumer string
	subject  string
}

// newFetchLoopHarness provisions stream+durable, connects a source, and
// starts fetchLoop with fast re-bind parameters.
func newFetchLoopHarness(t *testing.T, name string, rebindAttempts int) *fetchLoopHarness {
	t.Helper()

	url := natsStartServer(t, t.TempDir())
	js := natsJetStream(t, url)

	h := &fetchLoopHarness{
		js:       js,
		capture:  &captureHandler{},
		errCh:    make(chan error, 1),
		received: make(chan string, 100),
		drained:  make(chan struct{}),
		stream:   name + "_STREAM",
		consumer: name + "-consumer",
		subject:  "fl." + name,
	}

	_, err := js.AddStream(&nats.StreamConfig{Name: h.stream, Subjects: []string{h.subject}})
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}
	natsAddDurable(t, js, h.stream, h.consumer, h.subject)

	opts := source.NewOptions(
		source.WithEndpoint(url),
		source.WithStreamName(h.stream),
		source.WithBatchTimeout(200*time.Millisecond),
	)
	opts.ConsumerName = h.consumer

	src, err := source.NewSource(opts)
	if err != nil {
		t.Fatalf("NewSource failed: %v", err)
	}
	t.Cleanup(src.Close)

	err = src.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	c := &Connector{
		source:         src,
		workCh:         make(chan *source.MessageBatch, 4),
		health:         newHealthState(slog.New(h.capture)),
		stuckThreshold: 5 * time.Minute,
		rebindBase:     50 * time.Millisecond,
		rebindMax:      200 * time.Millisecond,
		rebindAttempts: rebindAttempts,
	}

	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	t.Cleanup(cancel)

	go c.fetchLoop(ctx, h.errCh)
	go func() {
		defer close(h.drained)
		for batch := range c.workCh {
			seqs := make([]uint64, 0, len(batch.Messages))
			for _, m := range batch.Messages {
				seqs = append(seqs, m.Sequence)
				h.received <- m.Msg.Subject
			}
			_ = batch.AckFunc(seqs)
		}
	}()

	return h
}

// publish sends one message on the harness subject.
func (h *fetchLoopHarness) publish(t *testing.T) {
	t.Helper()

	_, err := h.js.Publish(h.subject, []byte(`{"probe": true}`))
	if err != nil {
		t.Fatalf("failed to publish: %v", err)
	}
}

// expectDelivery waits for one message to arrive off workCh.
func (h *fetchLoopHarness) expectDelivery(t *testing.T, timeout time.Duration, msg string) {
	t.Helper()

	select {
	case <-h.received:
	case <-time.After(timeout):
		t.Fatal(msg)
	}
}

// countReason counts captured records at level with the given reason attr.
func countReason(capture *captureHandler, level slog.Level, reason string) int {
	n := 0
	records := capture.byLevel(level)
	for i := range records {
		if attrValue(records[i], "reason") == reason {
			n++
		}
	}

	return n
}

// waitFor polls cond until true or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal(msg)
}

// TestFetchLoopRebindsAfterConsumerRecreated pins the recovery path:
// deleting the durable mid-run logs exactly one unhealthy transition,
// re-creating it re-binds, resumes delivery, and logs exactly one recovery.
func TestFetchLoopRebindsAfterConsumerRecreated(t *testing.T) {
	h := newFetchLoopHarness(t, "rebind", 50)

	h.publish(t)
	h.expectDelivery(t, 10*time.Second, "no delivery before consumer deletion")

	err := h.js.DeleteConsumer(h.stream, h.consumer)
	if err != nil {
		t.Fatalf("failed to delete consumer: %v", err)
	}

	waitFor(t, 10*time.Second, func() bool {
		return countReason(h.capture, slog.LevelError, condRebinding) == 1
	}, "no unhealthy transition after consumer deletion")

	natsAddDurable(t, h.js, h.stream, h.consumer, h.subject)

	waitFor(t, 10*time.Second, func() bool {
		return countReason(h.capture, slog.LevelInfo, condRebinding) == 1
	}, "no recovery transition after consumer re-creation")

	// The re-created durable redelivers from the stream start, so any
	// delivery here proves the pipeline resumed.
	h.publish(t)
	h.expectDelivery(t, 10*time.Second, "no delivery after successful re-bind")

	if got := countReason(h.capture, slog.LevelError, condRebinding); got != 1 {
		t.Fatalf("got %d unhealthy transitions, want exactly 1", got)
	}

	select {
	case err := <-h.errCh:
		t.Fatalf("unexpected fatal error after recovery: %v", err)
	default:
	}
}

// TestFetchLoopFailsFastWhenConsumerNeverRecreated pins the fail-fast
// contract: past the retry budget the connector reports MaxRetriesExceeded
// and shuts the pipeline down instead of stalling silently.
func TestFetchLoopFailsFastWhenConsumerNeverRecreated(t *testing.T) {
	h := newFetchLoopHarness(t, "fatal", 3)

	h.publish(t)
	h.expectDelivery(t, 10*time.Second, "no delivery before consumer deletion")

	err := h.js.DeleteConsumer(h.stream, h.consumer)
	if err != nil {
		t.Fatalf("failed to delete consumer: %v", err)
	}

	select {
	case fatal := <-h.errCh:
		var connErr *connectorErrors.ConnectorError
		if !errors.As(fatal, &connErr) {
			t.Fatalf("fatal error is not a ConnectorError: %v", fatal)
		}
		if connErr.Code != connectorErrors.ErrCodeMaxRetriesExceeded {
			t.Fatalf("got error code %d, want ErrCodeMaxRetriesExceeded: %v", connErr.Code, fatal)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("no MaxRetriesExceeded after exhausting the re-bind budget")
	}

	select {
	case <-h.drained:
		// workCh closed: fetchLoop returned, workers would exit cleanly.
	case <-time.After(5 * time.Second):
		t.Fatal("workCh not closed after fatal re-bind failure")
	}
}
