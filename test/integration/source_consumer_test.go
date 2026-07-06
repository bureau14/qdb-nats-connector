// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.

// Consumer-binding integration tests for the NATS source: spawns a plain
// nats-server with JetStream and verifies that Source binds to pre-created
// durables (filtered and unfiltered) and fails loudly when the durable is
// absent (no auto-creation).
package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	connectorErrors "github.com/bureau14/qdb-nats-connector/internal/errors"
	"github.com/bureau14/qdb-nats-connector/internal/source"
	"github.com/nats-io/nats.go"
)

// skipMissingNats skips a NATS-dependent test locally; in CI
// (QDB_TEST_REQUIRE_NATS=1, set by scripts/cicd/40.test-integration.sh
// after provisioning the binary) a missing nats-server fails loudly
// instead of skipping silently.
func skipMissingNats(t *testing.T) {
	t.Helper()

	if os.Getenv("QDB_TEST_REQUIRE_NATS") == "1" {
		t.Fatal("nats-server required in this environment but not found in PATH")
	}

	t.Skip("nats-server binary not found in PATH")
}

// startNatsServer spawns a plain nats-server with JetStream enabled.
// Out: server URL; process is terminated via t.Cleanup.
func startNatsServer(t *testing.T, dir string) string {
	t.Helper()

	bin, err := exec.LookPath("nats-server")
	if err != nil {
		skipMissingNats(t)
	}

	port := freeTCPPort(t)
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
	waitForNatsServer(t, url)

	return url
}

// waitForNatsServer polls until the server accepts connections.
func waitForNatsServer(t *testing.T, url string) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		nc, err := nats.Connect(url)
		if err == nil {
			nc.Close()

			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatal("nats-server did not become ready within 10s")
}

// jetStreamContext connects and returns a JetStream context for test setup.
//
//nolint:ireturn // nats.go exposes the JetStream API as an interface
func jetStreamContext(t *testing.T, url string) nats.JetStreamContext {
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

// seedFilterStream creates a two-subject stream and publishes one message
// per subject.
func seedFilterStream(t *testing.T, js nats.JetStreamContext, streamName, subjectA, subjectB string) {
	t.Helper()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{subjectA, subjectB},
	})
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	for _, subject := range []string{subjectA, subjectB} {
		_, err = js.Publish(subject, []byte(`{"probe": true}`))
		if err != nil {
			t.Fatalf("failed to publish seed message on %s: %v", subject, err)
		}
	}
}

// addDurablePullConsumer pre-creates a durable pull consumer.
// In: filterSubject - "" for an unfiltered consumer
func addDurablePullConsumer(t *testing.T, js nats.JetStreamContext, streamName, consumerName, filterSubject string) {
	t.Helper()

	_, err := js.AddConsumer(streamName, &nats.ConsumerConfig{
		Durable:       consumerName,
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: filterSubject,
	})
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}
}

// connectSource builds a Source against url/stream/consumer and connects it.
func connectSource(t *testing.T, url, streamName, consumerName string) *source.Source {
	t.Helper()

	opts := source.NewOptions(
		source.WithEndpoint(url),
		source.WithStreamName(streamName),
	)
	opts.ConsumerName = consumerName

	src, err := source.NewSource(opts)
	if err != nil {
		t.Fatalf("NewSource failed: %v", err)
	}
	t.Cleanup(src.Close)

	err = src.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	return src
}

// fetchSubjects fetches one batch and returns the subjects of its messages.
func fetchSubjects(t *testing.T, src *source.Source) []string {
	t.Helper()

	batch, err := src.FetchBatch(context.Background())
	if err != nil {
		t.Fatalf("FetchBatch failed: %v", err)
	}

	subjects := make([]string, 0, len(batch.Messages))
	for _, m := range batch.Messages {
		subjects = append(subjects, m.Msg.Subject)
	}

	return subjects
}

func TestSourceBindsToFilteredConsumer(t *testing.T) {
	url := startNatsServer(t, t.TempDir())
	js := jetStreamContext(t, url)

	streamName := "FILTER_TEST"
	seedFilterStream(t, js, streamName, "it.filter.a", "it.filter.b")
	addDurablePullConsumer(t, js, streamName, "filtered-consumer", "it.filter.a")

	src := connectSource(t, url, streamName, "filtered-consumer")

	subjects := fetchSubjects(t, src)
	if len(subjects) != 1 {
		t.Fatalf("got %d messages, want 1 (filtered): %v", len(subjects), subjects)
	}
	if subjects[0] != "it.filter.a" {
		t.Fatalf("got subject %q, want %q", subjects[0], "it.filter.a")
	}
}

func TestSourceBindsToUnfilteredConsumer(t *testing.T) {
	url := startNatsServer(t, t.TempDir())
	js := jetStreamContext(t, url)

	streamName := "UNFILTERED_TEST"
	seedFilterStream(t, js, streamName, "it.unfiltered.a", "it.unfiltered.b")
	addDurablePullConsumer(t, js, streamName, "unfiltered-consumer", "")

	src := connectSource(t, url, streamName, "unfiltered-consumer")

	subjects := fetchSubjects(t, src)
	if len(subjects) != 2 {
		t.Fatalf("got %d messages, want 2 (unfiltered): %v", len(subjects), subjects)
	}
}

// TestSourceFailsOnAbsentConsumer pins the no-auto-creation contract: a
// missing durable is a loud SubscriptionFailed Connect error, not a
// silently auto-created consumer pulling the whole stream unfiltered.
func TestSourceFailsOnAbsentConsumer(t *testing.T) {
	url := startNatsServer(t, t.TempDir())
	js := jetStreamContext(t, url)

	streamName := "ABSENT_CONSUMER_TEST"
	seedFilterStream(t, js, streamName, "it.absent.a", "it.absent.b")

	opts := source.NewOptions(
		source.WithEndpoint(url),
		source.WithStreamName(streamName),
	)
	opts.ConsumerName = "absent-consumer"

	src, err := source.NewSource(opts)
	if err != nil {
		t.Fatalf("NewSource failed: %v", err)
	}
	t.Cleanup(src.Close)

	err = src.Connect(context.Background())
	if err == nil {
		t.Fatal("Connect succeeded with an absent durable consumer, want SubscriptionFailed")
	}

	var connErr *connectorErrors.ConnectorError
	if !errors.As(err, &connErr) {
		t.Fatalf("Connect error is not a ConnectorError: %v", err)
	}
	if connErr.Code != connectorErrors.ErrCodeSubscriptionFailed {
		t.Fatalf("got error code %d, want ErrCodeSubscriptionFailed: %v", connErr.Code, err)
	}
	if !errors.Is(err, nats.ErrConsumerNotFound) {
		t.Fatalf("error does not wrap nats.ErrConsumerNotFound: %v", err)
	}
}
