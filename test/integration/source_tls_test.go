// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.

// TLS integration test for the NATS source: spawns a dedicated TLS-only
// nats-server with a self-signed CA (generated in-process, no openssl
// dependency) and verifies that Source connects and fetches with
// WithTLSCAFile, and fails without it.
package integration

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/bureau14/qdb-nats-connector/internal/source"
	"github.com/nats-io/nats.go"
)

// writePEM writes a single PEM block to path.
func writePEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()

	data := pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der})
	err := os.WriteFile(path, data, 0o600)
	if err != nil {
		t.Fatalf("failed to write %s: %v", path, err)
	}
}

// newCACertificate creates a self-signed CA certificate + key.
func newCACertificate(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey, []byte) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate CA key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "qdb-nats-connector test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("failed to create CA certificate: %v", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("failed to parse CA certificate: %v", err)
	}

	return cert, key, der
}

// newServerCertificate creates a localhost server certificate signed by the CA.
func newServerCertificate(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey) ([]byte, *ecdsa.PrivateKey) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate server key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("failed to create server certificate: %v", err)
	}

	return der, key
}

// writeTLSTestCerts generates CA + server certs into dir.
// Out: caFile, certFile, keyFile paths
func writeTLSTestCerts(t *testing.T, dir string) (caFile, certFile, keyFile string) {
	t.Helper()

	ca, caKey, caDER := newCACertificate(t)
	serverDER, serverKey := newServerCertificate(t, ca, caKey)

	serverKeyDER, err := x509.MarshalECPrivateKey(serverKey)
	if err != nil {
		t.Fatalf("failed to marshal server key: %v", err)
	}

	caFile = filepath.Join(dir, "ca.pem")
	certFile = filepath.Join(dir, "server-cert.pem")
	keyFile = filepath.Join(dir, "server-key.pem")

	writePEM(t, caFile, "CERTIFICATE", caDER)
	writePEM(t, certFile, "CERTIFICATE", serverDER)
	writePEM(t, keyFile, "EC PRIVATE KEY", serverKeyDER)

	return caFile, certFile, keyFile
}

// freeTCPPort reserves an ephemeral port and releases it for reuse.
func freeTCPPort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate port: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()

	return port
}

// startTLSNatsServer spawns a TLS-only nats-server with JetStream enabled.
// Out: server URL; process is terminated via t.Cleanup.
func startTLSNatsServer(t *testing.T, dir, certFile, keyFile, caFile string) string {
	t.Helper()

	bin, err := exec.LookPath("nats-server")
	if err != nil {
		t.Skip("nats-server binary not found in PATH")
	}

	port := freeTCPPort(t)
	conf := fmt.Sprintf(`
listen: 127.0.0.1:%d
jetstream { store_dir: %q }
tls {
  cert_file: %q
  key_file: %q
}
`, port, filepath.Join(dir, "js"), certFile, keyFile)

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
	waitForTLSNatsServer(t, url, caFile)

	return url
}

// waitForTLSNatsServer polls until the server accepts TLS connections.
func waitForTLSNatsServer(t *testing.T, url, caFile string) {
	t.Helper()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		nc, err := nats.Connect(url, nats.RootCAs(caFile))
		if err == nil {
			nc.Close()

			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatal("nats-server did not become ready within 10s")
}

// seedTLSStream creates a JetStream stream with a pre-created durable pull
// consumer (the source never auto-creates one) and publishes one message.
func seedTLSStream(t *testing.T, url, caFile, streamName, consumerName, subject string) {
	t.Helper()

	nc, err := nats.Connect(url, nats.RootCAs(caFile))
	if err != nil {
		t.Fatalf("failed to connect for seeding: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("failed to create JetStream context: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{subject},
	})
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	_, err = js.AddConsumer(streamName, &nats.ConsumerConfig{
		Durable:       consumerName,
		AckPolicy:     nats.AckExplicitPolicy,
		FilterSubject: subject,
	})
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}

	_, err = js.Publish(subject, []byte(`{"probe": true}`))
	if err != nil {
		t.Fatalf("failed to publish seed message: %v", err)
	}
}

func TestSourceTLSCAFile(t *testing.T) {
	dir := t.TempDir()
	caFile, certFile, keyFile := writeTLSTestCerts(t, dir)
	url := startTLSNatsServer(t, dir, certFile, keyFile, caFile)

	streamName := "TLS_TEST"
	seedTLSStream(t, url, caFile, streamName, "tls-test-consumer", "tls.test")

	opts := source.NewOptions(
		source.WithEndpoint(url),
		source.WithStreamName(streamName),
		source.WithTLSCAFile(caFile),
	)
	opts.ConsumerName = "tls-test-consumer"

	src, err := source.NewSource(opts)
	if err != nil {
		t.Fatalf("NewSource failed: %v", err)
	}
	defer src.Close()

	ctx := context.Background()
	err = src.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect with TLS CA failed: %v", err)
	}

	batch, err := src.FetchBatch(ctx)
	if err != nil {
		t.Fatalf("FetchBatch failed: %v", err)
	}
	if len(batch.Messages) != 1 {
		t.Fatalf("got %d messages, want 1", len(batch.Messages))
	}
}

func TestSourceTLSWithoutCAFails(t *testing.T) {
	dir := t.TempDir()
	caFile, certFile, keyFile := writeTLSTestCerts(t, dir)
	url := startTLSNatsServer(t, dir, certFile, keyFile, caFile)

	opts := source.NewOptions(
		source.WithEndpoint(url),
		source.WithStreamName("TLS_TEST"),
	)
	opts.ConsumerName = "tls-test-consumer"

	src, err := source.NewSource(opts)
	if err != nil {
		t.Fatalf("NewSource failed: %v", err)
	}
	defer src.Close()

	err = src.Connect(context.Background())
	if err == nil {
		t.Fatal("expected Connect without CA to fail against private-CA TLS server")
	}
}
