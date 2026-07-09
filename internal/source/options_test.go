// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Unit tests for source options: pure logic, zero external dependencies.
package source

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// stubProvider: minimal OptionsProvider for FromOptionsProvider tests
type stubProvider struct {
	credsFile string
	tlsCAFile string
}

func (p stubProvider) URL() string                 { return "nats://localhost:4222" }
func (p stubProvider) StreamName() string          { return "STREAM" }
func (p stubProvider) CredsFile() string           { return p.credsFile }
func (p stubProvider) TLSCAFile() string           { return p.tlsCAFile }
func (p stubProvider) BatchSize() int              { return 100 }
func (p stubProvider) BatchTimeout() time.Duration { return time.Second }
func (p stubProvider) FetchTimeout() time.Duration { return 5 * time.Second }

func TestWithCredsFileSetsField(t *testing.T) {
	opts := NewOptions(WithCredsFile("/keys/real.creds"))

	if opts.CredsFile != "/keys/real.creds" {
		t.Fatalf("CredsFile = %q, want /keys/real.creds", opts.CredsFile)
	}
}

func TestWithTLSCAFileSetsField(t *testing.T) {
	opts := NewOptions(WithTLSCAFile("/keys/ca.pem"))

	if opts.TLSCAFile != "/keys/ca.pem" {
		t.Fatalf("TLSCAFile = %q, want /keys/ca.pem", opts.TLSCAFile)
	}
}

func TestFromOptionsProviderCarriesSecurityFiles(t *testing.T) {
	p := stubProvider{credsFile: "/a/user.creds", tlsCAFile: "/a/ca.pem"} //nolint:gosec // test fixture paths, not credentials

	opts := FromOptionsProvider(p)

	if opts.CredsFile != p.credsFile {
		t.Errorf("CredsFile = %q, want %q", opts.CredsFile, p.credsFile)
	}
	if opts.TLSCAFile != p.tlsCAFile {
		t.Errorf("TLSCAFile = %q, want %q", opts.TLSCAFile, p.tlsCAFile)
	}
}

func TestConnectOptionsEmptyConfig(t *testing.T) {
	natsOpts, err := connectOptions(Options{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Lifecycle event handlers are always registered.
	if len(natsOpts) != len(natsEventHandlers()) {
		t.Fatalf("got %d options, want %d", len(natsOpts), len(natsEventHandlers()))
	}
}

func TestConnectOptionsValidFiles(t *testing.T) {
	dir := t.TempDir()
	credsFile := filepath.Join(dir, "user.creds")
	caFile := filepath.Join(dir, "ca.pem")

	for _, f := range []string{credsFile, caFile} {
		err := os.WriteFile(f, []byte("placeholder"), 0o600)
		if err != nil {
			t.Fatalf("failed to write %s: %v", f, err)
		}
	}

	natsOpts, err := connectOptions(Options{
		CredsFile: credsFile,
		TLSCAFile: caFile,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Creds + CA options plus the always-registered lifecycle handlers.
	want := 2 + len(natsEventHandlers())
	if len(natsOpts) != want {
		t.Fatalf("got %d options, want %d", len(natsOpts), want)
	}
}

func TestConnectOptionsMissingCredsFile(t *testing.T) {
	_, err := connectOptions(Options{
		CredsFile: filepath.Join(t.TempDir(), "nonexistent.creds"),
	})

	if err == nil {
		t.Fatal("expected error for missing credentials file, got nil")
	}
}

func TestConnectOptionsMissingCAFile(t *testing.T) {
	_, err := connectOptions(Options{
		TLSCAFile: filepath.Join(t.TempDir(), "nonexistent.pem"),
	})

	if err == nil {
		t.Fatal("expected error for missing TLS CA file, got nil")
	}
}
