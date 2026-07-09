// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Unit tests for connector option loading: pure logic, zero external dependencies.
package connector

import "testing"

func TestLoadConfigNatsSecurityFlags(t *testing.T) {
	opts, err := LoadConfig([]string{
		"--stream", "STREAM",
		"--nats-creds-file", "/keys/real.creds",
		"--nats-ca-file", "/keys/ca.pem",
	}, func() {})
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if opts.CredsFile() != "/keys/real.creds" {
		t.Errorf("CredsFile() = %q, want /keys/real.creds", opts.CredsFile())
	}
	if opts.TLSCAFile() != "/keys/ca.pem" {
		t.Errorf("TLSCAFile() = %q, want /keys/ca.pem", opts.TLSCAFile())
	}
}

func TestLoadConfigNatsSecurityEnvVars(t *testing.T) {
	t.Setenv("QDB_NATS_NATS_CREDS_FILE", "/env/user.creds")
	t.Setenv("QDB_NATS_NATS_CA_FILE", "/env/ca.pem")

	opts, err := LoadConfig([]string{"--stream", "STREAM"}, func() {})
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if opts.CredsFile() != "/env/user.creds" {
		t.Errorf("CredsFile() = %q, want /env/user.creds", opts.CredsFile())
	}
	if opts.TLSCAFile() != "/env/ca.pem" {
		t.Errorf("TLSCAFile() = %q, want /env/ca.pem", opts.TLSCAFile())
	}
}

func TestLoadConfigNatsSecurityDefaultsEmpty(t *testing.T) {
	opts, err := LoadConfig([]string{"--stream", "STREAM"}, func() {})
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if opts.CredsFile() != "" {
		t.Errorf("CredsFile() = %q, want empty", opts.CredsFile())
	}
	if opts.TLSCAFile() != "" {
		t.Errorf("TLSCAFile() = %q, want empty", opts.TLSCAFile())
	}
}

func TestLoadConfigHTTPAddrDefault(t *testing.T) {
	opts, err := LoadConfig([]string{"--stream", "STREAM"}, func() {})
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if opts.HTTPAddr != ":9100" {
		t.Errorf("HTTPAddr = %q, want :9100", opts.HTTPAddr)
	}
}

func TestLoadConfigHTTPAddrEmptyDisables(t *testing.T) {
	opts, err := LoadConfig([]string{"--stream", "STREAM", "--http-addr", ""}, func() {})
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if opts.HTTPAddr != "" {
		t.Errorf("HTTPAddr = %q, want empty", opts.HTTPAddr)
	}
}

func TestLoadConfigHTTPAddrRejectsMissingPort(t *testing.T) {
	_, err := LoadConfig([]string{"--stream", "STREAM", "--http-addr", "9100"}, func() {})
	if err == nil {
		t.Fatal("LoadConfig accepted http-addr without host:port form")
	}
}

func TestLoadConfigHTTPAddrEnvVar(t *testing.T) {
	t.Setenv("QDB_NATS_HTTP_ADDR", "127.0.0.1:9200")

	opts, err := LoadConfig([]string{"--stream", "STREAM"}, func() {})
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if opts.HTTPAddr != "127.0.0.1:9200" {
		t.Errorf("HTTPAddr = %q, want 127.0.0.1:9200", opts.HTTPAddr)
	}
}
