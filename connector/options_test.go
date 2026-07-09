// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Unit tests for connector option loading: pure logic, zero external dependencies.
package connector

import (
	"testing"
	"time"

	"github.com/bureau14/qdb-nats-connector/internal/metrics"
)

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

func TestLoadConfigMetricsBucketDefaults(t *testing.T) {
	opts, err := LoadConfig([]string{"--stream", "STREAM"}, func() {})
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	want := metrics.Config{
		Lag:   metrics.BucketConfig{Start: time.Millisecond, Factor: 2.0, Count: 20},
		Write: metrics.BucketConfig{Start: time.Millisecond, Factor: 2.0, Count: 20},
	}
	if got := opts.MetricsConfig(); got != want {
		t.Errorf("MetricsConfig() = %+v, want %+v", got, want)
	}
}

func TestLoadConfigMetricsBucketFlagsOverride(t *testing.T) {
	opts, err := LoadConfig([]string{
		"--stream", "STREAM",
		"--metrics-lag-bucket-start", "10ms",
		"--metrics-lag-bucket-factor", "3.0",
		"--metrics-lag-bucket-count", "8",
		"--metrics-write-bucket-start", "5ms",
		"--metrics-write-bucket-factor", "1.5",
		"--metrics-write-bucket-count", "16",
	}, func() {})
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	want := metrics.Config{
		Lag:   metrics.BucketConfig{Start: 10 * time.Millisecond, Factor: 3.0, Count: 8},
		Write: metrics.BucketConfig{Start: 5 * time.Millisecond, Factor: 1.5, Count: 16},
	}
	if got := opts.MetricsConfig(); got != want {
		t.Errorf("MetricsConfig() = %+v, want %+v", got, want)
	}
}

func TestLoadConfigMetricsBucketValidation(t *testing.T) {
	cases := []struct {
		name string
		flag string
		val  string
	}{
		{"lag start zero", "--metrics-lag-bucket-start", "0s"},
		{"lag factor one", "--metrics-lag-bucket-factor", "1.0"},
		{"lag count zero", "--metrics-lag-bucket-count", "0"},
		{"lag count too large", "--metrics-lag-bucket-count", "65"},
		{"write start negative", "--metrics-write-bucket-start", "-1ms"},
		{"write factor below one", "--metrics-write-bucket-factor", "0.5"},
		{"write count zero", "--metrics-write-bucket-count", "0"},
		{"write count too large", "--metrics-write-bucket-count", "65"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadConfig([]string{"--stream", "STREAM", tc.flag, tc.val}, func() {})
			if err == nil {
				t.Fatalf("LoadConfig accepted %s=%s", tc.flag, tc.val)
			}
		})
	}
}
