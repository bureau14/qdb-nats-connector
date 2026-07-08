// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Unit tests for sink options: pure logic, zero external dependencies.
package sink

import (
	"testing"
	"time"

	qdb "github.com/bureau14/qdb-api-go/v3"
)

// stubProvider: minimal OptionsProvider for FromOptionsProvider tests
type stubProvider struct {
	pushMode qdb.WriterPushMode
}

func (p stubProvider) ClusterUri() string           { return "qdb://localhost:2836" }
func (p stubProvider) ClusterPublicKeyFile() string { return "" }
func (p stubProvider) UserSecurityFile() string     { return "" }
func (p stubProvider) Encryption() *qdb.Encryption  { return nil }
func (p stubProvider) Compression() qdb.Compression { return qdb.CompBalanced }
func (p stubProvider) DeduplicationMode() qdb.WriterDeduplicationMode {
	return qdb.WriterDeduplicationModeDrop
}
func (p stubProvider) PushMode() qdb.WriterPushMode { return p.pushMode }
func (p stubProvider) ClientMaxParallelism() *uint  { return nil }
func (p stubProvider) ClientMaxInBufSize() *uint    { return nil }
func (p stubProvider) Timeout() *time.Duration      { return nil }

// FromOptionsProvider must carry the provider's push mode into the sink
// options; a dropped push mode silently reverts every write to the async
// default (QDB-19359 follow-up: dead --qdb-push-mode flag).
func TestFromOptionsProviderCarriesPushMode(t *testing.T) {
	modes := []qdb.WriterPushMode{
		qdb.WriterPushModeTransactional,
		qdb.WriterPushModeAsync,
		qdb.WriterPushModeFast,
	}
	for _, mode := range modes {
		opts := FromOptionsProvider(stubProvider{pushMode: mode})

		if opts.PushMode != mode {
			t.Errorf("PushMode = %v, want %v", opts.PushMode, mode)
		}
	}
}
