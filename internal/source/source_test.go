// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Unit tests for the pending-message cache: pure logic, no NATS.
package source

import (
	"context"
	"testing"
	"time"
)

func TestPendingMessagesServesFreshCache(t *testing.T) {
	s := &Source{}
	s.pendingCount.Store(42)
	s.pendingAt.Store(time.Now().UnixNano())

	// JetStream is nil: any refresh attempt would error, so a nil error
	// proves the cache short-circuited the RPC.
	got, err := s.PendingMessages(context.Background(), time.Hour)
	if err != nil {
		t.Fatalf("PendingMessages = err %v, want cached hit", err)
	}
	if got != 42 {
		t.Fatalf("PendingMessages = %d, want 42", got)
	}
}

func TestPendingMessagesZeroMaxAgeForcesRefresh(t *testing.T) {
	s := &Source{}
	s.pendingCount.Store(42)
	s.pendingAt.Store(time.Now().UnixNano())

	// maxAge 0 must bypass the cache; with nil JetStream the refresh
	// fails and returns the last-known value alongside the error.
	got, err := s.PendingMessages(context.Background(), 0)
	if err == nil {
		t.Fatal("PendingMessages = nil error, want forced-refresh failure")
	}
	if got != 42 {
		t.Fatalf("PendingMessages = %d, want last-known 42", got)
	}
}

func TestPendingMessagesEmptyCacheRefreshes(t *testing.T) {
	s := &Source{}

	// Zero pendingAt means never populated: even a huge maxAge must not
	// claim freshness.
	got, err := s.PendingMessages(context.Background(), time.Hour)
	if err == nil {
		t.Fatal("PendingMessages = nil error, want refresh failure on empty cache")
	}
	if got != 0 {
		t.Fatalf("PendingMessages = %d, want 0", got)
	}
}
