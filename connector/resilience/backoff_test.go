// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Unit tests for Backoff: doubling, cap, reset, context cancellation.
package resilience

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackoffDoublesUpToCap(t *testing.T) {
	b := NewBackoff(100*time.Millisecond, time.Second)

	want := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		time.Second,
		time.Second,
	}
	for i, w := range want {
		assert.Equal(t, w, b.Next(), "attempt %d", i)
	}
}

func TestBackoffCapNotPowerOfTwoMultiple(t *testing.T) {
	b := NewBackoff(time.Second, 5*time.Second)

	assert.Equal(t, time.Second, b.Next())
	assert.Equal(t, 2*time.Second, b.Next())
	assert.Equal(t, 4*time.Second, b.Next())
	assert.Equal(t, 5*time.Second, b.Next())
	assert.Equal(t, 5*time.Second, b.Next())
}

func TestBackoffReset(t *testing.T) {
	b := NewBackoff(100*time.Millisecond, time.Second)

	b.Next()
	b.Next()
	b.Reset()

	assert.Equal(t, 100*time.Millisecond, b.Next())
}

func TestBackoffWaitSleepsFullDelay(t *testing.T) {
	b := NewBackoff(50*time.Millisecond, time.Second)

	start := time.Now()
	err := b.Wait(context.Background())

	require.NoError(t, err)
	assert.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)
}

func TestBackoffWaitReturnsPromptlyOnCancel(t *testing.T) {
	b := NewBackoff(time.Hour, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := b.Wait(ctx)

	require.ErrorIs(t, err, context.Canceled)
	assert.Less(t, time.Since(start), time.Second)
}
