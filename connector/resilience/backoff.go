// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package resilience: exponential backoff for sequential retry loops
// Types: Backoff
// Ex: b := NewBackoff(time.Second, 32*time.Second); b.Wait(ctx)
package resilience

import (
	"context"
	"time"
)

// Backoff: exponential delay sequence for one goroutine retrying
// sequentially (fetch errors, subscription re-binds). Unlike
// CircuitBreaker -- admission control for concurrent request paths --
// it sleeps between attempts and never rejects. Not goroutine-safe:
// single-owner by design.
type Backoff struct {
	base time.Duration
	max  time.Duration
	next time.Duration
}

// NewBackoff creates a backoff starting at base and doubling up to maxDelay.
// In: base, maxDelay time.Duration - initial and capped delay
// Out: Backoff - ready to use, first delay = base
// Ex: NewBackoff(100*time.Millisecond, 5*time.Second)
func NewBackoff(base, maxDelay time.Duration) Backoff {
	return Backoff{base: base, max: maxDelay, next: base}
}

// Next returns the current delay and doubles it, capped at max.
// In: none
// Out: time.Duration - delay to apply for this attempt
// Ex: Next() → 100ms, then 200ms, ... capped at max
func (b *Backoff) Next() time.Duration {
	d := b.next
	b.next *= 2
	if b.next > b.max {
		b.next = b.max
	}

	return d
}

// Wait sleeps for the next delay, honoring context cancellation.
// In: ctx context.Context - cancellation
// Out: error - nil after full sleep, ctx.Err() on cancellation
// Ex: Wait(ctx) → nil (slept), or context.Canceled
func (b *Backoff) Wait(ctx context.Context) error {
	select {
	case <-time.After(b.Next()):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Reset restarts the sequence at the base delay.
// In: none
// Out: none
// Ex: Reset() → next Wait sleeps base again
func (b *Backoff) Reset() {
	b.next = b.base
}
