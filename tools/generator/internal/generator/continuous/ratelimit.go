// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.

// Package continuous: continuous generation mode with rate limiting
// Types: RateLimiter
// Ex: limiter := NewRateLimiter(1000.0) // 1000 msgs/sec
package continuous

import (
	"sync"
	"time"
)

// RateLimiter implements token bucket algorithm for smooth rate control
// Uses token bucket to limit generation rate in continuous mode
// In: rate (tokens per second), capacity (bucket size)
// Ex: limiter.Wait() blocks until token available
type RateLimiter struct {
	rate     float64    // tokens per second
	capacity float64    // max tokens in bucket
	tokens   float64    // current tokens
	lastTime time.Time  // last token update
	mu       sync.Mutex // thread safety
}

// NewRateLimiter creates token bucket rate limiter
// In: rate in messages per second
// Out: initialized RateLimiter
// Ex: NewRateLimiter(1000) → limiter for 1k msgs/sec
func NewRateLimiter(rate float64) *RateLimiter {
	return &RateLimiter{
		rate:     rate,
		capacity: rate, // 1 second burst capacity
		tokens:   rate, // start with full bucket
		lastTime: time.Now(),
	}
}

// Wait blocks until token available
// In: none
// Out: none (blocks until ready)
// Ex: limiter.Wait() → blocks if rate exceeded
func (r *RateLimiter) Wait() {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(r.lastTime).Seconds()

	// Add new tokens based on elapsed time
	r.tokens += elapsed * r.rate
	if r.tokens > r.capacity {
		r.tokens = r.capacity
	}
	r.lastTime = now

	// Wait if no tokens available
	if r.tokens < 1 {
		sleepTime := time.Duration((1 - r.tokens) / r.rate * float64(time.Second))
		time.Sleep(sleepTime)

		// Update tokens after sleep
		now = time.Now()
		elapsed = now.Sub(r.lastTime).Seconds()
		r.tokens += elapsed * r.rate
		r.lastTime = now
	}

	// Consume one token
	r.tokens -= 1
}

// SetRate updates generation rate
// In: new rate in messages per second
// Out: none
// Ex: limiter.SetRate(5000) → updates to 5k msgs/sec
func (r *RateLimiter) SetRate(rate float64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.rate = rate
	r.capacity = rate
}
