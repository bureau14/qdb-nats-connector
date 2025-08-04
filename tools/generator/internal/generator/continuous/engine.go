// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.

package continuous

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"sync"
	"time"

	"github.com/bureau14/qdb-nats-connector/tools/generator/internal"
	"github.com/bureau14/qdb-nats-connector/tools/generator/internal/generator"
)

// Engine runs continuous data generation with rate limiting
// Manages lifecycle of continuous generation including rate control,
// memory monitoring, and progress reporting
type Engine struct {
	template          *internal.Template
	generators        map[string]*generator.GeneratorInstance
	rateLimiter       *RateLimiter
	duration          time.Duration
	mu                sync.RWMutex
	messagesGenerated uint64
	startTime         time.Time
}

// NewEngine creates continuous generation engine
// In: template, generators, rate (msgs/sec), duration (0=infinite)
// Out: initialized engine
// Ex: NewEngine(tmpl, gens, 1000, 5*time.Minute)
func NewEngine(template *internal.Template, generators map[string]*generator.GeneratorInstance, rate float64, duration time.Duration) *Engine {
	return &Engine{
		template:    template,
		generators:  generators,
		rateLimiter: NewRateLimiter(rate),
		duration:    duration,
		startTime:   time.Now(),
	}
}

// Run executes continuous generation loop
// In: context for cancellation, writer for output
// Out: error if generation fails
// Ex: engine.Run(ctx, writer) → generates until ctx cancelled
func (e *Engine) Run(ctx context.Context, writer io.Writer) error {
	encoder := json.NewEncoder(writer)

	// Create progress reporter
	progressTicker := time.NewTicker(10 * time.Second)
	defer progressTicker.Stop()

	// Create timeout if duration specified
	var timeoutCh <-chan time.Time
	if e.duration > 0 {
		timer := time.NewTimer(e.duration)
		defer timer.Stop()
		timeoutCh = timer.C
	}

	// Create done channel for clean shutdown
	done := make(chan struct{})
	defer close(done)

	// Start progress reporter goroutine
	go e.reportProgress(progressTicker.C, done)

	// Main generation loop
	for {
		select {
		case <-ctx.Done():
			slog.Info("continuous generation stopped", "reason", "context cancelled")

			return nil
		case <-timeoutCh:
			slog.Info("continuous generation completed", "duration", e.duration)

			return nil
		default:
			// Rate limiting
			e.rateLimiter.Wait()

			// Generate record
			record, err := e.generateSingleRecord(ctx)
			if err != nil {
				return fmt.Errorf("failed to generate record: %w", err)
			}

			// Write record
			err = encoder.Encode(record)
			if err != nil {
				return fmt.Errorf("failed to encode record: %w", err)
			}

			// Update counter
			e.mu.Lock()
			e.messagesGenerated++
			e.mu.Unlock()
		}
	}
}

// GetStats returns generation statistics
// Out: messages generated, elapsed time, current rate
// Ex: msgs, elapsed, rate := engine.GetStats()
func (e *Engine) GetStats() (messagesGenerated uint64, elapsed time.Duration, rate float64) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	elapsed = time.Since(e.startTime)
	rate = float64(e.messagesGenerated) / elapsed.Seconds()

	return e.messagesGenerated, elapsed, rate
}

// generateSingleRecord creates one record using generators
func (e *Engine) generateSingleRecord(ctx context.Context) (map[string]interface{}, error) {
	record := make(map[string]interface{})

	for fieldName, generator := range e.generators {
		value, err := generator.Generate(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to generate field %s: %w", fieldName, err)
		}
		record[fieldName] = value
	}

	if e.template.Table != "" {
		record["$table"] = e.template.Table
	}

	return record, nil
}

// reportProgress logs generation statistics
func (e *Engine) reportProgress(ticker <-chan time.Time, done <-chan struct{}) {
	for {
		select {
		case <-ticker:
			e.mu.RLock()
			count := e.messagesGenerated
			e.mu.RUnlock()

			elapsed := time.Since(e.startTime)
			rate := float64(count) / elapsed.Seconds()

			var m runtime.MemStats
			runtime.ReadMemStats(&m)

			slog.Info("generation progress",
				"messages", count,
				"rate", fmt.Sprintf("%.2f msgs/sec", rate),
				"elapsed", elapsed.Round(time.Second),
				"memory_mb", m.Alloc/1024/1024,
			)
		case <-done:
			return
		}
	}
}
