// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Package connector: liveness probes for blocking pipeline stages
// Types: Probe
// Ex: p.Begin("process"); process(batch); p.End()
package connector

import (
	"sync"
	"time"
)

// Probe: liveness marker for one blocking pipeline stage. The owning
// goroutine brackets each blocking section with Begin/End; the health
// monitor samples the in-flight age from another goroutine. A cleared
// probe (goroutine waiting for work) is healthy by definition -- idle
// never looks stuck. Zero value is ready to use; do not copy after use.
type Probe struct {
	mu    sync.Mutex
	stage string
	since time.Time
}

// Begin marks entry into a blocking stage.
// In: stage string - stage name ("fetch", "dispatch", "process")
// Out: none
// Ex: p.Begin("process")
func (p *Probe) Begin(stage string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.stage = stage
	p.since = time.Now()
}

// End clears the probe on return from the blocking stage.
// In: none
// Out: none
// Ex: p.End()
func (p *Probe) End() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.stage = ""
	p.since = time.Time{}
}

// Touch refreshes the in-flight age without changing stage: "still making
// observable progress in this stage." No-op on a cleared probe -- progress
// in no stage is not progress, and Touch must never resurrect state (e.g. a
// straggler callback firing after End). Touch never sets a stage; Begin/End
// bracket ownership stays with the goroutine.
// In: none
// Out: none
// Ex: p.Touch() // sink retry boundary reached, worker not wedged
func (p *Probe) Touch() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.stage == "" {
		return
	}

	p.since = time.Now()
}

// Sample reports the in-flight stage and its age at the given instant.
// In: now time.Time - monitor's clock (injected for deterministic tests)
// Out: stage, age, active - zero values with active=false when cleared
// Ex: Sample(time.Now()) → "process", 3m12s, true
func (p *Probe) Sample(now time.Time) (string, time.Duration, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.stage == "" {
		return "", 0, false
	}

	return p.stage, now.Sub(p.since), true
}
