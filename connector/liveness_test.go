// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
// Unit tests for Probe: Begin/End/Sample semantics and concurrent access.
package connector

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestProbeIdleIsInactive(t *testing.T) {
	var p Probe

	stage, age, active := p.Sample(time.Now())

	assert.False(t, active)
	assert.Empty(t, stage)
	assert.Zero(t, age)
}

func TestProbeBeginSampleEnd(t *testing.T) {
	var p Probe

	p.Begin("process")
	stage, age, active := p.Sample(time.Now().Add(time.Minute))

	assert.True(t, active)
	assert.Equal(t, "process", stage)
	assert.GreaterOrEqual(t, age, time.Minute)

	p.End()
	_, _, active = p.Sample(time.Now())

	assert.False(t, active)
}

func TestProbeReBeginResetsAge(t *testing.T) {
	var p Probe

	p.Begin("fetch")
	p.Begin("dispatch")
	stage, age, active := p.Sample(time.Now())

	assert.True(t, active)
	assert.Equal(t, "dispatch", stage)
	assert.Less(t, age, time.Minute)
}

func TestProbeTouchRefreshesAgeKeepsStage(t *testing.T) {
	var p Probe

	p.Begin("process")
	p.mu.Lock()
	p.since = time.Now().Add(-6 * time.Minute)
	p.mu.Unlock()

	p.Touch()
	stage, age, active := p.Sample(time.Now())

	assert.True(t, active)
	assert.Equal(t, "process", stage)
	assert.Less(t, age, time.Minute)
}

func TestProbeTouchOnClearedProbeIsNoop(t *testing.T) {
	var p Probe

	p.Touch()
	_, _, active := p.Sample(time.Now())

	assert.False(t, active)
	p.mu.Lock()
	assert.True(t, p.since.IsZero())
	p.mu.Unlock()

	p.Begin("process")
	p.End()
	p.Touch()
	_, _, active = p.Sample(time.Now())

	assert.False(t, active)
}

func TestProbeConcurrentAccess(t *testing.T) {
	var p Probe
	var wg sync.WaitGroup

	wg.Add(3)
	go func() {
		defer wg.Done()
		for range 1000 {
			p.Begin("process")
			p.End()
		}
	}()
	go func() {
		defer wg.Done()
		for range 1000 {
			p.Sample(time.Now())
		}
	}()
	go func() {
		defer wg.Done()
		for range 1000 {
			p.Touch()
		}
	}()
	wg.Wait()

	_, _, active := p.Sample(time.Now())
	assert.False(t, active)
}
