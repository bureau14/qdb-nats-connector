// Copyright (c) 2009-2025, quasardb SAS. All rights reserved.

package main

import (
	"log/slog"
	"time"

	"github.com/bureau14/qdb-nats-connector/internal/telemetry"
	"github.com/coreos/go-systemd/v22/daemon"
)

// sdNotifyReady sends READY=1 to systemd; silent no-op outside systemd
// (NOTIFY_SOCKET unset). Injected as opts.OnReady. NOTIFY_SOCKET is
// kept in the environment -- the watchdog petter sends through the
// same socket afterwards.
// In: none
// Out: none - send failures are logged, never fatal
// Ex: opts.OnReady = sdNotifyReady
func sdNotifyReady() {
	acked, err := daemon.SdNotify(false, daemon.SdNotifyReady)
	if err != nil {
		slog.Warn("sd-notify READY failed", "error", err)

		return
	}
	if acked {
		slog.Info("sd-notify READY sent")
	}
}

// watchdogPetter pets the systemd watchdog at half the WATCHDOG_USEC
// interval. Every pet is gated on the exact /livez verdict
// (telemetry.Alive), so a wedged process stops petting and systemd
// kills and restarts it; dependency outages (NATS/QDB down) never
// stop pets -- a restart would not help. Lifecycle mirrors
// telemetry.StatsLogger: Start spawns the tick goroutine, Stop signals
// it and waits for exit.
type watchdogPetter struct {
	interval time.Duration
	src      telemetry.HealthSource
	stop     chan struct{}
	done     chan struct{}
}

// newWatchdogPetter probes WATCHDOG_USEC and builds the petter; nil
// when systemd did not request a watchdog (loop never starts).
// In: src telemetry.HealthSource - liveness inputs (*connector.Connector)
// Out: *watchdogPetter - unstarted petter, nil when watchdog disabled
// Ex: wd := newWatchdogPetter(c) → petter ticking at WATCHDOG_USEC/2
func newWatchdogPetter(src telemetry.HealthSource) *watchdogPetter {
	timeout, err := daemon.SdWatchdogEnabled(false)
	if err != nil {
		slog.Warn("systemd watchdog probe failed", "error", err)

		return nil
	}
	if timeout <= 0 {
		return nil
	}

	slog.Info("systemd watchdog enabled", "timeout", timeout, "pet_interval", timeout/2)

	return &watchdogPetter{
		interval: timeout / 2,
		src:      src,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Start launches the pet tick goroutine.
// In: none
// Out: none
// Ex: wd.Start() → WATCHDOG=1 every interval while alive
func (w *watchdogPetter) Start() {
	go w.run()
}

// Stop terminates the pet tick goroutine and waits for it to exit.
// In: none
// Out: none
// Ex: wd.Stop() → tick goroutine exited
func (w *watchdogPetter) Stop() {
	close(w.stop)
	<-w.done
}

// run pets on every tick until stopped.
// In: none
// Out: none - closes done on exit
// Ex: go w.run()
func (w *watchdogPetter) run() {
	defer close(w.done)

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-w.stop:
			return
		case <-ticker.C:
			w.pet()
		}
	}
}

// pet sends one WATCHDOG=1, skipped when the /livez verdict fails --
// withholding pets is the restart signal.
// In: none
// Out: none - send failures are logged, never fatal
// Ex: w.pet() → WATCHDOG=1 datagram to NOTIFY_SOCKET
func (w *watchdogPetter) pet() {
	alive, reason := telemetry.Alive(w.src, time.Now())
	if !alive {
		slog.Warn("Skipping systemd watchdog pet", "reason", reason)

		return
	}

	_, err := daemon.SdNotify(false, daemon.SdNotifyWatchdog)
	if err != nil {
		slog.Warn("sd-notify WATCHDOG failed", "error", err)
	}
}
