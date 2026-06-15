#!/usr/bin/env bash
# Stop the JetStream NATS server started by start-nats.sh and remove its store.
#
# Idempotent and best-effort: a no-op when no server is running. Invoked from
# the Buildkite pre-exit hook (mirroring stop-services.sh for qdbd), so it must
# tolerate running on platforms / jobs where NATS was never started. It does
# NOT depend on the scripts/tests/ submodule.

set -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "$(dirname "${SCRIPT_DIR}")")"

NATS_DIR="${BASE_DIR}/nats"
NATS_BIN_DIR="${NATS_DIR}/bin"
NATS_STORE_DIR="${NATS_DIR}/jetstream"
NATS_PID_FILE="${NATS_DIR}/nats-server.pid"

# Send SIGTERM (then SIGKILL after a grace period) to the recorded nats-server
# PID if it is still alive, then remove the PID file. No-op without a PID file.
stop_recorded_server() {
    [[ -f "${NATS_PID_FILE}" ]] || return 0
    local pid
    pid="$(cat "${NATS_PID_FILE}" 2>/dev/null || true)"
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
        echo "stop-nats: stopping nats-server (pid ${pid})"
        kill -TERM "${pid}" 2>/dev/null || true
        local timeout=10
        while [[ ${timeout} -gt 0 ]] && kill -0 "${pid}" 2>/dev/null; do
            sleep 1
            timeout=$((timeout - 1))
        done
        if kill -0 "${pid}" 2>/dev/null; then
            echo "stop-nats: nats-server did not exit gracefully, sending SIGKILL"
            kill -KILL "${pid}" 2>/dev/null || true
        fi
    fi
    rm -f "${NATS_PID_FILE}"
}

# Reap any stray nats-server launched from NATS_BIN_DIR (covers a server that
# was reparented after start-nats.sh exited). Scoped to our own binary path so
# unrelated nats-server processes on the agent are untouched.
reap_stray_servers() {
    command -v pkill >/dev/null 2>&1 || return 0
    pkill -f "${NATS_BIN_DIR}/nats-server" 2>/dev/null || true
}

# Stop the server and delete the JetStream store so re-runs start clean.
main() {
    stop_recorded_server
    reap_stray_servers
    rm -rf "${NATS_STORE_DIR}"
    echo "stop-nats: done"
}

main "$@"
