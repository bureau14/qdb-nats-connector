#!/usr/bin/env bash
# Buildkite e2e (examples) test step for qdb-nats-connector.
#
# Invoked by .buildkite/steps/_build.yml after scripts/cicd/40.test-integration.sh.
# Runs the finance-ohlc golden-data example end-to-end at 10000 messages against:
#   - qdbd : already provisioned by scripts/tests/setup/start-services.sh
#   - NATS : a JetStream server provisioned just-in-time by start-nats.sh
#
# Scope: Linux only for the initial proof-of-concept. On non-Linux agents this
# is a no-op so the shared per-platform _build.yml chain stays green. NATS
# cleanup is handled by stop-nats.sh via the Buildkite pre-exit hook (mirrors
# stop-services.sh).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "$(dirname "${SCRIPT_DIR}")")"

# E2E is gated to Linux for the PoC (finance-ohlc only). Windows/FreeBSD/macOS
# agents skip it until their tooling (nats CLI, numdiff) and the bash/make
# examples flow are validated cross-platform.
if [[ "$(uname)" != "Linux" ]]; then
    echo "==[ test-e2e: skipped on $(uname) (Linux-only proof-of-concept) ]=="
    exit 0
fi

# Source shared CGO and Go-toolchain env helpers.
source "${SCRIPT_DIR}/00.common.sh"

# Required when the docker plugin propagates the host UID into the rhel7
# container: git refuses to operate on a workspace owned by a different user
# without this. Mirrors scripts/cicd/40.test-integration.sh.
git config --global --add safe.directory '*'

cd "${BASE_DIR}"

if [[ ! -d "qdb/lib" || ! -d "qdb/include" ]]; then
    echo "ERROR: expected qdb/lib and qdb/include to be present." >&2
    echo "The qdb-c-api fetch is user-managed; run it before 50.test-e2e.sh." >&2
    exit 1
fi

# Load the same CGO + Go-toolchain env as build/test so the qdb CLI tools
# (qdb/bin/qdbsh, qdb/bin/qdb_export) and the connector -- all CGO binaries --
# can locate libqdb_api.so at runtime, and so the examples Makefile's `go build`
# of qdb-data-gen / qdb-data-loader resolves the toolchain.
cicd_setup_qdb_env
cicd_setup_go_toolchain

# The examples Makefile rebuilds qdb-data-gen / qdb-data-loader via `go build`.
# Inside bureau14/builder:rhel7 the agent UID has no /etc/passwd entry, so VCS
# stamping fails; disable it the same way 20.build.sh does.
export GOFLAGS="${GOFLAGS:+${GOFLAGS} }-buildvcs=false"

echo "+++ Start NATS (JetStream)"
bash "${SCRIPT_DIR}/start-nats.sh"

echo "+++ Run e2e example: finance-ohlc (10000 messages)"
make -C examples test EXAMPLE=finance-ohlc NUM_MESSAGES=10000
