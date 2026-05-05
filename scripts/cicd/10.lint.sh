#!/usr/bin/env bash
# Buildkite lint step for qdb-nats-connector.
# Invoked by .buildkite/steps/_lint.yml inside bureau14/builder:rhel7.
# Runs golangci-lint with zero tolerance for violations (per CLAUDE.md).
# Failure mode: non-zero exit propagates immediately; set -e handles it.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "$(dirname "${SCRIPT_DIR}")")"

cd "${BASE_DIR}"

# Warn when qdb/ is absent -- golangci-lint may fail on CGO typechecking,
# but we let it surface the precise error rather than masking it here.
if [[ ! -d "qdb" ]]; then
    echo "WARNING: qdb/ directory not found -- CGO typechecking may fail." >&2
    echo "Populate qdb/lib and qdb/include via the qdb-c-api fetch step." >&2
fi

if ! command -v golangci-lint > /dev/null 2>&1; then
    echo "golangci-lint not found in PATH; install it on the agent or in" >&2
    echo "the docker image bureau14/builder:rhel7." >&2
    exit 1
fi

golangci-lint run ./...
