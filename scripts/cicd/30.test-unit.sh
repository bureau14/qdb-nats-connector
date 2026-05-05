#!/usr/bin/env bash
# Buildkite unit-test step for qdb-nats-connector.
# Invoked by .buildkite/steps/_test_unit.yml.
# Runs all non-integration tests across the module.
#
# Integration tests live under test/integration/ and are guarded by
# //go:build integration, so omitting -tags=integration excludes them.
# The previously-downloaded bin/qdb-nats-connector artifact is present on
# disk (per brief decision 4) but is not invoked by these unit tests.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "$(dirname "${SCRIPT_DIR}")")"

# Source shared CGO env helpers; CGO is required for compilation even when
# the tests themselves exercise pure-Go logic.
source "${SCRIPT_DIR}/00.common.sh"

cd "${BASE_DIR}"

# --- env setup ---

if [[ ! -d "qdb/lib" || ! -d "qdb/include" ]]; then
    echo "ERROR: expected qdb/lib and qdb/include to be present." >&2
    echo "The qdb-c-api fetch is currently user-managed; run it before 30.test-unit.sh." >&2
    exit 1
fi

cicd_setup_qdb_env

# --- test ---

echo "Running unit tests (no -tags=integration)"
go test -short -race ./...
