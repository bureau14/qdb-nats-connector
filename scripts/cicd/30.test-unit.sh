#!/usr/bin/env bash
# Buildkite unit-test step for qdb-nats-connector.
# Invoked by .buildkite/steps/_test_unit.yml.
# Runs all non-integration tests across the module using ${GO} (wired by
# cicd_setup_go_toolchain via GOROOT injected from pipeline.py::_go_env_for_agent).
#
# Integration tests live under test/integration/ and are guarded by
# //go:build integration, so omitting -tags=integration excludes them.
# The previously-downloaded bin/qdb-nats-connector artifact is present on
# disk (per brief decision 4) but is not invoked by these unit tests.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "$(dirname "${SCRIPT_DIR}")")"

# Source shared CGO and Go-toolchain env helpers.
source "${SCRIPT_DIR}/00.common.sh"

# Required when the docker plugin propagates the host UID into the container:
# git refuses to operate on a workspace owned by a different user without this.
# Mirrors qdb-api-go/scripts/teamcity/10.build.sh:9.
git config --global --add safe.directory '*'

cd "${BASE_DIR}"

# --- env setup ---

if [[ ! -d "qdb/lib" || ! -d "qdb/include" ]]; then
    echo "ERROR: expected qdb/lib and qdb/include to be present." >&2
    echo "The qdb-c-api fetch is currently user-managed; run it before 30.test-unit.sh." >&2
    exit 1
fi

cicd_setup_qdb_env
cicd_setup_go_toolchain

# --- test ---

GO_EXTRA_FLAGS=()

if [[ "$(uname)" == MINGW* ]]; then
    echo "Running Windows unit tests with native PATH exec wrapper"
    # -exec wrapper intentionally does not copy DLLs. It normalizes PATH to
    # native Windows form before each generated *.test.exe starts, so the
    # Windows loader can resolve repo-local qdb DLLs and MinGW runtimes under
    # the Buildkite/WinSW service context.
    # -buildvcs=false: same rhel7 uid-929/no-passwd VCS-stamping failure as 20.build.sh.
    "${GO}" test "${GO_EXTRA_FLAGS[@]}" -buildvcs=false -short -race \
        -exec "bash ${SCRIPT_DIR}/windows-go-test-exec.sh" ./...
else
    echo "Running unit tests (no -tags=integration)"
    # -buildvcs=false: same rhel7 uid-929/no-passwd VCS-stamping failure as 20.build.sh.
    "${GO}" test "${GO_EXTRA_FLAGS[@]}" -buildvcs=false -short -race ./...
fi
