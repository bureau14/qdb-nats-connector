#!/usr/bin/env bash
# Buildkite lint step for qdb-nats-connector.
# Invoked by .buildkite/steps/_lint.yml inside bureau14/builder:rhel7.
# Installs golangci-lint at a pinned version into ${BASE_DIR}/bin/ (so the
# version pin lives in this repo, not in the builder AMI), then runs the
# linter.  The qdb-artifacts plugin populates qdb/ before this script runs,
# which provides the CGO headers that golangci-lint's typecheck linter needs.
#
# The Go toolchain is wired by cicd_setup_go_toolchain (00.common.sh) from
# GOROOT injected by pipeline.py::_go_env_for_agent().

set -euxo pipefail

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

# Fail early when the C API is absent; golangci-lint's typecheck linter
# invokes the Go compiler, which requires the CGO headers.
if [[ ! -d "qdb/lib" || ! -d "qdb/include" ]]; then
    echo "ERROR: expected qdb/lib and qdb/include to be present." >&2
    echo "The qdb-c-api fetch is currently user-managed; run it before 10.lint.sh." >&2
    exit 1
fi

cicd_setup_qdb_env
cicd_setup_go_toolchain
# No cicd_setup_cpu_baseline here, unlike 20/30/40/50: the lint step is not
# per-platform, so pipeline.py::_lint_step composes its env without the
# CPU_ENV layer and there is no QDB_CPU_ARCHITECTURE_CORE2 to derive from.
# Lint produces no shipped binary, so the microarch level is irrelevant.

# --- install golangci-lint ---

# Windows MSYS shells report MINGW* from uname; the installed binary gains .exe.
SUFFIX=""
if [[ "$(uname)" == MINGW* ]]; then
    SUFFIX=".exe"
fi

# Pin to a specific v2 release so the linter version is controlled by this repo,
# not by whatever happens to be in the builder image or module proxy cache.
GOLANGCI_LINT_VERSION="v2.13.1"
GOLANGCI_LINT_BIN="${BASE_DIR}/bin/golangci-lint${SUFFIX}"

if [[ ! -x "${GOLANGCI_LINT_BIN}" ]]; then
    mkdir -p "${BASE_DIR}/bin"
    # GOBIN directs go install to write the binary into ${BASE_DIR}/bin rather
    # than ${GOPATH}/bin, keeping the install local to this job.
    GOBIN="${BASE_DIR}/bin" \
        "${GO}" install "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@${GOLANGCI_LINT_VERSION}"
fi

# --- lint ---

"${GOLANGCI_LINT_BIN}" run --timeout=5m
