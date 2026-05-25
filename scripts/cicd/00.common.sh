#!/usr/bin/env bash
# Thin shim that exposes cicd_setup_qdb_env for consumption by the CI build
# and test steps.  The canonical CGO environment definitions live in .envrc
# at the repo root; this file exists to:
#   1. Preserve the existing CI call contract (source 00.common.sh; cicd_setup_qdb_env).
#   2. Emit the diagnostic echo lines that CI log parsers rely on -- .envrc
#      is deliberately silent so that the direnv developer experience is clean.
#
# Sourced by 20.build.sh and 30.test-unit.sh; not an executable pipeline step
# (the leading 00. signals "loaded first, runs nothing").

set -eu

# Resolve repo root (two levels up from this file: scripts/cicd/ -> scripts/ -> repo root).
# Use realpath when available for symlink resolution; fall back to pwd-based expansion.
if command -v realpath > /dev/null 2>&1; then
    _CICD_SCRIPT_DIR="$(realpath "$(dirname "${BASH_SOURCE[0]}")")"
else
    _CICD_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fi
BASE_DIR="$(dirname "$(dirname "${_CICD_SCRIPT_DIR}")")"
export BASE_DIR
export TEST_REPORT_DIR="${BASE_DIR}/test-reports"
mkdir -p "${TEST_REPORT_DIR}"

# cicd_setup_qdb_env -- source .envrc to populate CGO environment variables,
# then log the OS-specific subset of those variables for CI diagnostics.
#
# Inputs:  BASE_DIR (repo root, exported above)
#          Expects qdb/lib and qdb/include to exist (populated by the
#          user-managed qdb-c-api fetch step before CI scripts run).
#
# Outputs: Exports a subset of the following env vars depending on OS
#          (the full matrix is defined in .envrc):
#   Linux / FreeBSD : LD_LIBRARY_PATH, CGO_CFLAGS, CGO_LDFLAGS
#   Darwin          : DYLD_LIBRARY_PATH, CGO_CFLAGS, CGO_LDFLAGS
#   MINGW (Windows) : PATH, CGO_CFLAGS, CGO_LDFLAGS
cicd_setup_qdb_env() {
    # Source the canonical file.  bash functions share the parent shell's
    # environment, so all `export` statements in .envrc propagate to the
    # caller -- this is the load-bearing assumption of the shim design.
    source "${BASE_DIR}/.envrc"

    local os
    os="$(uname)"

    # Emit the same diagnostic lines the previous monolithic cicd_setup_qdb_env
    # produced so that any CI log greps on "cicd_setup_qdb_env: " continue to
    # match.  The variables being echoed are already exported by the source
    # call above.
    case "${os}" in
        Linux|FreeBSD)
            echo "cicd_setup_qdb_env: LD_LIBRARY_PATH=${LD_LIBRARY_PATH}"
            echo "cicd_setup_qdb_env: CGO_CFLAGS=${CGO_CFLAGS}"
            echo "cicd_setup_qdb_env: CGO_LDFLAGS=${CGO_LDFLAGS}"
            ;;
        Darwin)
            echo "cicd_setup_qdb_env: DYLD_LIBRARY_PATH=${DYLD_LIBRARY_PATH}"
            echo "cicd_setup_qdb_env: CGO_CFLAGS=${CGO_CFLAGS}"
            echo "cicd_setup_qdb_env: CGO_LDFLAGS=${CGO_LDFLAGS}"
            ;;
        MINGW*)
            echo "cicd_setup_qdb_env: PATH prepended with qdb/lib and qdb/bin"
            echo "cicd_setup_qdb_env: CGO_CFLAGS=${CGO_CFLAGS}"
            echo "cicd_setup_qdb_env: CGO_LDFLAGS=${CGO_LDFLAGS}"
            ;;
        *)
            # .envrc already returned 1 for an unknown OS under set -e, so this
            # branch is unreachable in normal execution.  Kept for defense-in-depth
            # in case .envrc is sourced under a shell that does not honour set -e.
            echo "cicd_setup_qdb_env: unknown OS '${os}'" >&2
            return 1
            ;;
    esac
}

export -f cicd_setup_qdb_env

# cicd_setup_go_toolchain -- derive GO from GOROOT and validate the binary.
#
# Inputs:  GOROOT  -- set by .buildkite/pipeline.py::_go_env_for_agent() from
#                     the per-OS QDB_CICD_AGENT_GO124_ROOT agent env var; the
#                     Buildkite agent shell substitutes the value at job-start.
#          GOPATH  -- set by the same mechanism from QDB_CICD_AGENT_GO124_PATH.
#
# Outputs: GO      -- absolute path to the go binary (${GOROOT}/bin/go[.exe]).
#          GOROOT, GOPATH, PATH -- re-exported (PATH prepended with ${GOROOT}/bin).
#
# Note:    This function intentionally does NOT source .envrc.  CGO-env wiring
#          is the orthogonal concern of cicd_setup_qdb_env; the two functions
#          are called in sequence but are fully independent.
cicd_setup_go_toolchain() {
    if [[ -z "${GOROOT:-}" ]]; then
        echo "cicd_setup_go_toolchain: GOROOT is not set." >&2
        echo "Expected injection from pipeline.py::_go_env_for_agent() via QDB_CICD_AGENT_GO124_ROOT." >&2
        return 1
    fi

    # Windows MSYS shells report MINGW* from uname; the go binary uses .exe there.
    local suffix=""
    if [[ "$(uname)" == MINGW* ]]; then
        suffix=".exe"
    fi

    GO="${GOROOT}/bin/go${suffix}"

    if [[ ! -x "${GO}" ]]; then
        echo "cicd_setup_go_toolchain: go binary not executable at ${GO}" >&2
        echo "cicd_setup_go_toolchain: GOROOT=${GOROOT}" >&2
        echo "cicd_setup_go_toolchain: contents of ${GOROOT}/bin:" >&2
        ls "${GOROOT}/bin" >&2 || true
        return 1
    fi

    export GO GOROOT GOPATH="${GOPATH:-}"
    PATH="${GOROOT}/bin:${PATH}"
    export PATH

    # We need the output of `go test` to be in JUnit format for Buildkite's test reporting, but `go test` doesn't support that natively.
    # We use the go-junit-report tool to convert the output of `go test` into JUnit XML format.
    # Validate that go-junit-report is installed, if not install it
    if ! command -v go-junit-report > /dev/null 2>&1; then
        echo "go-junit-report not found, installing"
        ${GO} install github.com/jstemmer/go-junit-report/v2@latest
    else
        echo "go-junit-report is already installed; skipping installation."
    fi
    export GO_JUNIT_REPORT="${GOPATH}/bin/go-junit-report"
    $GO_JUNIT_REPORT --version

    echo "cicd_setup_go_toolchain: GOROOT=${GOROOT}"
    echo "cicd_setup_go_toolchain: GOPATH=${GOPATH:-}"
    echo "cicd_setup_go_toolchain: GO=${GO}"
    echo "cicd_setup_go_toolchain: $("${GO}" version)"
    echo "cicd_setup_go_toolchain: GO_JUNIT_REPORT=${GO_JUNIT_REPORT}"
}

export -f cicd_setup_go_toolchain
