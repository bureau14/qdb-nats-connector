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
#
# Diagnostic instrumentation (2026-05-12):
#   The Windows-only diagnostic dump and -x -v go-flag additions below are
#   blast-radius debugging added after the GCC 7.1.0 -> 16.1.0 UCRT mingw
#   upgrade caused a new cgo regression.  Search for "Windows diagnostic
#   dump" markers to locate and (later) prune the block.

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

# --- Windows diagnostic dump (temporary, added 2026-05-12) ---
# Blast-radius probe to capture the post-mingw-upgrade cgo failure context
# on Buildkite Windows agents.  Gated on uname so non-Windows builds are
# unaffected.  Every probe command is suffixed `|| true` so a missing tool
# (e.g. gcc not on PATH) reports its absence without aborting the script.
# Markers "=== Windows diagnostic dump start/end ===" make the block easy
# to grep in Buildkite logs and easy to delete in a future cleanup pass.
# (No `set +x` toggle needed here: this script runs under `set -euo
# pipefail` without `-x`, so the trace would not interleave anyway.
# `set +e` IS needed: probe commands inside (gcc, ls, etc.) intentionally
# exit non-zero in failure cases; those exits are themselves diagnostic
# information and must not abort the script under -e.)
if [[ "$(uname)" == MINGW* ]]; then
    set +e
    echo "=== Windows diagnostic dump start ==="

    echo "--- Group A: gcc fingerprint ---"
    echo "+ which gcc"; which gcc 2>&1 || true
    echo "+ gcc --version"; gcc --version 2>&1 || true
    echo "+ gcc -dumpmachine"; gcc -dumpmachine 2>&1 || true
    echo "+ gcc -dumpversion"; gcc -dumpversion 2>&1 || true
    echo "+ gcc -v -E -xc /dev/null (tail 20)"; gcc -v -E -xc /dev/null 2>&1 | tail -20 || true
    echo "+ gcc -print-search-dirs"; gcc -print-search-dirs 2>&1 || true
    echo "+ gcc predefined macros (UCRT|MSVCRT|MINGW|_WIN)"; gcc -E -dM -xc /dev/null 2>&1 | grep -iE 'UCRT|MSVCRT|MINGW|_WIN' | sort || true

    echo "--- Group B: Go env ---"
    echo "+ ${GO} version"; "${GO}" version 2>&1 || true
    echo "+ ${GO} env CC CXX CGO_ENABLED CGO_CFLAGS CGO_LDFLAGS GOROOT GOPATH GOOS GOARCH GOAMD64"
    "${GO}" env CC CXX CGO_ENABLED CGO_CFLAGS CGO_LDFLAGS GOROOT GOPATH GOOS GOARCH GOAMD64 2>&1 || true

    echo "--- Group C: mingw install contents ---"
    echo "+ ls /c/mingw64/bin (head 50)"; ls /c/mingw64/bin 2>&1 | head -50 || true
    echo "+ ls /c/mingw64/x86_64-w64-mingw32/lib (head 50)"; ls /c/mingw64/x86_64-w64-mingw32/lib 2>&1 | head -50 || true

    echo "--- Group D: linker fingerprint ---"
    echo "+ which ld"; which ld 2>&1 || true
    echo "+ ld --version"; ld --version 2>&1 || true

    echo "--- Group E: environment variables (filtered) ---"
    env | grep -iE 'GCC|CGO|MINGW|GOAMD|GOROOT|GOPATH|GOOS|GOARCH|^PATH=|^CC=' | sort || true

    echo "--- Group F: minimal cgo sanity test (with explicit stdout/stderr capture) ---"
    _diag_tmp="$(mktemp -d)"
    cat > "${_diag_tmp}/hello.c" <<'__DIAG_EOF__'
#include <stdio.h>
int main(void) { puts("ok"); return 0; }
__DIAG_EOF__
    # Redirect gcc's stdout and stderr to separate files so we can show them
    # explicitly via `cat` -- this bypasses any pipe buffering that may swallow
    # gcc's output when invoked through buildkite-agent's process tree.
    gcc -v -o "${_diag_tmp}/hello.exe" "${_diag_tmp}/hello.c" \
        > "${_diag_tmp}/gcc-stdout.log" 2> "${_diag_tmp}/gcc-stderr.log"
    _diag_gcc_exit=$?
    echo "diag: gcc exit code: ${_diag_gcc_exit}"
    echo "diag: gcc stdout (full):"
    cat "${_diag_tmp}/gcc-stdout.log" 2>&1 || echo "(stdout log missing)"
    echo "diag: gcc stderr (full):"
    cat "${_diag_tmp}/gcc-stderr.log" 2>&1 || echo "(stderr log missing)"
    echo "diag: produced files:"
    ls -la "${_diag_tmp}/" 2>&1
    if [[ "${_diag_gcc_exit}" -eq 0 && -f "${_diag_tmp}/hello.exe" ]]; then
        echo "diag: minimal C binary execution attempt:"
        "${_diag_tmp}/hello.exe" 2>&1
        echo "diag: execution exit: $?"
    fi
    rm -rf "${_diag_tmp}"
    unset _diag_tmp _diag_gcc_exit

    echo "--- Group G: process and environment context (CI vs SSH delta) ---"
    echo "+ whoami"; whoami 2>&1 || true
    echo "+ pwd"; pwd 2>&1 || true
    echo "+ TEMP=${TEMP:-unset}  TMP=${TMP:-unset}  USERPROFILE=${USERPROFILE:-unset}  LOCALAPPDATA=${LOCALAPPDATA:-unset}"
    echo "+ HOMEDRIVE=${HOMEDRIVE:-unset}  HOMEPATH=${HOMEPATH:-unset}"
    echo "+ parent process (bash \$PPID=$$):"
    powershell.exe -NoProfile -Command 'Get-WmiObject Win32_Process -Filter "ProcessId=$pid" | Select Name,ParentProcessId,CommandLine | Format-List; $p=(Get-WmiObject Win32_Process -Filter "ProcessId=$pid").ParentProcessId; while ($p -gt 0) { $pp=Get-WmiObject Win32_Process -Filter "ProcessId=$p"; if (!$pp) { break }; Write-Output "  parent: PID=$($pp.ProcessId) Name=$($pp.Name)"; $p=$pp.ParentProcessId }' 2>&1 | head -30 || true

    echo "=== Windows diagnostic dump end ==="
    set -e
fi

# --- test ---

# GO_EXTRA_FLAGS adds -x -v on Windows to capture full gcc/cgo subprocess
# output during cgo'd test-binary linking (companion to the diagnostic
# dump above; remove together when pruning).
GO_EXTRA_FLAGS=()
if [[ "$(uname)" == MINGW* ]]; then
    GO_EXTRA_FLAGS=(-x -v)
fi

echo "Running unit tests (no -tags=integration)"
# -buildvcs=false: same rhel7 uid-929/no-passwd VCS-stamping failure as 20.build.sh.
if [[ "$(uname)" == MINGW* ]]; then
    echo "Running Windows unit tests with native PATH exec wrapper"
    GO_EXTRA_FLAGS+=(-exec "bash ${SCRIPT_DIR}/windows-go-test-exec.sh")
fi

"${GO}" test "${GO_EXTRA_FLAGS[@]}" -buildvcs=false -short -race ./...
