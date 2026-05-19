#!/usr/bin/env bash
# go test -exec wrapper for Windows cgo/race tests.
#
# Buildkite runs under WinSW, where the native Windows loader environment can
# differ from an interactive Git Bash session. This wrapper does not copy DLLs;
# it normalizes the PATH seen by each generated *.test.exe so repo-local qdb
# DLLs and MinGW runtime DLLs are visible as native Windows paths at process
# startup.

set -euo pipefail

if (( $# < 1 )); then
    echo "usage: $0 <test-exe> [test-args...]" >&2
    exit 2
fi

exe="$1"
shift

if cygpath_bin="$(command -v cygpath 2>/dev/null)"; then
    path_win="$(${cygpath_bin} -wp "${PATH}")"
    exe="$(${cygpath_bin} -u "${exe}")"

    # Convert the CI-prepared MSYS PATH to native Windows form. The qdb and
    # MinGW entries are sourced from .envrc/cicd_setup_qdb_env; this wrapper
    # should not duplicate that path policy.
    export PATH="${path_win}"
fi

exec "${exe}" "$@"
