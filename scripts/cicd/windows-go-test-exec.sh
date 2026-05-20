#!/usr/bin/env bash
# go test -exec wrapper for Windows cgo tests.
#
# This wrapper is used to convert the PATH environment variable to Windows format and exec the generated test binaries in that environment,
# so that the Windows loader can resolve repo-local qdb DLLs and MinGW runtimes under the Buildkite/WinSW service context.
# This wrapper changes PATH only in the context of the test binary execution, and does not affect the parent scripts (if invoked through -exec flag).

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

    export PATH="${path_win}"
fi

exec "${exe}" "$@"
