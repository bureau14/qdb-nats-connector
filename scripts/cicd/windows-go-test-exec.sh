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
    qdb_bin_win="$(${cygpath_bin} -w "${QDB_API_DIR}/bin")"
    qdb_lib_win="$(${cygpath_bin} -w "${QDB_LIB_DIR}")"
    mingw_bin_win="C:\\mingw64\\bin"
    path_win="$(${cygpath_bin} -wp "${PATH}")"
    exe="$(${cygpath_bin} -u "${exe}")"

    # Important: do all cygpath calls before exporting a native semicolon PATH.
    # Once PATH is in native Windows form, MSYS bash can no longer find Unix
    # tools by command lookup, but the Windows test process will receive the
    # loader-friendly native PATH it needs.
    export PATH="${qdb_bin_win};${qdb_lib_win};${mingw_bin_win};${path_win}"
else
    export PATH="${QDB_API_DIR}/bin:${QDB_LIB_DIR}:/c/mingw64/bin:${PATH}"
fi

exec "${exe}" "$@"
