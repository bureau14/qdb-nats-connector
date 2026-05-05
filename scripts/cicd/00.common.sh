#!/usr/bin/env bash
# CGO env setup helpers shared by buildkite steps.
# Sourced by 20.build.sh and 30.test-unit.sh; not an executable
# pipeline step (the leading 00. signals "loaded first, runs nothing").

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

# cicd_setup_qdb_env -- configure CGO environment variables to locate the
# QuasarDB C API libraries installed at ${BASE_DIR}/qdb.
#
# Inputs:  BASE_DIR (repo root, exported above)
#          Expects qdb/lib and qdb/include to exist (populated by the
#          user-managed qdb-c-api fetch step before CI scripts run).
#
# Outputs: Exports a subset of the following env vars depending on OS:
#   Linux / FreeBSD : LD_LIBRARY_PATH
#   Darwin          : DYLD_LIBRARY_PATH, CGO_CFLAGS, CGO_LDFLAGS
#   MINGW (Windows) : PATH
#
# Supported uname values: Linux, FreeBSD, Darwin, MINGW*.
cicd_setup_qdb_env() {
    local qdb_api_dir="${BASE_DIR}/qdb"
    local qdb_lib_dir="${BASE_DIR}/qdb/lib"

    local os
    os="$(uname)"

    case "${os}" in
        Linux|FreeBSD)
            LD_LIBRARY_PATH="${qdb_lib_dir}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
            export LD_LIBRARY_PATH
            echo "cicd_setup_qdb_env: LD_LIBRARY_PATH=${LD_LIBRARY_PATH}"
            ;;
        Darwin)
            DYLD_LIBRARY_PATH="${qdb_lib_dir}${DYLD_LIBRARY_PATH:+:${DYLD_LIBRARY_PATH}}"
            CGO_CFLAGS="${CGO_CFLAGS:+${CGO_CFLAGS} }-I${qdb_api_dir}/include"
            CGO_LDFLAGS="${CGO_LDFLAGS:+${CGO_LDFLAGS} }-L${qdb_lib_dir} -Wl,-rpath -Wl,${qdb_lib_dir}"
            export DYLD_LIBRARY_PATH CGO_CFLAGS CGO_LDFLAGS
            echo "cicd_setup_qdb_env: DYLD_LIBRARY_PATH=${DYLD_LIBRARY_PATH}"
            echo "cicd_setup_qdb_env: CGO_CFLAGS=${CGO_CFLAGS}"
            echo "cicd_setup_qdb_env: CGO_LDFLAGS=${CGO_LDFLAGS}"
            ;;
        MINGW*)
            PATH="${qdb_lib_dir}:${qdb_api_dir}/bin:${PATH}"
            export PATH
            echo "cicd_setup_qdb_env: PATH prepended with qdb/lib and qdb/bin"
            ;;
        *)
            echo "cicd_setup_qdb_env: unknown OS '${os}'" >&2
            return 1
            ;;
    esac
}

export -f cicd_setup_qdb_env
