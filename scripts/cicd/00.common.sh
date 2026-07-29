#!/usr/bin/env bash
# Shared helpers for the CI build and test steps:
#   cicd_setup_qdb_env      -- CGO environment (sourced from .envrc)
#   cicd_setup_go_toolchain -- GOROOT/GOPATH/GO resolution
#   cicd_artifact_os        -- OS token for release archive names
#   cicd_artifact_platform  -- "<system>-64bit[-<cpu>]" token for the same
#
# The canonical CGO environment definitions live in .envrc at the repo root;
# the setup half of this file exists to:
#   1. Preserve the existing CI call contract (source 00.common.sh; cicd_setup_qdb_env).
#   2. Emit the diagnostic echo lines that CI log parsers rely on -- .envrc
#      is deliberately silent so that the direnv developer experience is clean.
#
# Sourced by 10.lint.sh, 20.build.sh, 30.test-unit.sh, 40.test-integration.sh
# and 50.test-e2e.sh; not an executable pipeline step (the leading 00. signals
# "loaded first, runs nothing").

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

# cicd_artifact_os -- emit the OS token used in QuasarDB release archive names.
#
# Mirrors CMAKE_SYSTEM_NAME lowercased, which is what quasardb's
# cmake_modules/cpack_config.cmake:20 interpolates into every published
# artifact name.  This is an existing QuasarDB-wide standard, not a naming
# scheme chosen here.
#
# Note:    deliberately NOT the Buildkite Platform.os value -- the pipeline
#          calls macOS "macos", the artifact grammar calls it "darwin".
#
# Outputs: the token on stdout; non-zero exit on an unrecognised OS.
cicd_artifact_os() {
    case "$(uname)" in
        Linux )   echo "linux"   ;;
        Darwin )  echo "darwin"  ;;
        FreeBSD ) echo "freebsd" ;;
        MINGW* )  echo "windows" ;;
        * )
            echo "cicd_artifact_os: unsupported OS '$(uname)'" >&2
            return 1
            ;;
    esac
}

export -f cicd_artifact_os

# cicd_setup_cpu_baseline -- translate QDB_CPU_ARCHITECTURE_CORE2 into GOAMD64.
#
# QDB_CPU_ARCHITECTURE_CORE2 is quasardb's canonical "build for the legacy
# baseline" switch (cmake_modules/options.cmake:1), set per-platform by
# .buildkite/pipeline.py exactly as quasardb's own pipeline does.  Honouring
# the same variable keeps a single knob driving both the archive name
# (cicd_artifact_platform) and the Go codegen level.  There is deliberately no
# positive haswell flag on either side -- absence means haswell.
#
# Mapping rationale: core2 compiles as -march=core2, i.e. up to SSSE3 only
# (quasardb cmake_modules/compiler_flags.cmake:162-164).  GOAMD64=v2 requires
# SSE4.2 and POPCNT, which Core 2 does NOT have, so v1 is the correct floor --
# v2 would emit instructions that fault on the very hardware this variant
# exists to serve.  The default build is -march=haswell (AVX2/BMI2/FMA) == v3.
# Matches the mapping already documented in adr/011-version-information.md.
#
# Inputs:  GO -- resolved by cicd_setup_go_toolchain; this function must be
#          called after it.  GOARCH is probed via ${GO} rather than uname -m
#          because uname -m is unreliable under MSYS2.
#          QDB_CPU_ARCHITECTURE_CORE2 -- "ON", or absent on haswell/ARM legs.
#
# Outputs: GOAMD64 -- exported, because go build and go test read it from the
#          environment, and 50.test-e2e.sh shells out to examples/Makefile
#          which runs its own go build.
cicd_setup_cpu_baseline() {
    local goarch
    goarch="$("${GO}" env GOARCH | tr -d '\r')"

    if [[ "${goarch}" != "amd64" ]]; then
        unset GOAMD64
        echo "cicd_setup_cpu_baseline: GOARCH=${goarch}, GOAMD64 not applicable"
        return
    fi

    if [[ "${QDB_CPU_ARCHITECTURE_CORE2:-OFF}" == "ON" ]]; then
        export GOAMD64="v1"
    else
        export GOAMD64="v3"
    fi

    echo "cicd_setup_cpu_baseline: QDB_CPU_ARCHITECTURE_CORE2=${QDB_CPU_ARCHITECTURE_CORE2:-OFF} GOAMD64=${GOAMD64}"
}

export -f cicd_setup_cpu_baseline

# cicd_artifact_platform -- emit the "<system>-64bit[-<cpu>]" platform token.
#
# Port of quasardb's cmake_modules/cpu_arch.cmake:10-36.  The branch order is
# the CMake's, not ours: core2 wins over the ARM processor suffix, and a
# default x86-64 (haswell) build gets no cpu suffix at all.  Combined with
# cpack_config.cmake:20 this reproduces the published grammar
#
#     qdb-<version>-<system>-64bit[-<cpu>]-<component>.<ext>
#
# so connector archives sort alongside the server/c-api/utils archives on the
# download site.  qdb-pkg-archives/repackage.sh:62-71 repacks .tar.zst into
# .tar.gz/.zip preserving the basename verbatim, so this token ends up in the
# public artifact name unchanged.
#
# Inputs:  QDB_CPU_ARCHITECTURE_CORE2 -- "ON" selects the legacy baseline.
#          This is quasardb's own switch (cmake_modules/options.cmake:1),
#          honoured here so one variable drives both the archive name and the
#          Go codegen level (see cicd_setup_cpu_baseline).
#
# Outputs: the token on stdout.
cicd_artifact_platform() {
    local system machine

    system="$(cicd_artifact_os)"

    if [[ "${QDB_CPU_ARCHITECTURE_CORE2:-OFF}" == "ON" ]]; then
        echo "${system}-64bit-core2"
        return
    fi

    # QDB_CPU_IS_ARM equivalent.  For a native build uname -m *is*
    # CMAKE_SYSTEM_PROCESSOR, which is precisely why Linux emits "aarch64"
    # while Darwin emits "arm64" -- that asymmetry is inherited from the
    # published names rather than being a bug to normalise away.  MSYS2
    # reports x86_64 and there is no Windows ARM target, so Windows always
    # falls through to the no-suffix branch.
    #
    # 64bit is hardcoded: the connector has no 32-bit target.
    machine="$(uname -m)"
    case "${machine}" in
        aarch64 | arm64 | arm* ) echo "${system}-64bit-${machine}" ;;
        * )                      echo "${system}-64bit" ;;
    esac
}

export -f cicd_artifact_platform
