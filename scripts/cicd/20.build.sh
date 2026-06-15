#!/usr/bin/env bash
# Buildkite build step for qdb-nats-connector.
# Invoked by .buildkite/steps/_build.yml.
# Compiles all three connector binaries via ${GO} build directly (no make),
# and packages bin/qdb-nats-connector into
# artifacts/qdb-${VERSION}-${OS}-${ARCH}-nats-connector.tar.zst
# for upload.  The Go toolchain is wired by cicd_setup_go_toolchain (00.common.sh),
# which derives GO from GOROOT injected by pipeline.py::_go_env_for_agent().
#
# The qdb-c-api fetch (populating qdb/lib and qdb/include) is the
# user's responsibility and must run before this script.
#
# Diagnostic instrumentation (2026-05-12):
#   The Windows-only diagnostic dump and -x -v go-flag additions below are
#   blast-radius debugging added after the GCC 7.1.0 -> 16.1.0 UCRT mingw
#   upgrade caused a new cgo regression.  Search for "Windows diagnostic
#   dump" markers to locate and (later) prune the block.

set -euxo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "$(dirname "${SCRIPT_DIR}")")"

# Source shared CGO and Go-toolchain env helpers (00.common.sh).
source "${SCRIPT_DIR}/00.common.sh"

# Required when the docker plugin propagates the host UID into the container:
# git refuses to operate on a workspace owned by a different user without this.
# Mirrors qdb-api-go/scripts/teamcity/10.build.sh:9.
git config --global --add safe.directory '*'

cd "${BASE_DIR}"

# --- env setup ---

# Fail early with a clear message when the C API is absent.
# The qdb/ directory is populated by the user-managed fetch step; if it
# is missing the CGO compilation will fail with confusing linker errors.
if [[ ! -d "qdb/lib" || ! -d "qdb/include" ]]; then
    echo "ERROR: expected qdb/lib and qdb/include to be present." >&2
    echo "The qdb-c-api fetch is currently user-managed; run it before 20.build.sh." >&2
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
if [[ "$(uname)" == MINGW* ]]; then
    # Disable -x (trace) AND -e (abort-on-error) for the diag block: every
    # probe inside is best-effort, and non-zero exits from gcc/ls/etc. are
    # themselves diagnostic information that must not abort the script.
    set +ex
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

    # --- Group H: toolchain bisection (which gcc sub-tool dies, and why) ---
    # Builds 34 and 35 prove a *plain* `gcc hello.c` (Group F, no cgo, no Go)
    # exits 1 with no error after the driver echoes the `as.exe -v` line --
    # i.e. cc1 runs and prints its banner, then the assembler stage produces
    # zero output and the driver fails.  So the mingw toolchain on the agent
    # is broken independently of cgo/Go/temp-dir.  This group resolves the
    # exact sub-tool binaries gcc will exec, runs each directly with stderr
    # captured, then bisects the pipeline stage by stage (-E -> -S -> as ->
    # -c -> link) on a trivial file so the failing stage and its real error
    # (missing binary, DLL load failure, crash) are unambiguous.
    echo "--- Group H: toolchain bisection ---"
    echo "+ whoami.exe /user /groups (Windows token; MSYS 'whoami' shadows it)"
    whoami.exe /user /groups 2>&1 | head -8 || true

    echo "+ resolved sub-tool paths (gcc -print-prog-name):"
    for _t in cc1 as collect2 ld; do
        _p="$(gcc -print-prog-name=${_t} 2>&1)"
        echo "  ${_t} -> ${_p}"
        # gcc returns the bare name when it cannot resolve a real path.
        if [[ "${_p}" != "${_t}" && -e "${_p}" ]]; then
            ls -l "${_p}" 2>&1 || true
        else
            echo "  (not a resolvable path; PATH lookup of ${_t}:)"; which "${_t}" 2>&1 || true
            command -v "${_t}.exe" >/dev/null 2>&1 && ls -l "$(command -v ${_t}.exe)" 2>&1 || true
        fi
    done

    echo "+ direct sub-tool --version (exit code each; silent+nonzero => broken/DLL-load failure):"
    for _c in "as --version" "ld --version" "gcc -print-prog-name=collect2"; do
        echo "  \$ ${_c}"
        eval "${_c}" > "${TMPDIR:-/tmp}/h_tool.out" 2> "${TMPDIR:-/tmp}/h_tool.err"
        echo "    exit=$?  stdout1=$(head -1 "${TMPDIR:-/tmp}/h_tool.out" 2>/dev/null)  stderr1=$(head -1 "${TMPDIR:-/tmp}/h_tool.err" 2>/dev/null)"
    done
    _as_real="$(gcc -print-prog-name=as 2>/dev/null)"
    if [[ -n "${_as_real}" && -e "${_as_real}" ]]; then
        echo "+ '${_as_real}' --version (the exact assembler gcc execs):"
        "${_as_real}" --version > "${TMPDIR:-/tmp}/as.out" 2> "${TMPDIR:-/tmp}/as.err"
        echo "  exit=$?"; echo "  stdout:"; cat "${TMPDIR:-/tmp}/as.out" 2>&1 | head -3
        echo "  stderr:"; cat "${TMPDIR:-/tmp}/as.err" 2>&1 | head -10
        echo "+ DLL imports of that as.exe (objdump -p; missing dep => silent exit):"
        objdump -p "${_as_real}" 2>&1 | grep -i "DLL Name" | sort -u || true
        command -v ldd >/dev/null 2>&1 && { echo "+ ldd as.exe:"; ldd "${_as_real}" 2>&1 | head -30; }
    fi

    echo "+ staged compile bisection on a trivial file:"
    _hp="${TMPDIR:-/tmp}/htc"
    rm -rf "${_hp}"; mkdir -p "${_hp}"
    printf 'int main(void){return 0;}\n' > "${_hp}/h.c"
    ( cd "${_hp}" \
      && { echo "  [1] gcc -E (preprocess)"; gcc -E h.c            > h.i   2> e1; echo "      exit=$? $(tr -d '\r' <e1 | tail -1)"; } \
      && { echo "  [2] gcc -S (cc1 -> .s)"; gcc -S h.c             -o h.s  2> e2; echo "      exit=$? $(tr -d '\r' <e2 | tail -1)"; } \
      && { echo "  [3] as h.s (assemble)"; "${_as_real:-as}" h.s   -o h.o  2> e3; echo "      exit=$? $(tr -d '\r' <e3 | tail -1)"; } \
      && { echo "  [4] gcc -c (drive cc1+as)"; gcc -c h.c          -o hc.o 2> e4; echo "      exit=$? $(tr -d '\r' <e4 | tail -1)"; } \
      && { echo "  [5] gcc -pipe -c (no temp file)"; gcc -pipe -c h.c -o hp.o 2> e5; echo "      exit=$? $(tr -d '\r' <e5 | tail -1)"; } \
      && { echo "  [6] gcc link (collect2/ld)"; gcc h.c            -o h.exe 2> e6; echo "      exit=$? $(tr -d '\r' <e6 | tail -1)"; } \
      ) 2>&1 || true
    echo "  produced:"; ls -l "${_hp}" 2>&1 | grep -E '\.(i|s|o|exe)$' || echo "  (none)"

    # The bash `exit=$?` above is truncated to 0-255 and loses the Win32 /
    # NTSTATUS code that distinguishes WHY a sub-tool died:
    #   0xC0000135 STATUS_DLL_NOT_FOUND   -> a dependency DLL is missing
    #   0xC0000142 STATUS_DLL_INIT_FAILED -> a dependency DLL failed to init
    #   0xC0000005 ACCESS_VIOLATION       -> the tool crashed
    #   1                                  -> a real assembler/compiler error
    # Re-run the assembler stage standalone and surface the raw code via cmd
    # (%ERRORLEVEL% is the full signed Win32 code) and PowerShell (-PassThru
    # ExitCode, printed hex).  This is the single most decisive datum.
    echo "+ assembler-stage NTSTATUS post-mortem:"
    _asm="${_as_real:-as}"
    if [[ -d "${_hp}" && -f "${_hp}/h.s" ]]; then
        _swin="$(cygpath -w "${_hp}/h.s" 2>/dev/null || echo "${_hp}/h.s")"
        _owin="$(cygpath -w "${_hp}/h2.o" 2>/dev/null || echo "${_hp}/h2.o")"
        _awin="$(cygpath -w "${_asm}" 2>/dev/null || echo "${_asm}")"
        cmd.exe /c ""${_awin}" "${_swin}" -o "${_owin}" & echo as_errorlevel=%ERRORLEVEL%" 2>&1 | tr -d '\r' | tail -2 || true
        powershell.exe -NoProfile -Command "\$p=Start-Process -FilePath '${_awin}' -ArgumentList '\"${_swin}\"','-o','\"${_owin}\"' -NoNewWindow -Wait -PassThru -EA Stop; 'as ExitCode=0x{0:X8} ({0})' -f \$p.ExitCode" 2>&1 | tr -d '\r' | tail -1 || true
    else
        echo "  (no h.s from stage [2]; cc1 stage already failed -- see bisection above)"
    fi
    echo "+ Windows Error Reporting reports (last 5; exception/fault module):"
    powershell.exe -NoProfile -Command '
      try { Get-WinEvent -FilterHashtable @{LogName="Application"; ProviderName="Windows Error Reporting"} -MaxEvents 5 -EA Stop |
        Select-Object TimeCreated,@{n="M";e={(($_.Message -split [Environment]::NewLine) | Select-Object -First 6) -join " | "}} |
        Format-List } catch { "no WER events: $($_.Exception.Message)" }' 2>&1 | head -40 || true
    echo "+ AV / EDR minifilters (fltmc) + Defender realtime state:"
    fltmc.exe filters 2>&1 | head -15 || true
    powershell.exe -NoProfile -Command '(Get-MpComputerStatus | Select RealTimeProtectionEnabled,AntivirusEnabled | Format-List) 2>$null; (Get-MpPreference | Select @{n="Excl";e={$_.ExclusionPath -join ";"}} | Format-List) 2>$null' 2>&1 | head -12 || true

    echo "+ recent Windows app-crash / Defender events (as.exe/gcc/cc1):"
    powershell.exe -NoProfile -Command '
      try { Get-WinEvent -FilterHashtable @{LogName="Application"; Id=1000,1001,1026} -MaxEvents 8 -EA Stop |
        Select-Object TimeCreated,Id,@{n="M";e={($_.Message -split [Environment]::NewLine)[0]}} |
        Format-Table -Auto -Wrap } catch { "no Application crash events: $($_.Exception.Message)" }
      try { Get-WinEvent -FilterHashtable @{LogName="Microsoft-Windows-Windows Defender/Operational"; Id=1116,1117} -MaxEvents 5 -EA Stop |
        Select-Object TimeCreated,Id,@{n="M";e={($_.Message -split [Environment]::NewLine)[0]}} |
        Format-Table -Auto -Wrap } catch { "no Defender block events" }' 2>&1 | head -30 || true
    rm -rf "${_hp}" 2>/dev/null
    unset _t _p _c _as_real _hp

    echo "=== Windows diagnostic dump end ==="
    set -ex
fi

# --- build ---

# Detect platform-specific binary suffix (Windows under MINGW uses .exe).
# Moved before the build block so SUFFIX is available for output path composition.
# GO_EXTRA_FLAGS adds -x -v on Windows to capture full gcc/cgo subprocess output
# (companion to the diagnostic dump above; remove together when pruning).
SUFFIX=""
GO_EXTRA_FLAGS=()
if [[ "$(uname)" == MINGW* ]]; then
    SUFFIX=".exe"
    GO_EXTRA_FLAGS=(-x -v)
fi

# Inline the same flag composition the Makefile uses in BUILD_MODE=release.
# Calling ${GO} directly avoids GNU-make dependency on FreeBSD (BSD make
# rejects ifeq) and Windows (no GNU make on MSYS2 agents).
VERSION="$(cat VERSION)"
GIT_SHA="$(git rev-parse HEAD)"
BUILD_TIME="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
KERNEL_VERSION="$(uname -r)"

# Derive ${OS} and ${ARCH} for the release archive name.  Mirrors
# ~/git/qdb-api-rest/scripts/common.sh:111-160 verbatim so the archive
# naming matches the qdb-api-rest convention: qdb-${VERSION}-${OS}-${ARCH}-...
case "$(uname)" in
    Darwin | Linux | FreeBSD )
        ARCH="$(uname -m)"
        if [[ "${ARCH}" == "x86_64" || "${ARCH}" == "amd64" ]]; then
            ARCH="amd64"
        else
            ARCH="aarch64"
        fi
        ;;
    MINGW* )
        # Windows agents are always amd64; uname -m is unreliable under MSYS2.
        ARCH="amd64"
        ;;
    * )
        echo "ERROR: unable to probe ARCH for $(uname)" >&2
        exit 1
        ;;
esac

case "$(uname)" in
    MINGW* )    OS="windows" ;;
    Darwin )    OS="darwin" ;;
    Linux )     OS="linux" ;;
    FreeBSD )   OS="freebsd" ;;
    * )
        echo "ERROR: unable to probe OS for $(uname)" >&2
        exit 1
        ;;
esac

BUILD_MODE="release"
# -mod=vendor: build strictly from the committed vendor/ tree; fail loudly
# instead of silently fetching from the proxy if vendor/ is missing or stale.
GOFLAGS="-trimpath -mod=vendor"
GCFLAGS=""
LDFLAGS="-X main.version=${VERSION} \
         -X main.commit=${GIT_SHA} \
         -X main.buildTime=${BUILD_TIME} \
         -X main.buildMode=${BUILD_MODE} \
         -X main.goamd64=${GOAMD64:-} \
         -X main.kernelVersion=${KERNEL_VERSION}"

mkdir -p "${BASE_DIR}/bin"

# -buildvcs=false: Go 1.18+ auto VCS stamping fails inside bureau14/builder:rhel7
# because uid 929 has no /etc/passwd entry, so git rejects the repo as unsafe
# in the subprocess go-build spawns.  The commit SHA is already injected via
# -X main.commit=${GIT_SHA}, so auto-stamping is redundant here.
GOFLAGS="${GOFLAGS}" GOAMD64="${GOAMD64:-}" \
    "${GO}" build "${GO_EXTRA_FLAGS[@]+"${GO_EXTRA_FLAGS[@]}"}" -buildvcs=false -gcflags="${GCFLAGS}" -ldflags "${LDFLAGS}" \
    -o "${BASE_DIR}/bin/qdb-nats-connector${SUFFIX}" ./cmd/qdb-nats-connector

GOFLAGS="${GOFLAGS}" GOAMD64="${GOAMD64:-}" \
    "${GO}" build "${GO_EXTRA_FLAGS[@]+"${GO_EXTRA_FLAGS[@]}"}" -buildvcs=false -gcflags="${GCFLAGS}" -ldflags "${LDFLAGS}" \
    -o "${BASE_DIR}/bin/qdb-data-gen${SUFFIX}" ./tools/generator

GOFLAGS="${GOFLAGS}" GOAMD64="${GOAMD64:-}" \
    "${GO}" build "${GO_EXTRA_FLAGS[@]+"${GO_EXTRA_FLAGS[@]}"}" -buildvcs=false -gcflags="${GCFLAGS}" -ldflags "${LDFLAGS}" \
    -o "${BASE_DIR}/bin/qdb-data-loader${SUFFIX}" ./tools/loader

CONNECTOR_BIN="${BASE_DIR}/bin/qdb-nats-connector${SUFFIX}"

if [[ ! -f "${CONNECTOR_BIN}" ]]; then
    echo "ERROR: expected binary not found at ${CONNECTOR_BIN}" >&2
    exit 1
fi

# --- package ---

mkdir -p "${BASE_DIR}/artifacts"

ARCHIVE="${BASE_DIR}/artifacts/qdb-${VERSION}-${OS}-${ARCH}-nats-connector.tar.zst"

# --use-compress-program=zstd works on both GNU tar (Linux) and BSD tar
# (FreeBSD, macOS) as long as zstd is on PATH; avoids format-flag divergence.
tar --use-compress-program=zstd \
    -cf "${ARCHIVE}" \
    -C "${BASE_DIR}" \
    "bin/qdb-nats-connector${SUFFIX}"

echo "Packaged: ${ARCHIVE} ($(du -h "${ARCHIVE}" | cut -f1))"
