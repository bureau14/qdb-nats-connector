#!/usr/bin/env bash
# Buildkite e2e (examples) test step for qdb-nats-connector.
#
# Invoked by .buildkite/steps/_build.yml after scripts/cicd/40.test-integration.sh.
# Runs the finance-ohlc golden-data example end-to-end at 10000 messages against:
#   - qdbd : already provisioned by scripts/tests/setup/start-services.sh
#   - NATS : a JetStream server provisioned just-in-time by start-nats.sh
#
# Scope: runs on every platform (Linux, macOS, Windows, FreeBSD). NATS cleanup
# is handled by stop-nats.sh via the Buildkite pre-exit hook (mirrors
# stop-services.sh).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "$(dirname "${SCRIPT_DIR}")")"

# Source shared CGO and Go-toolchain env helpers.
source "${SCRIPT_DIR}/00.common.sh"

# Required when the docker plugin propagates the host UID into the rhel7
# container: git refuses to operate on a workspace owned by a different user
# without this. Mirrors scripts/cicd/40.test-integration.sh.
git config --global --add safe.directory '*'

cd "${BASE_DIR}"

if [[ ! -d "qdb/lib" || ! -d "qdb/include" ]]; then
    echo "ERROR: expected qdb/lib and qdb/include to be present." >&2
    echo "The qdb-c-api fetch is user-managed; run it before 50.test-e2e.sh." >&2
    exit 1
fi

# Load the same CGO + Go-toolchain env as build/test so the qdb CLI tools
# (qdb/bin/qdbsh, qdb/bin/qdb_export) and the connector -- all CGO binaries --
# can locate libqdb_api.so at runtime, and so the examples Makefile's `go build`
# of qdb-data-gen / qdb-data-loader resolves the toolchain.
cicd_setup_qdb_env
cicd_setup_go_toolchain

# The examples Makefile invokes `go` by name (not via $GO), relying on PATH.
# cicd_setup_go_toolchain exports $GO as an absolute path but prepends a
# Windows-style GOROOT\bin to PATH, which the MSYS shell that mingw32-make
# spawns cannot parse (':' is its PATH separator), so `go` is not found. On
# Windows, prepend the unix-converted go directory so the make recipes resolve
# it. No-op elsewhere (cygpath is MSYS-only; go is already on PATH there).
if command -v cygpath > /dev/null 2>&1 && [[ -n "${GO:-}" ]]; then
    PATH="$(cygpath -u "$(dirname "${GO}")"):${PATH}"
    export PATH
fi

# Windows: the Buildkite agent runs under a service whose spawned processes skip
# the PATH-based DLL search (NTSTATUS 0xC00000BE), so the connector .exe cannot
# locate libqdb_api.dll via PATH and exits before main() with no output. Windows
# always searches the executable's own directory first, so co-locate the qdb
# DLLs next to the connector binary. (The qdb/nats CLI tools and the loader work
# without this because they are not CGO-linked against the qdb C API.)
case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*)
        mkdir -p "${BASE_DIR}/bin"
        for _dlldir in "${BASE_DIR}/qdb/bin" "${BASE_DIR}/qdb/lib"; do
            [[ -d "${_dlldir}" ]] || continue
            find "${_dlldir}" -maxdepth 1 -name '*.dll' -exec cp -f {} "${BASE_DIR}/bin/" \;
        done
        echo "test-e2e: co-located qdb DLLs into ${BASE_DIR}/bin"
        ls "${BASE_DIR}/bin/"*.dll 2>/dev/null || echo "test-e2e: WARNING no qdb DLLs found to co-locate"
        ;;
esac

# The examples Makefile rebuilds qdb-data-gen / qdb-data-loader via `go build`.
# Inside bureau14/builder:rhel7 the agent UID has no /etc/passwd entry, so VCS
# stamping fails; disable it the same way 20.build.sh does.
export GOFLAGS="${GOFLAGS:+${GOFLAGS} }-buildvcs=false"

echo "+++ Start NATS (JetStream)"
bash "${SCRIPT_DIR}/start-nats.sh"

# examples/Makefile uses GNU make extensions (define/endef/foreach/eval/ifeq).
# The GNU make binary's name varies by platform: `make` on Linux/macOS; `gmake`
# on FreeBSD (whose default `make` is BSD make and aborts with "Invalid line
# endef"); and on Windows MSYS2 agents `make` is often absent while the MinGW
# toolchain ships `mingw32-make`. Pick the first candidate that exists. Mirrors
# the FreeBSD/Windows GNU-make caveat documented in 20.build.sh.
case "$(uname -s)" in
    FreeBSD)              MAKE_CANDIDATES="gmake make" ;;
    MINGW*|MSYS*|CYGWIN*) MAKE_CANDIDATES="make mingw32-make gmake" ;;
    *)                    MAKE_CANDIDATES="make gmake" ;;
esac
MAKE_BIN=""
for _m in ${MAKE_CANDIDATES}; do
    if command -v "${_m}" > /dev/null 2>&1; then
        MAKE_BIN="${_m}"
        break
    fi
done
if [[ -z "${MAKE_BIN}" ]]; then
    echo "ERROR: examples/Makefile needs GNU make; none of [${MAKE_CANDIDATES}] found on $(uname -s)." >&2
    echo "Install GNU make on this agent (FreeBSD: pkg install -y gmake; MSYS2: pacman -S make)." >&2
    exit 1
fi
echo "test-e2e: using '${MAKE_BIN}' as GNU make"

# On e2e failure, dump the connector and qdbd logs. The example's wait step
# aborts on the first connector ERROR but only echoes that one line, and the
# qdbd-side logs are never surfaced in CI -- so a server-side refusal/crash
# (seen on FreeBSD: "writer_write ... Connection refused") is undiagnosable
# from the build output alone. These files are not Buildkite artifacts.
# dump_e2e_failure_logs <example>: on e2e failure, dump that example's
# connector log and the qdbd logs. The example's wait step aborts on the first
# connector ERROR but only echoes that one line, and the qdbd-side logs are
# never surfaced in CI -- so a server-side refusal/crash (seen on FreeBSD:
# "writer_write ... Connection refused") is undiagnosable from the build output
# alone. These files are not Buildkite artifacts.
dump_e2e_failure_logs() {
    local example="$1"
    local setup_dir="${BASE_DIR}/scripts/tests/setup"
    echo "+++ e2e failed (${example}) -- connector log"
    local connector_log="${BASE_DIR}/examples/${example}-connector.log"
    [[ -f "${connector_log}" ]] && tail -100 "${connector_log}" || echo "(no connector log at ${connector_log})"

    echo "+++ e2e failed (${example}) -- qdbd insecure stderr"
    local qdbd_err="${setup_dir}/qdbd_log_insecure.err.txt"
    [[ -f "${qdbd_err}" ]] && tail -120 "${qdbd_err}" || echo "(no qdbd stderr at ${qdbd_err})"

    echo "+++ e2e failed (${example}) -- qdbd insecure structured log"
    local qdbd_log_dir="${setup_dir}/insecure/log"
    if [[ -d "${qdbd_log_dir}" ]]; then
        find "${qdbd_log_dir}" -type f -name '*.log' -exec tail -120 {} +
    else
        echo "(no qdbd log dir at ${qdbd_log_dir})"
    fi
}

# Examples run end-to-end against the shared qdbd + NATS server. Each uses its
# own NATS stream and QuasarDB tables and stops its connector before the next,
# so they run sequentially without interfering.
E2E_EXAMPLES="finance-ohlc industrial-sensor network-metrics"
for _example in ${E2E_EXAMPLES}; do
    echo "+++ Run e2e example: ${_example} (10000 messages)"
    if ! "${MAKE_BIN}" -C examples test EXAMPLE="${_example}" NUM_MESSAGES=10000; then
        dump_e2e_failure_logs "${_example}"
        exit 1
    fi
done
