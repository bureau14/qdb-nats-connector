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
dump_e2e_failure_logs() {
    local setup_dir="${BASE_DIR}/scripts/tests/setup"
    echo "+++ e2e failed -- connector log"
    local connector_log="${BASE_DIR}/examples/finance-ohlc-connector.log"
    [[ -f "${connector_log}" ]] && tail -100 "${connector_log}" || echo "(no connector log at ${connector_log})"

    echo "+++ e2e failed -- qdbd insecure stderr"
    local qdbd_err="${setup_dir}/qdbd_log_insecure.err.txt"
    [[ -f "${qdbd_err}" ]] && tail -120 "${qdbd_err}" || echo "(no qdbd stderr at ${qdbd_err})"

    echo "+++ e2e failed -- qdbd insecure structured log"
    local qdbd_log_dir="${setup_dir}/insecure/log"
    if [[ -d "${qdbd_log_dir}" ]]; then
        find "${qdbd_log_dir}" -type f -name '*.log' -exec tail -120 {} +
    else
        echo "(no qdbd log dir at ${qdbd_log_dir})"
    fi
}

echo "+++ Run e2e example: finance-ohlc (10000 messages)"
if ! "${MAKE_BIN}" -C examples test EXAMPLE=finance-ohlc NUM_MESSAGES=10000; then
    dump_e2e_failure_logs
    exit 1
fi
