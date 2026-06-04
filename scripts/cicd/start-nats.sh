#!/usr/bin/env bash
# Provision and launch a JetStream-enabled NATS server for the e2e examples.
#
# Downloads the nats-server and nats CLI binaries just-in-time from the public
# qdb-cicd-builddeps S3 bucket (the same bucket used for Buildkite agent build
# dependencies), extracts them into <repo>/nats/bin/, and launches nats-server
# in JetStream mode on the default client port (4222).
#
# Deliberately self-contained: it does NOT source or depend on the
# scripts/tests/ submodule. Cleanup is the job of stop-nats.sh (invoked from
# the Buildkite pre-exit hook), mirroring start-services.sh / stop-services.sh
# for qdbd.

set -euo pipefail

NATS_SERVER_VERSION="2.14.1"
NATS_CLI_VERSION="0.4.0"
S3_BASE_URL="https://qdb-cicd-builddeps-20260226074339625300000001.s3.eu-west-1.amazonaws.com/nats"
NATS_PORT="4222"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "$(dirname "${SCRIPT_DIR}")")"

NATS_DIR="${BASE_DIR}/nats"
NATS_BIN_DIR="${NATS_DIR}/bin"
NATS_STORE_DIR="${NATS_DIR}/jetstream"
NATS_PID_FILE="${NATS_DIR}/nats-server.pid"
NATS_LOG_FILE="${NATS_DIR}/nats-server.log"

# Windows archives ship `.exe` binaries (nats-server.exe, nats.exe); every other
# platform ships extensionless ones. Append the suffix to both the name searched
# for inside the archive and the installed path so the fetch/launch logic works
# unchanged across MSYS/Git-bash and Unix.
case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*) EXE_EXT=".exe" ;;
    *)                    EXE_EXT="" ;;
esac
NATS_SERVER_BIN="${NATS_BIN_DIR}/nats-server${EXE_EXT}"

# Map `uname` to the os/arch tokens used in the S3 artifact filenames.
# Echoes "<os> <arch>"; exits non-zero on an unsupported platform.
detect_platform() {
    local os arch
    case "$(uname -s)" in
        Linux)   os="linux" ;;
        Darwin)  os="darwin" ;;
        FreeBSD) os="freebsd" ;;
        MINGW*|MSYS*|CYGWIN*) os="windows" ;;
        *) echo "start-nats: unsupported OS '$(uname -s)'" >&2; exit 1 ;;
    esac
    case "$(uname -m)" in
        x86_64|amd64)  arch="amd64" ;;
        aarch64|arm64) arch="arm64" ;;
        *) echo "start-nats: unsupported arch '$(uname -m)'" >&2; exit 1 ;;
    esac
    echo "${os} ${arch}"
}

# Download an archive from S3 and install one binary from it into NATS_BIN_DIR.
# Idempotent: skips the fetch when the destination binary already exists.
# Args: archive_filename  binary_name  dest_path
fetch_binary() {
    local archive="$1" binname="$2" dest="$3"
    if [[ -x "${dest}" ]]; then
        echo "start-nats: ${binname} already present at ${dest}"
        return 0
    fi
    local url="${S3_BASE_URL}/${archive}"
    local tmp
    tmp="$(mktemp -d)"
    echo "start-nats: downloading ${url}"
    curl -fsSL -o "${tmp}/${archive}" "${url}"
    case "${archive}" in
        *.tar.gz) tar -xzf "${tmp}/${archive}" -C "${tmp}" ;;
        *.zip)    unzip -q -o "${tmp}/${archive}" -d "${tmp}" ;;
        *) echo "start-nats: unknown archive type '${archive}'" >&2; rm -rf "${tmp}"; exit 1 ;;
    esac
    local found
    found="$(find "${tmp}" -type f -name "${binname}" -print -quit)"
    if [[ -z "${found}" ]]; then
        echo "start-nats: '${binname}' not found inside ${archive}" >&2
        rm -rf "${tmp}"
        exit 1
    fi
    mkdir -p "$(dirname "${dest}")"
    install -m 0755 "${found}" "${dest}"
    rm -rf "${tmp}"
    echo "start-nats: installed ${binname} -> ${dest}"
}

# Launch nats-server in JetStream mode in the background, recording its PID.
# Idempotent: returns early if a previously-started server is still alive.
launch_server() {
    if [[ -f "${NATS_PID_FILE}" ]]; then
        local oldpid
        oldpid="$(cat "${NATS_PID_FILE}" 2>/dev/null || true)"
        if [[ -n "${oldpid}" ]] && kill -0 "${oldpid}" 2>/dev/null; then
            echo "start-nats: nats-server already running (pid ${oldpid})"
            return 0
        fi
    fi
    mkdir -p "${NATS_STORE_DIR}"
    echo "start-nats: launching nats-server -js on 127.0.0.1:${NATS_PORT}"
    nohup "${NATS_SERVER_BIN}" \
        -a 127.0.0.1 -p "${NATS_PORT}" -js -sd "${NATS_STORE_DIR}" \
        > "${NATS_LOG_FILE}" 2>&1 &
    echo "$!" > "${NATS_PID_FILE}"
}

# Block until the client port accepts TCP connections, or fail after a timeout
# (dumping the server log to aid CI debugging).
wait_until_ready() {
    local i
    for i in $(seq 1 30); do
        if (exec 3<>"/dev/tcp/127.0.0.1/${NATS_PORT}") 2>/dev/null; then
            exec 3>&- 3<&- || true
            echo "start-nats: nats-server is accepting connections on ${NATS_PORT}"
            return 0
        fi
        sleep 1
    done
    echo "start-nats: nats-server did not become ready within 30s; log follows:" >&2
    cat "${NATS_LOG_FILE}" >&2 || true
    exit 1
}

# Resolve the platform, fetch both binaries, launch the server, wait for ready.
main() {
    local os arch platform server_ext
    platform="$(detect_platform)" || exit 1
    read -r os arch <<< "${platform}"

    if [[ "${os}" == "windows" ]]; then
        server_ext="zip"
    else
        server_ext="tar.gz"
    fi

    mkdir -p "${NATS_BIN_DIR}"
    fetch_binary "nats-server-v${NATS_SERVER_VERSION}-${os}-${arch}.${server_ext}" \
        "nats-server${EXE_EXT}" "${NATS_SERVER_BIN}"
    fetch_binary "nats-${NATS_CLI_VERSION}-${os}-${arch}.zip" \
        "nats${EXE_EXT}" "${NATS_BIN_DIR}/nats${EXE_EXT}"

    launch_server
    wait_until_ready
}

main "$@"
