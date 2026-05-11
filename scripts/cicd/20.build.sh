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

# --- build ---

# Detect platform-specific binary suffix (Windows under MINGW uses .exe).
# Moved before the build block so SUFFIX is available for output path composition.
SUFFIX=""
if [[ "$(uname)" == MINGW* ]]; then
    SUFFIX=".exe"
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
GOFLAGS="-trimpath"
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
    "${GO}" build -buildvcs=false -gcflags="${GCFLAGS}" -ldflags "${LDFLAGS}" \
    -o "${BASE_DIR}/bin/qdb-nats-connector${SUFFIX}" ./cmd/qdb-nats-connector

GOFLAGS="${GOFLAGS}" GOAMD64="${GOAMD64:-}" \
    "${GO}" build -buildvcs=false -gcflags="${GCFLAGS}" -ldflags "${LDFLAGS}" \
    -o "${BASE_DIR}/bin/qdb-data-gen${SUFFIX}" ./tools/generator

GOFLAGS="${GOFLAGS}" GOAMD64="${GOAMD64:-}" \
    "${GO}" build -buildvcs=false -gcflags="${GCFLAGS}" -ldflags "${LDFLAGS}" \
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
