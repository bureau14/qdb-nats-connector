#!/usr/bin/env bash
# Buildkite build step for qdb-nats-connector.
# Invoked by .buildkite/steps/_build.yml.
# Compiles all three connector binaries via ${GO} build directly (no make),
# and packages bin/qdb-nats-connector into
# artifacts/qdb-${VERSION}-<system>-64bit[-<cpu>]-nats-connector.tar.zst
# for upload -- quasardb's CPack naming grammar, produced by
# cicd_artifact_platform (00.common.sh).
# The Go toolchain is wired by cicd_setup_go_toolchain (00.common.sh),
# which derives GO from GOROOT injected by pipeline.py::_go_env_for_agent().
#
# The qdb-c-api fetch (populating qdb/lib and qdb/include) is the
# user's responsibility and must run before this script.
#
# On Linux the C API is linked statically from qdb/lib/libqdb_api.a (see
# .envrc), so the Linux archive ships the connector binary alone; the other
# platforms co-locate the shared libqdb_api runtime next to it.

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

# Linux links the static archive (vendored qdb-api-go library_link.go); a
# c-api package without it means a quasardb build that predates QDB-19063,
# which cgo would only report as a bare "cannot find -lqdb_api" from the
# linker.
if [[ "$(uname)" == "Linux" && ! -f "qdb/lib/libqdb_api.a" ]]; then
    echo "ERROR: expected qdb/lib/libqdb_api.a to be present for the static Linux link." >&2
    exit 1
fi

cicd_setup_qdb_env
cicd_setup_go_toolchain
cicd_setup_cpu_baseline

# --- build ---

# Detect platform-specific binary suffix (Windows under MINGW uses .exe).
# Defined before the build block so SUFFIX is available for output path composition.
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

# ${OS} selects the platform-specific packaging bits below: which libqdb_api
# runtime to co-locate (glob further down) and whether to ship the systemd
# unit.  The archive *name* no longer derives from it -- that is
# cicd_artifact_platform's job, which reproduces quasardb's CPack grammar.
OS="$(cicd_artifact_os)"

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

# The vendored qdb-api-go decides the link mode (static on Linux); guard
# that a future vendor bump or flag change does not silently record
# libqdb_api.so as a runtime dependency again, which would fail on hosts
# that do not ship it.
if [[ "$(uname)" == "Linux" ]]; then
    for _bin in qdb-nats-connector qdb-data-gen qdb-data-loader; do
        if readelf -d "${BASE_DIR}/bin/${_bin}" | grep -q 'NEEDED.*libqdb_api'; then
            echo "ERROR: ${_bin} links libqdb_api.so dynamically; expected the static archive." >&2
            readelf -d "${BASE_DIR}/bin/${_bin}" | grep NEEDED >&2
            exit 1
        fi
    done
    unset _bin
fi

# --- package ---
#
# The archive is a self-contained, relocatable dependency artifact:
#   bin/  connector binary; on Linux it carries libqdb_api statically, on
#         the other platforms the libqdb_api runtime is co-located next to it
#         (mirrors qdb-api-rest scripts/teamcity/30.package.sh) and the
#         binary resolves it via the $ORIGIN/@loader_path rpath baked in
#         .envrc, or exe-dir DLL search on Windows.
#   etc/  example parser configs
# qdb-pkg-archives repackages this .tar.zst into the external .tar.gz
# verbatim, so the internal and external layouts are identical.

mkdir -p "${BASE_DIR}/artifacts"

PKG_DIR="${BASE_DIR}/artifacts/pkg"
rm -rf "${PKG_DIR}"
mkdir -p "${PKG_DIR}/bin" "${PKG_DIR}/etc"

cp "${CONNECTOR_BIN}" "${PKG_DIR}/bin/"

shopt -s nullglob
case "${OS}" in
    linux )   QDB_RUNTIME_LIBS=() ;;
    windows ) QDB_RUNTIME_LIBS=("${BASE_DIR}/qdb/bin/qdb_api.dll") ;;
    darwin )  QDB_RUNTIME_LIBS=("${BASE_DIR}/qdb/lib/"libqdb_api*.dylib) ;;
    * )       QDB_RUNTIME_LIBS=("${BASE_DIR}/qdb/lib/"libqdb_api*.so*) ;;
esac
shopt -u nullglob

if [[ "${OS}" != "linux" ]]; then
    if [[ ${#QDB_RUNTIME_LIBS[@]} -eq 0 || ! -f "${QDB_RUNTIME_LIBS[0]}" ]]; then
        echo "ERROR: no libqdb_api runtime library found for packaging" >&2
        exit 1
    fi

    cp "${QDB_RUNTIME_LIBS[@]}" "${PKG_DIR}/bin/"
fi

cp "${BASE_DIR}/examples/finance-ohlc.yaml" \
   "${BASE_DIR}/examples/industrial-sensor.yaml" \
   "${BASE_DIR}/examples/network-metrics.yaml" \
   "${PKG_DIR}/etc/"

# systemd unit: linux-only packaging artifact.
if [[ "${OS}" == "linux" ]]; then
    cp "${BASE_DIR}/examples/qdb-nats-connector.service" "${PKG_DIR}/etc/"
fi

# Archive name follows quasardb's published CPack grammar
#     qdb-<version>-<system>-64bit[-<cpu>]-<component>.<ext>
# via cicd_artifact_platform (00.common.sh), so connector archives sit
# alongside the server/c-api/utils archives on the download site.
ARCHIVE="${BASE_DIR}/artifacts/qdb-${VERSION}-$(cicd_artifact_platform)-nats-connector.tar.zst"

# --use-compress-program=zstd works on both GNU tar (Linux) and BSD tar
# (FreeBSD, macOS) as long as zstd is on PATH; avoids format-flag divergence.
tar --use-compress-program=zstd \
    -cf "${ARCHIVE}" \
    -C "${PKG_DIR}" \
    bin etc

rm -rf "${PKG_DIR}"

echo "Packaged: ${ARCHIVE} ($(du -h "${ARCHIVE}" | cut -f1))"
