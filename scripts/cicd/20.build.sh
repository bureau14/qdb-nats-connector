#!/usr/bin/env bash
# Buildkite build step for qdb-nats-connector.
# Invoked by .buildkite/steps/_build.yml.
# Compiles the connector via `make build` and packages bin/qdb-nats-connector
# into artifacts/qdb-nats-connector-{slug}.tar.zst for upload.
#
# The qdb-c-api fetch (populating qdb/lib and qdb/include) is the
# user's responsibility and must run before this script.

set -euxo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "$(dirname "${SCRIPT_DIR}")")"

# Source shared CGO env helpers (00.common.sh).
source "${SCRIPT_DIR}/00.common.sh"

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

# --- build ---

# GOAMD64 may already be set by the pipeline (decision 4: CPU_ENV injection).
# The Makefile honours the env var, so no explicit override is needed here.
make build

# Detect platform-specific binary suffix (Windows under MINGW uses .exe).
SUFFIX=""
if [[ "$(uname)" == MINGW* ]]; then
    SUFFIX=".exe"
fi

CONNECTOR_BIN="${BASE_DIR}/bin/qdb-nats-connector${SUFFIX}"

if [[ ! -f "${CONNECTOR_BIN}" ]]; then
    echo "ERROR: expected binary not found at ${CONNECTOR_BIN}" >&2
    exit 1
fi

# --- package ---

mkdir -p "${BASE_DIR}/artifacts"

# SLUG identifies the platform in the archive name; it comes from the
# step env (BUILDKITE_STEP_SLUG) or is derived from BUILDKITE_STEP_KEY
# by stripping the "build-" prefix.  Both are set by the pipeline template.
SLUG="${BUILDKITE_STEP_SLUG:-${BUILDKITE_STEP_KEY#build-}}"
if [[ -z "${SLUG}" ]]; then
    echo "ERROR: could not determine SLUG (neither BUILDKITE_STEP_SLUG nor BUILDKITE_STEP_KEY is set)" >&2
    exit 1
fi

ARCHIVE="${BASE_DIR}/artifacts/qdb-nats-connector-${SLUG}.tar.zst"

# --use-compress-program=zstd works on both GNU tar (Linux) and BSD tar
# (FreeBSD, macOS) as long as zstd is on PATH; avoids format-flag divergence.
tar --use-compress-program=zstd \
    -cf "${ARCHIVE}" \
    -C "${BASE_DIR}" \
    "bin/qdb-nats-connector${SUFFIX}"

echo "Packaged: ${ARCHIVE} ($(du -h "${ARCHIVE}" | cut -f1))"
