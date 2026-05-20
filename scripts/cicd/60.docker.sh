#!/usr/bin/env bash
# Buildkite docker-build step for qdb-nats-connector.
# Invoked by .buildkite/steps/_docker.yml on the default-debian-amd64
# bare-host agent (NOT inside a docker plugin wrapper).
#
# Builds bureau14/qdb-nats-connector from the Dockerfile at the repo root,
# tags it as both ${VERSION} and latest, then runs `docker run --rm
# <image>:<version> --version` as a smoke test that the binary launches
# and libqdb_api.so loads via the baked rpath.
#
# The image is NOT pushed to any registry (temporary scaffolding -- will
# be superseded once QuasarDB 3.14.3 ships an official artifact).

set -euxo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "$(dirname "${SCRIPT_DIR}")")"

# Bare-host agent has no UID-mapping concern, but the safe.directory line
# is consistent with the rest of scripts/cicd/ and harmless on bare host.
git config --global --add safe.directory '*'

cd "${BASE_DIR}"

# --- metadata extraction ---

VERSION="$(cat VERSION)"
GIT_SHA="$(git rev-parse HEAD)"
BUILD_TIME="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

IMAGE="bureau14/qdb-nats-connector"

# --- build ---

docker build \
    --build-arg "VERSION=${VERSION}" \
    --build-arg "GIT_SHA=${GIT_SHA}" \
    --build-arg "BUILD_TIME=${BUILD_TIME}" \
    -t "${IMAGE}:${VERSION}" \
    -t "${IMAGE}:latest" \
    -f Dockerfile \
    .

# --- validate ---

# Smoke test: the binary must run, libqdb_api.so must load via rpath,
# and --version must exit 0.
docker run --rm "${IMAGE}:${VERSION}" --version

echo "Built and validated: ${IMAGE}:${VERSION} (also tagged :latest)"
