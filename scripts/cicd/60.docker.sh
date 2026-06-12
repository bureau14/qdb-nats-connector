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
# On master builds, the :latest tag is pushed to Docker Hub using the
# read-write credential pair from SSM; branch and local runs build and
# smoke-test only. The :${VERSION} tag is never pushed.

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

# --- push (master builds only) ---

# Only the moving :latest pointer is published. Credentials are the
# read-write Docker Hub pair from SSM, written into a throwaway
# DOCKER_CONFIG so the agent's persistent docker config (which holds the
# fleet-wide read-only pull credential) is never touched.

if [[ "${BUILDKITE_BRANCH:-}" == "master" ]]
then
    SSM_PREFIX="${BUILDKITE_SSM_PREFIX:-/services/buildkite}"
    SSM_REGION="${AWS_DEFAULT_REGION:-eu-west-1}"

    DOCKER_CONFIG="$(mktemp -d)"
    export DOCKER_CONFIG
    trap 'rm -rf "${DOCKER_CONFIG}"' EXIT

    # set +x: the token must never reach the build log.
    set +x
    echo "Authenticating docker push from ${SSM_PREFIX}/config/dockerhub/push-username"
    PUSH_USERNAME="$(aws ssm get-parameter \
        --name "${SSM_PREFIX}/config/dockerhub/push-username" \
        --region "${SSM_REGION}" \
        --with-decryption \
        --query 'Parameter.Value' \
        --output text)"
    aws ssm get-parameter \
        --name "${SSM_PREFIX}/credentials/dockerhub/push-token" \
        --region "${SSM_REGION}" \
        --with-decryption \
        --query 'Parameter.Value' \
        --output text \
        | docker login --username "${PUSH_USERNAME}" --password-stdin
    set -x

    docker push "${IMAGE}:latest"
    docker logout

    echo "Pushed: ${IMAGE}:latest"
else
    echo "Skipping docker push: branch '${BUILDKITE_BRANCH:-}' is not master"
fi
