#!/usr/bin/env bash

set -eu

SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
source "$SCRIPT_DIR/common.sh"

echo "Running qdb-nats-connector integration tests..."
echo "================================================"
echo ""
echo "Prerequisites:"
echo "- NATS server at localhost:4222"
echo "- QuasarDB cluster at localhost:2836"
echo ""

# Allow environment overrides
export NATS_ENDPOINT=${NATS_ENDPOINT:-"nats://localhost:4222"}
export QDB_CLUSTER_URI=${QDB_CLUSTER_URI:-"qdb://127.0.0.1:2836"}
export QDB_PUBLIC_KEY=${QDB_PUBLIC_KEY:-"cluster_public.key"}
export QDB_USER_SECURITY=${QDB_USER_SECURITY:-"users.cfg"}

echo "Configuration:"
echo "- NATS_ENDPOINT: $NATS_ENDPOINT"
echo "- QDB_CLUSTER_URI: $QDB_CLUSTER_URI"
echo ""

# Build qdb_nats_connector
(
    pushd ${BASE_DIR}

    # Run integration tests with build tag
    echo "Running tests..."
    ${GO} test -v -tags=integration -json ./... "$@"

    popd
)
