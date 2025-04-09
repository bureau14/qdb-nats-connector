#!/usr/bin/env bash

set -eu

SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
source "$SCRIPT_DIR/common.sh"

# Build qdb_nats_connector
(
    pushd ${BASE_DIR}
    ${GO} test -v -json ./...
    popd
)
