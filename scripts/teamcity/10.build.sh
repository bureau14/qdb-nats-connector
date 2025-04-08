#!/usr/bin/env bash

set -eux

SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
source "$SCRIPT_DIR/common.sh"

SUFFIX=""

case $(uname) in
    MINGW* )
        SUFFIX=".exe"
        ;;
esac

# Build qdb_rest
(
    pushd ${BASE_DIR}
    ${GO} build -x -v -o qdb_nats_connector$SUFFIX
    popd
)
