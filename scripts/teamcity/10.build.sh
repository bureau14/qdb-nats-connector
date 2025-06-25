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

# Necessary because we do the checkout on the "host" machine while run build
# inside a docker container.
git config --global --add safe.directory '*'

# Build qdb_nats_connector
(
    pushd ${BASE_DIR}
    ${GO} build -x -v -o qdb_nats_connector$SUFFIX
    popd
)
