#!/usr/bin/env bash
# qdbd version gate -- BRANCH-ONLY, DO NOT MERGE UPSTREAM.
#
# Scope: branch 3.14.x-20260727-qdb-3.14.2 only. This branch exists to keep the
# connector passing e2e against qdbd 3.14.2 specifically; master and 3.14.x pin
# newer qdbd and must never carry this file. To remove, delete examples/compat/
# plus the marked blocks in examples/Makefile and scripts/cicd/50.test-e2e.sh.
#
# Why this exists
# ---------------
# .buildkite/pipeline.py pins quasardb-build downloads to
# refs/heads/3.14.2-test-runner via a by_project override. That ref only has
# published artifacts for the two linux variants. For windows, freebsd and macos
# the qdb-artifacts plugin SILENTLY falls back to refs/heads/master and logs a
# single warning nobody reads:
#
#   [artifacts] WARNING: falling back from ref=refs/heads/3.14.2-test-runner to
#               ref=refs/heads/master (no artifacts on the requested ref)
#   [artifacts]   start: artifacts/qdb-3.15.0.dev0-windows-64bit-server.tar.zst
#
# The result (observed in build #245): 3 of 5 platforms were running qdbd
# 3.15.0.dev0 while reporting green, so the branch's entire premise -- "this is
# validated against 3.14.2" -- was quietly false for most of the matrix.
#
# This gate turns that silent downgrade into a loud, early failure. Until
# 3.14.2-test-runner publishes artifacts for the windows, freebsd and macos
# variants, those three legs are EXPECTED to fail here. That red state is the
# correct signal, not a regression.
#
# Probing qdb_export specifically (rather than qdbd) is deliberate: it is the
# binary whose null-DOUBLE rendering differs across versions and which
# qdb-3.14.2-nan-to-empty.sh compensates for. All three binaries ship in the
# same artifact set and cannot drift apart within one CI run.
#
# Usage: assert-qdb-version.sh <expected-version> <qdb-bin-dir>

set -euo pipefail

# Executable suffix for the current platform, mirroring ../common.sh:13-17.
exe_ext() {
    case "$(uname -s)" in
        MINGW*|MSYS*|CYGWIN*) echo ".exe" ;;
        *)                    echo "" ;;
    esac
}

# Extract the version token a qdb CLI binary reports. Output shape:
#     qdb_export version: 3.15.0.dev0
#     build: 66a4221e1e1390d673cf558311f3e0aa23a4e534
#     date: 2026-05-29 13:19:35 +0200
qdb_reported_version() {
    local bin="$1"

    "$bin" --version 2>&1 | awk '/version:/ { print $NF; exit }'
}

assert_qdb_version() {
    local expected="$1" bin_dir="$2"
    local bin="$bin_dir/qdb_export$(exe_ext)"

    if [[ ! -x "$bin" ]]; then
        echo "[ERROR] qdb_export not found or not executable: $bin" >&2
        return 1
    fi

    local actual
    actual="$(qdb_reported_version "$bin")"

    if [[ -z "$actual" ]]; then
        echo "[ERROR] could not parse a version from '$bin --version'" >&2
        return 1
    fi

    if [[ "$actual" != "$expected" ]]; then
        echo "[ERROR] qdb version gate: expected $expected, found $actual ($bin)" >&2
        echo "[ERROR] This platform almost certainly fell back from" >&2
        echo "[ERROR]   refs/heads/3.14.2-test-runner to refs/heads/master" >&2
        echo "[ERROR] because 3.14.2-test-runner publishes no artifacts for it." >&2
        echo "[ERROR] Search the build log for '[artifacts] WARNING: falling back'." >&2
        return 1
    fi

    echo "[INFO] qdb version gate: $actual ($bin)"
}

main() {
    if (( $# != 2 )); then
        echo "usage: ${0##*/} <expected-version> <qdb-bin-dir>" >&2
        return 2
    fi

    assert_qdb_version "$1" "$2"
}

main "$@"
