#!/usr/bin/env bash
# qdbd 3.14.2 compatibility shim -- BRANCH-ONLY, DO NOT MERGE UPSTREAM.
#
# Scope: branch 3.14.x-20260727-qdb-3.14.2 only. master and 3.14.x pin newer
# qdbd builds and must never carry this file. To remove the shim entirely,
# delete examples/compat/ plus the marked blocks in examples/Makefile and
# scripts/cicd/50.test-e2e.sh -- nothing else references it.
#
# The problem
# -----------
# qdb_export suppresses nulls in apps/railgun-common/parser/write_value.hpp:170
# via `if (is_null_value(value)) return;`. is_null_value (null_value.hpp:19-26)
# is `value == null_value<T>`, and null_value<double> is quiet_NaN(). Because
# NaN == NaN is always false the guard never fires for doubles, so the value
# reaches write_value.cpp:49-53 -- fmt::format_to(it, "{:.16g}", value) -- which
# prints the literal text `nan`.
#
# quasardb commit 7299b92 ("QDB-17968 - Deprecate single column API",
# 2026-01-09) added an explicit `!std::isnan(v)` guard at
# apps/railgun-exporter/runner.cpp:85-88 that returns early and writes an empty
# field instead. That commit is in master and 3.14.x but NOT in the v3.14.2 tag,
# and NOT in 3.14.2-test-runner (whose export tree is byte-identical to v3.14.2).
#
# The golden CSVs under datasets/*/expected/ were generated with a post-7299b92
# qdb_export, so their null DOUBLEs are empty fields. Against qdbd 3.14.2 the
# same nulls export as `nan`, and compare_csv() (../common.sh:162-216) reports
#   row N col 3: [nan] != []
# because is_num() (../common.sh:172) does not match `nan`, so the pair falls
# through to the byte-equality branch.
#
# What this does
# --------------
# Rewrites `nan` fields to empty fields in a directory of freshly exported CSVs,
# immediately before validation. compare_csv() is deliberately left untouched so
# comparison semantics stay identical to 3.14.x.
#
# DOUBLE is the only affected type. Verified against the v3.14.2 source:
# is_null_value works correctly for int64 (value == qdb_int64_undefined, plain
# integer equality), for timestamps (qdb_min_timespec via operator==) and for
# blob/string/symbol (the std::span overload, value.empty()). Double is the sole
# type whose null sentinel is NaN and therefore the sole type where `==` fails.
# No other sentinel token needs handling here.
#
# inf and -inf are deliberately left alone: the 7299b92 guard is isnan-only, so
# both qdbd generations render infinities identically.
#
# Matching bare `nan` is safe because qdb_export quotes string, symbol and blob
# columns -- e.g. 2025-01-16T09:04:10,"sensor-01",,"B1-1-sensor-01",2 -- so a
# string whose value happened to be nan would export as the 5-character token
# "nan" (quotes included) and would not match. An unquoted bare `nan` field can
# only be a DOUBLE.
#
# `NaN` / `NAN` are NOT matched: fmt's lowercase {:g} always renders specials in
# lowercase, so those spellings would be dead code. `-nan` is matched purely as a
# zero-cost guard -- it cannot occur today, since every null double here comes
# from the connector's math.NaN() sentinel fill (internal/parser/yaml.go:487),
# whose bit pattern 0x7FF8000000000001 has the sign bit clear.
#
# Caveats
# -------
# The awk below splits on a bare comma and is NOT RFC4180-aware. That is safe
# only because no column in these examples contains an embedded comma (field
# counts are uniform per table: 5 for industrial, 9 for finance). If a future
# re-sync from 3.14.x adds a free-text string column to a table that ALSO has a
# nullable double, this would silently corrupt those rows. Do not assume this
# script understands quoting.
#
# Only actual/ is rewritten; expected/ is never touched. Golden data is a shared,
# cached artifact (the .extracted marker gates re-download) and must stay
# pristine.
#
# This is a no-op on newer qdbd, which is REQUIRED rather than incidental: the
# buildkite artifacts plugin currently falls back to refs/heads/master for the
# windows, freebsd and macos variants (no 3.14.2-test-runner build exists for
# them), so a single CI run exercises both qdbd generations at once.
#
# `make generate-golden` deliberately does NOT invoke this shim, and MUST NOT be
# run on this branch against qdbd 3.14.2: action_prepare_golden copies
# actual/*.csv straight into expected/, which would bake `nan` tokens into a
# golden set and later produce an inverted, confusing `[] != [nan]` failure.
#
# Running `./<example>.sh export && ./<example>.sh validate` by hand bypasses
# this shim -- only `make test` goes through the hook. Invoke this script
# directly against datasets/<example>-<count>/actual if you need the manual path.

set -euo pipefail
shopt -s nullglob

# Rewrite every field that is exactly `nan` or `-nan` into an empty field, in
# place. Lines containing no such field are emitted byte-for-byte unchanged:
# awk rebuilds $0 from OFS only when a field is assigned, so untouched lines
# pass through print verbatim.
normalize_nan_fields() {
    local csv="$1"

    if ! awk '
        BEGIN { FS = OFS = "," }
        {
            for (i = 1; i <= NF; i++)
                if ($i == "nan" || $i == "-nan") $i = ""
            print
        }
    ' "$csv" >"$csv.tmp"; then
        rm -f "$csv.tmp"
        echo "[ERROR] failed to normalize $csv" >&2
        return 1
    fi

    mv -f "$csv.tmp" "$csv"
}

# Apply normalize_nan_fields to every *.csv directly in <dir>.
#
# An empty directory is an error, not a no-op: action_export recreates actual/
# and export_table_csv dies on any export failure, so by construction the
# directory is populated when this runs. Treating "no files" as success would
# mask a genuine earlier export failure.
normalize_dir() {
    local dir="$1"
    local csvs=( "$dir"/*.csv )

    if (( ${#csvs[@]} == 0 )); then
        echo "[ERROR] no CSV files in $dir -- the export step should have written some" >&2
        return 1
    fi

    local csv
    for csv in "${csvs[@]}"; do
        normalize_nan_fields "$csv"
    done

    echo "[INFO] qdb 3.14.2 compat: normalized ${#csvs[@]} CSV file(s) in $dir"
}

main() {
    if (( $# != 1 )); then
        echo "usage: ${0##*/} <csv-directory>" >&2
        return 2
    fi

    local dir="$1"
    if [[ ! -d "$dir" ]]; then
        echo "[ERROR] not a directory: $dir" >&2
        return 1
    fi

    normalize_dir "$dir"
}

main "$@"
