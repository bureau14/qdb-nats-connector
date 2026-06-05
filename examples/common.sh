#!/usr/bin/env bash
# Common infrastructure for golden data testing framework
# Provides shared utilities for environment, logging, error handling, process management, and validation

set -euo pipefail

# Executable suffix for the current platform: ".exe" on Windows (MSYS/Git-bash
# reports MINGW/MSYS/CYGWIN from uname), empty everywhere else. Every binary the
# examples invoke -- the connector, the qdb/nats CLIs, the data tools -- is
# built/shipped with this suffix, so scripts append ${EXE_EXT} rather than
# repeating the OS check. Defined at source time so scripts that source this
# file can use it immediately (before setup_environment runs).
case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*) EXE_EXT=".exe" ;;
    *)                    EXE_EXT="" ;;
esac
export EXE_EXT

# Environment Setup
# Source .env if exists and set defaults for QDB_URI and NATS_URL
setup_environment() {
    local script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    
    # Source .env if it exists in current directory (for testing) or script directory
    local env_file=""
    if [[ -f ".env" ]]; then
        env_file=".env"
    elif [[ -f "$script_dir/.env" ]]; then
        env_file="$script_dir/.env"
    fi
    
    if [[ -n "$env_file" ]]; then
        log_debug "Loading environment from $env_file"
        # Source .env while preserving existing exports
        set -a
        source "$env_file"
        set +a
    fi
    
    # Set defaults if not already defined
    export QDB_URI="${QDB_URI:-qdb://127.0.0.1:2836}"
    export NATS_URL="${NATS_URL:-nats://localhost:4222}"
    
    # Tool locations, referenced by explicit path (PATH is intentionally NOT
    # mutated). The qdb CLI tools live in <repo>/qdb/bin (assumed always
    # present); the NATS binaries are provisioned into <repo>/nats/bin by
    # scripts/cicd/start-nats.sh. All are overridable from the environment.
    local repo_root
    repo_root="$(cd "$script_dir/.." && pwd)"
    export QDB_BIN_DIR="${QDB_BIN_DIR:-$repo_root/qdb/bin}"
    export NATS_BIN_DIR="${NATS_BIN_DIR:-$repo_root/nats/bin}"
    export QDBSH="${QDBSH:-$QDB_BIN_DIR/qdbsh$EXE_EXT}"
    export QDB_EXPORT="${QDB_EXPORT:-$QDB_BIN_DIR/qdb_export$EXE_EXT}"
    export NATS_CLI="${NATS_CLI:-$NATS_BIN_DIR/nats$EXE_EXT}"

    log_debug "Environment setup: QDB_URI=$QDB_URI, NATS_URL=$NATS_URL"
    log_debug "Tools: QDBSH=$QDBSH, QDB_EXPORT=$QDB_EXPORT, NATS_CLI=$NATS_CLI"
}

# Logging Functions
log_info() {
    echo "[INFO] $(date '+%Y-%m-%d %H:%M:%S') $*"
}

log_error() {
    echo "[ERROR] $(date '+%Y-%m-%d %H:%M:%S') $*" >&2
}

log_debug() {
    if [[ "${DEBUG:-0}" == "1" ]]; then
        echo "[DEBUG] $(date '+%Y-%m-%d %H:%M:%S') $*"
    fi
}

# CPU and Worker Configuration
get_cpu_count() {
    if command -v nproc >/dev/null 2>&1; then
        nproc
    elif command -v sysctl >/dev/null 2>&1; then
        sysctl -n hw.ncpu
    else
        echo 4  # fallback
    fi
}

# Calculate default worker count (use all CPUs)
get_default_workers() {
    get_cpu_count
}

# Command Execution

# Run a command, wrapping it in `direnv exec .` when direnv is available so the
# repo .envrc CGO env (LD_LIBRARY_PATH / DYLD_LIBRARY_PATH / CGO_*) loads on
# developer machines. In CI direnv is absent and the identical env is exported
# by scripts/cicd/00.common.sh::cicd_setup_qdb_env before the examples run, so
# the command runs directly. The no-direnv branch uses `exec`, replacing the
# current (sub)shell, so a backgrounded caller's $! is the target process
# itself -- preserving correct PID-file tracking and signal delivery (and the
# connector path stays in the command line for the pgrep leak guard). Call this
# only as a backgrounded or terminal command (see finance-ohlc.sh action_run).
run_with_direnv() {
    if command -v direnv >/dev/null 2>&1; then
        direnv exec . "$@"
    else
        exec "$@"
    fi
}

# Error Handling
die() {
    local message="$1"
    local exit_code="${2:-1}"
    log_error "$message"
    exit "$exit_code"
}

require_command() {
    local cmd="$1"
    if ! command -v "$cmd" >/dev/null 2>&1; then
        die "Required command '$cmd' not found. Please install it first."
    fi
    log_debug "Required command '$cmd' is available"
}

# CSV Comparison

# Compare two CSV files row-by-row with per-field numeric tolerance, replacing
# the former numdiff dependency. A field that parses as a number on BOTH sides
# matches when their absolute difference is <= CSV_ABS_TOLERANCE (default
# 0.001); every other field (ISO timestamps, quoted strings, empty/null) must
# match byte-for-byte. Row order and per-row field counts must align -- the
# QuasarDB export order is deterministic. Both files are streamed in O(1)
# memory: awk walks the actual file and pulls each positionally-matching golden
# row via `getline`. On a full match logs [OK] and returns 0; on any difference
# logs [FAIL], prints up to the first 10 differences to stderr, and returns 1.
#
# NOTE: awk arithmetic is IEEE-754 double precision, so integer fields beyond
# 2^53 (~9.0e15) would not compare exactly. The example datasets stay far below
# that (max ~1e9), so this is not a concern here.
#
# Usage: compare_csv <actual_csv> <golden_csv> <label>
compare_csv() {
    local actual="$1"
    local golden="$2"
    local label="$3"
    local diff_output

    # Declare diff_output on its own line above: a combined `local x=$(...)`
    # would mask awk's exit status with local's (always 0).
    if diff_output=$(awk -v golden="$golden" -v tol="${CSV_ABS_TOLERANCE:-0.001}" '
        function abs(x)    { return x < 0 ? -x : x }
        function is_num(s) { return s ~ /^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][+-]?[0-9]+)?$/ }
        BEGIN { FS = ","; diffs = 0; shown = 0; max_show = 10 }
        {
            # Pull the positionally-corresponding row from the golden file.
            if ((getline gline < golden) <= 0) {
                if (shown < max_show) { printf("row %d: present in actual, missing in golden\n", NR); shown++ }
                diffs++
                next
            }
            if ($0 == gline) next                       # identical row: fast path
            gn = split(gline, g, ",")
            if (NF != gn) {
                if (shown < max_show) { printf("row %d: field count differs (actual=%d, golden=%d)\n", NR, NF, gn); shown++ }
                diffs++
                next
            }
            for (i = 1; i <= NF; i++) {
                if ($i == g[i]) continue
                if (is_num($i) && is_num(g[i])) {
                    if (abs(($i + 0) - (g[i] + 0)) <= tol) continue
                    if (shown < max_show) { printf("row %d col %d: |%s - %s| > %s\n", NR, i, $i, g[i], tol); shown++ }
                } else {
                    if (shown < max_show) { printf("row %d col %d: [%s] != [%s]\n", NR, i, $i, g[i]); shown++ }
                }
                diffs++
            }
        }
        END {
            if ((getline gline < golden) > 0) {
                if (shown < max_show) { printf("row %d: golden has more rows than actual\n", NR + 1); shown++ }
                diffs++
            }
            close(golden)
            exit (diffs > 0) ? 1 : 0
        }
    ' "$actual"); then
        log_info "[OK] $label validation passed"
        return 0
    fi

    log_error "[FAIL] $label validation failed"
    log_debug "First 10 differences for $label:"
    printf '%s\n' "$diff_output" >&2
    return 1
}

# Error trap function for debugging unexpected exits
setup_error_trap() {
    error_trap() {
        local line_num=$1
        local error_code=$2
        local command="$3"
        echo "[FATAL ERROR] Script failed at line $line_num with exit code $error_code" >&2
        echo "[FATAL ERROR] Failed command: $command" >&2
        echo "[FATAL ERROR] Call stack:" >&2
        local i=0
        while caller $i >&2; do
            ((i++))
        done
        exit $error_code
    }

    # Set up error trapping
    trap 'error_trap ${LINENO} $? "$BASH_COMMAND"' ERR
}

# Process Management

# Write PID to file atomically, fail if file exists
write_pid_file() {
    local pid_file="$1"
    local pid="${2:-$$}"
    local temp_file="${pid_file}.tmp.$$"
    
    # Write to temporary file first
    echo "$pid" > "$temp_file"
    
    # Try to create link atomically (will fail if file exists)
    if ln "$temp_file" "$pid_file" 2>/dev/null; then
        # Success - link created
        rm -f "$temp_file"
        log_debug "Written PID $pid to $pid_file"
    else
        # File already exists - check if process is running
        rm -f "$temp_file"
        local existing_pid
        if existing_pid=$(read_pid_file "$pid_file" 2>/dev/null); then
            die "PID file $pid_file already exists with running process $existing_pid"
        else
            log_debug "Removing stale PID file $pid_file and retrying"
            rm -f "$pid_file"
            echo "$pid" > "$temp_file"
            if ! ln "$temp_file" "$pid_file" 2>/dev/null; then
                rm -f "$temp_file"
                die "Failed to create PID file after removing stale file"
            fi
            rm -f "$temp_file"
            log_debug "Written PID $pid to $pid_file after removing stale file"
        fi
    fi
}

# Read PID from file, validate process exists
read_pid_file() {
    local pid_file="$1"
    
    if [[ ! -f "$pid_file" ]]; then
        die "PID file $pid_file does not exist"
    fi
    
    local pid
    pid=$(cat "$pid_file")
    
    if [[ ! "$pid" =~ ^[0-9]+$ ]]; then
        die "Invalid PID in file $pid_file: $pid"
    fi
    
    # Check if process exists
    if ! kill -0 "$pid" 2>/dev/null; then
        die "Process $pid from $pid_file is not running"
    fi
    
    echo "$pid"
}

# Remove PID file
cleanup_pid_file() {
    local pid_file="$1"
    
    if [[ -f "$pid_file" ]]; then
        rm -f "$pid_file"
        log_debug "Cleaned up PID file $pid_file"
    fi
}

# Service Validation

# Check if service is running on port
check_service_running() {
    local service_name="$1"
    local port="$2"
    local timeout="${3:-5}"
    
    log_debug "Checking if $service_name is running on port $port"
    
    # Use timeout command if available, otherwise use a simple nc check
    if command -v timeout >/dev/null 2>&1; then
        if timeout "$timeout" bash -c "echo >/dev/tcp/localhost/$port" 2>/dev/null; then
            log_debug "$service_name is running on port $port"
            return 0
        fi
    else
        # Fallback for systems without timeout
        if nc -z localhost "$port" 2>/dev/null; then
            log_debug "$service_name is running on port $port"
            return 0
        fi
    fi
    
    log_debug "$service_name is not running on port $port"
    return 1
}

# Check NATS and QuasarDB are running
validate_environment() {
    log_info "Validating environment services..."
    
    # Extract port from NATS_URL (format: nats://host:port)
    local nats_port
    if [[ "$NATS_URL" =~ nats://[^:]+:([0-9]+) ]]; then
        nats_port="${BASH_REMATCH[1]}"
    else
        nats_port="4222"  # Default NATS port
    fi
    
    # Extract port from QDB_URI (format: qdb://host:port)
    local qdb_port
    if [[ "$QDB_URI" =~ qdb://[^:]+:([0-9]+) ]]; then
        qdb_port="${BASH_REMATCH[1]}"
    else
        qdb_port="2836"  # Default QuasarDB port
    fi
    
    # Check NATS
    if ! check_service_running "NATS" "$nats_port"; then
        die "NATS is not running on port $nats_port. Please start NATS server first."
    fi
    
    # Check QuasarDB
    if ! check_service_running "QuasarDB" "$qdb_port"; then
        die "QuasarDB is not running on port $qdb_port. Please start QuasarDB server first."
    fi
    
    log_info "Environment validation successful - NATS and QuasarDB are running"
}

# Refuse to proceed if any qdb-nats-connector process is already running.
# Detection is process-based so it catches leaked or orphaned connectors
# regardless of PID-file state, including ones reparented to init. The inspector
# varies by platform: pgrep on Linux/macOS/FreeBSD, tasklist on Windows (the
# connector is a native .exe and MSYS2 ships no pgrep), ps as a last resort.
# Hard error (die) listing the offending processes; no bypass.
assert_no_connector_running() {
    local matches=""

    if command -v pgrep >/dev/null 2>&1; then
        matches=$(pgrep -lf 'bin/qdb-nats-connector' 2>/dev/null || true)
    elif command -v tasklist >/dev/null 2>&1; then
        matches=$(tasklist 2>/dev/null | grep -i 'qdb-nats-connector' || true)
    elif command -v ps >/dev/null 2>&1; then
        matches=$(ps -e 2>/dev/null | grep 'qdb-nats-connector' | grep -v grep || true)
    else
        log_info "No process inspector (pgrep/tasklist/ps) available; skipping connector preflight check"
        return 0
    fi

    if [[ -n "$matches" ]]; then
        log_error "Refusing to start: a qdb-nats-connector process is already running:"
        printf '%s\n' "$matches" >&2
        die "Stop the running connector(s) above first (e.g. 'kill <PID>'), then re-run."
    fi
}

# Silent Cleanup Functions

# Drop QuasarDB table if it exists, silently ignoring errors
drop_table_if_exists() {
    local table_name="$1"
    set +e  # Temporarily disable error checking
    "$QDBSH" -c "DROP TABLE \"${table_name}\"" 2>/dev/null
    set -e  # Re-enable error checking
}

# Row Count Detection

# Sum SELECT COUNT(*) across one or more QDB tables. Echoes the total to stdout;
# a query that fails or returns nothing contributes 0. Missing tables (e.g. early
# in a test before create) are treated as empty -- the loop keeps polling.
count_qdb_rows() {
    local total=0 t count
    for t in "$@"; do
        # qdbsh COUNT output:
        #   COUNT(col1)   COUNT(col2)   ...
        #   ----------------------------------
        #            2,000       1,712   ...
        # Grab the first numeric cell under the divider; strip thousands commas.
        count=$("$QDBSH" -c "SELECT COUNT(*) FROM \"$t\"" 2>/dev/null \
            | awk '/^-{3,}$/{getline; print $1; exit}' | tr -d ',')
        total=$((total + ${count:-0}))
    done
    echo "$total"
}

# Dump connector crash diagnostics to stderr.
#
# A Go runtime crash (panic, fatal error, or CGO SIGSEGV) prints a full
# goroutine dump -- often hundreds of lines -- whose *cause* sits at the TOP
# (`panic:` / `fatal error:` / `[signal SIGSEGV ...]`) and whose crashing stack
# follows immediately after. A plain `tail` captures only the bottom (an idle
# goroutine) and hides the reason, so this prints from the first crash marker
# onward, plus a tail for context. The whole log is also echoed when short, so
# the reason survives even with no recognizable marker (e.g. a CGO abort).
dump_connector_crash() {
    local log_file="$1"
    [[ -f "$log_file" ]] || { log_error "No connector log at $log_file"; return; }

    local lines marker
    lines=$(wc -l <"$log_file" | tr -d ' ')
    log_error "Connector log: $log_file ($lines lines)"

    # First line of the crash report -- the reason. SIGSEGV/SIGABRT lines, Go
    # panics, runtime fatal errors, and cgo aborts all start with one of these.
    marker=$(grep -niE '^(panic:|fatal error:|fatal:|runtime:|\[signal |SIG[A-Z]+:|unexpected fault)' "$log_file" \
        | head -1 | cut -d: -f1)

    if [[ -n "$marker" ]]; then
        log_error "Crash reason (from line $marker):"
        # Cap the dumped stack so a huge all-goroutines dump stays readable;
        # the crashing goroutine is always within the first ~60 lines.
        sed -n "${marker},$((marker + 60))p" "$log_file" >&2
    elif [[ "$lines" -le 200 ]]; then
        log_error "No crash marker found; full connector log:"
        cat "$log_file" >&2
    else
        log_error "No crash marker found; last 80 log lines:"
        tail -80 "$log_file" >&2
    fi
}

# Wait until SUM(COUNT(*)) across the listed tables reaches expected_rows.
# Queries QDB directly, so it observes actual write-visibility instead of the
# connector's buffered "wrote rows" log lines -- which return before data is
# query-visible and previously raced the subsequent export step.
# Fails fast on connector crash: ERROR-level log entry or PID gone.
# Args: log_file expected timeout interval pid_file <table>...
wait_for_qdb_rows() {
    local log_file="$1" expected="$2" timeout="$3" interval="$4" pid_file="$5"
    shift 5
    local -a tables=("$@")

    log_info "Waiting for $expected rows across ${#tables[@]} table(s) (timeout: ${timeout}s)"

    local elapsed=0 last=0 current pid
    while [[ $elapsed -lt $timeout ]]; do
        if [[ -f "$log_file" ]] && grep -q '"level":"ERROR"' "$log_file" 2>/dev/null; then
            log_error "ERROR detected in connector log:"
            grep '"level":"ERROR"' "$log_file" | tail -1
            return 1
        fi
        if [[ -n "$pid_file" && -f "$pid_file" ]]; then
            pid=$(cat "$pid_file" 2>/dev/null || true)
            if [[ -n "$pid" ]] && ! kill -0 "$pid" 2>/dev/null; then
                log_error "Connector process (PID: $pid) has exited unexpectedly"
                dump_connector_crash "$log_file"
                return 1
            fi
        fi

        current=$(count_qdb_rows "${tables[@]}")
        if [[ $current -ne $last ]]; then
            log_info "Progress: $current / $expected rows in QDB"
            last=$current
        fi
        if [[ $current -ge $expected ]]; then
            log_info "Reached $current rows in QDB (expected: $expected)"
            return 0
        fi

        sleep "$interval"
        elapsed=$((elapsed + interval))
    done

    log_error "Timeout after ${timeout}s. QDB has $last rows, expected $expected"
    return 1
}

# Initialize environment when sourced
setup_environment