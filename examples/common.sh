#!/bin/bash
# Common infrastructure for golden data testing framework
# Provides shared utilities for environment, logging, error handling, process management, and validation

set -euo pipefail

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
    
    log_debug "Environment setup: QDB_URI=$QDB_URI, NATS_URL=$NATS_URL"
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
# Detection is process-based (pgrep -f on the connector binary path) so it catches
# leaked or orphaned connectors regardless of PID-file state, including ones that have
# been reparented to init. Hard error (die) listing the offending processes; no bypass.
assert_no_connector_running() {
    local pattern='bin/qdb-nats-connector'

    command -v pgrep >/dev/null 2>&1 || \
        die "pgrep not found; it is required for the connector preflight check"

    local matches
    matches=$(pgrep -lf "$pattern" 2>/dev/null || true)

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
    qdbsh -c "DROP TABLE \"${table_name}\"" 2>/dev/null
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
        count=$(qdbsh -c "SELECT COUNT(*) FROM \"$t\"" 2>/dev/null \
            | awk '/^-{3,}$/{getline; print $1; exit}' | tr -d ',')
        total=$((total + ${count:-0}))
    done
    echo "$total"
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
                [[ -f "$log_file" ]] && { log_error "Last log entries:"; tail -10 "$log_file"; }
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