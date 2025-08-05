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

# Silent Cleanup Functions

# Drop QuasarDB table if it exists, silently ignoring errors
drop_table_if_exists() {
    local table_name="$1"
    set +e  # Temporarily disable error checking
    qdbsh -c "DROP TABLE \"${table_name}\"" 2>/dev/null
    set -e  # Re-enable error checking
}

# Row Count Detection

# Parse connector logs for row count messages
# The QuasarDB Go API logs: L().Info("wrote rows", "count", totalRows, "duration", elapsed)
# This appears as structured logs, so we look for "wrote rows" and extract count
parse_processed_rows() {
    local log_file="$1"
    
    if [[ ! -f "$log_file" ]]; then
        log_error "Log file $log_file does not exist"
        return 1
    fi
    
    local total_rows=0
    
    # Parse structured log format looking for "wrote rows" with count
    # Expected format: [timestamp] level=info msg="wrote rows" count=123 duration=...
    # Also handle slog JSON format: {"time":"...","level":"INFO","msg":"wrote rows","count":123}
    while IFS= read -r line; do
        # Handle key=value format (default slog text format)
        if [[ "$line" =~ wrote\ rows.*count=([0-9]+) ]]; then
            local count="${BASH_REMATCH[1]}"
            total_rows=$((total_rows + count))
            log_debug "Found wrote rows: count=$count, total=$total_rows"
        # Handle JSON format
        elif [[ "$line" =~ \"msg\":\"wrote\ rows\".*\"count\":([0-9]+) ]]; then
            local count="${BASH_REMATCH[1]}"
            total_rows=$((total_rows + count))
            log_debug "Found wrote rows (JSON): count=$count, total=$total_rows"
        fi
    done < "$log_file"
    
    echo "$total_rows"
}

# Wait for expected row count with timeout
wait_for_row_count() {
    local log_file="$1"
    local expected_rows="$2"
    local timeout_seconds="${3:-300}"  # Default 5 minutes
    local check_interval="${4:-5}"     # Default 5 seconds
    local pid_file="${5:-}"           # Optional PID file to check
    
    log_info "Waiting for $expected_rows rows to be processed (timeout: ${timeout_seconds}s)"
    
    local elapsed=0
    local last_reported_count=0
    
    while [[ $elapsed -lt $timeout_seconds ]]; do
        # Check if log file exists and for ERROR logs
        if [[ -f "$log_file" ]]; then
            # Check for ERROR level logs
            if grep -q '"level":"ERROR"' "$log_file" 2>/dev/null; then
                log_error "ERROR detected in connector log:"
                grep '"level":"ERROR"' "$log_file" | tail -1
                return 1
            fi
            
            local current_count
            current_count=$(parse_processed_rows "$log_file")
            
            # DEBUG: Log every check iteration
            log_debug "[DEBUG] Elapsed: ${elapsed}s, Current count: $current_count, Expected: $expected_rows"
            
            # Report progress if count changed
            if [[ $current_count -ne $last_reported_count ]]; then
                log_info "Progress: $current_count / $expected_rows rows processed"
                last_reported_count=$current_count
                
                # DEBUG: Show recent log entries when progress is made
                log_debug "[DEBUG] Recent log entries showing progress:"
                tail -3 "$log_file" | while IFS= read -r line; do
                    log_debug "[DEBUG] Log: $line"
                done
            fi
            
            # Check if we've reached the expected count
            if [[ $current_count -ge $expected_rows ]]; then
                log_info "Successfully processed $current_count rows (expected: $expected_rows)"
                return 0
            fi
        else
            log_debug "[DEBUG] Log file $log_file does not exist yet"
        fi
        
        # Check if process is still running (if PID file provided)
        if [[ -n "$pid_file" ]] && [[ -f "$pid_file" ]]; then
            local pid
            pid=$(cat "$pid_file" 2>/dev/null || echo "")
            if [[ -n "$pid" ]] && ! kill -0 "$pid" 2>/dev/null; then
                log_error "Connector process (PID: $pid) has exited unexpectedly"
                if [[ -f "$log_file" ]]; then
                    log_error "Last log entries:"
                    tail -10 "$log_file"
                fi
                return 1
            fi
            log_debug "[DEBUG] Connector process $pid is still running"
        fi
        
        sleep "$check_interval"
        elapsed=$((elapsed + check_interval))
    done
    
    # Timeout reached
    local final_count
    if [[ -f "$log_file" ]]; then
        final_count=$(parse_processed_rows "$log_file")
    else
        final_count=0
    fi
    
    log_error "Timeout reached after ${timeout_seconds}s. Processed $final_count rows, expected $expected_rows"
    return 1
}

# Initialize environment when sourced
setup_environment