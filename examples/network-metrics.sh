#!/bin/bash
# Network device metrics monitoring example - Modular actions for golden data testing
set -euo pipefail

# Script configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source common infrastructure with error handling
if ! source "$SCRIPT_DIR/common.sh"; then
    echo "[FATAL ERROR] Failed to source common.sh from $SCRIPT_DIR/common.sh" >&2
    echo "[FATAL ERROR] Please ensure the common.sh file exists and is readable" >&2
    exit 1
fi

# Setup standardized error trapping
setup_error_trap

# Override die function to ensure errors are always visible in Make
die() {
    local message="$1"
    local exit_code="${2:-1}"
    
    # Ensure message goes to stderr with timestamp
    echo "[ERROR] $(date '+%Y-%m-%d %H:%M:%S') $message" >&2
    
    # Force flush stderr
    exec 2>&2
    
    # Exit with specified code
    exit "$exit_code"
}

# Script-specific configuration
DATA_FILE="$SCRIPT_DIR/network-metrics-data.jsonl"
CONFIG_FILE="$SCRIPT_DIR/network-metrics.yaml"
STREAM_NAME="NETWORK_STREAM"
SUBJECT="network.metrics"
NUM_MESSAGES="${NUM_MESSAGES:-1000}"
PID_FILE="$SCRIPT_DIR/network-metrics-connector.pid"
LOG_FILE="$SCRIPT_DIR/network-metrics-connector.log"
CONNECTOR_BINARY="$SCRIPT_DIR/../bin/qdb-nats-connector"
EXPORT_DIR="$SCRIPT_DIR/exports"
TESTDATA_DIR="${TESTDATA_DIR:-$SCRIPT_DIR/testdata}"

# Device configuration for data generation
declare -a DEVICES=("router-01" "router-02" "switch-01" "switch-02" "firewall-01")

# Device type parameters will be set in get_device_params function

# Single table for network metrics (no dynamic routing)
TABLE_NAME="network_metrics"

# Get device parameters based on device type
get_device_params() {
    local device_type="$1"
    case "$device_type" in
        "router")
            echo "1000000 20 0.5 50000000 5.0"
            ;;
        "switch")
            echo "500000 10 0.2 25000000 2.0"
            ;;
        "firewall")
            echo "2000000 100 1.0 30000000 10.0"
            ;;
        *)
            echo "1000000 20 0.5 50000000 5.0"  # default to router params
            ;;
    esac
}

usage() {
    echo "Usage: $0 {create|generate|load|run|wait|stop|export|validate|prepare-golden}"
    echo
    echo "Actions:"
    echo "  create         - Create NATS JetStream stream and QuasarDB table"
    echo "  generate       - Generate test dataset only"
    echo "  load           - Load generated data into NATS JetStream"
    echo "  run            - Start connector in background with PID management"
    echo "  wait           - Wait for processing completion via row count"
    echo "  stop           - Stop connector gracefully using PID file"
    echo "  export         - Export data from QuasarDB to CSV files"
    echo "  validate       - Compare exported data with golden data"
    echo "  prepare-golden - Organize files for golden data packaging"
    echo
    echo "Environment variables:"
    echo "  NUM_MESSAGES   - Number of messages to generate (default: 1000)"
    echo "  TESTDATA_DIR   - Directory for test data (default: ./testdata)"
    echo "  DEBUG          - Enable debug logging (1 = enabled)"
    echo
    echo "Requirements:"
    echo "  qdb-data-gen   - Required for data generation"
    echo "  qdb-data-loader - Required for loading data into NATS"
    exit 1
}

# Generate test dataset
action_generate() {
    log_info "Generating $NUM_MESSAGES network metrics messages..."

    # Verify NUM_MESSAGES is a valid number
    if ! [[ "$NUM_MESSAGES" =~ ^[0-9]+$ ]] || [[ "$NUM_MESSAGES" -eq 0 ]]; then
        die "Invalid NUM_MESSAGES value: '$NUM_MESSAGES'. Must be a positive integer."
    fi

    # Verify we can write to the data file directory
    local data_dir
    data_dir=$(dirname "$DATA_FILE")
    if [[ ! -w "$data_dir" ]]; then
        die "Cannot write to data directory: $data_dir. Please check permissions."
    fi

    log_info "Generating $NUM_MESSAGES network metrics messages..."
    # Generate flat structure first
    bin/qdb-data-gen network-metrics-generator.yaml --count "$NUM_MESSAGES" | \
    jq -c '{
      device: {
        info: {
          id: (["router-01", "router-02", "switch-01", "switch-02", "firewall-01"][.device_info_id - 1])
        }
      },
      datacenter: (["DC1", "DC1", "DC1", "DC1", "DC2"][.device_info_id - 1]),
      rack: (["R42", "R43", "R42", "R43", "R01"][.device_info_id - 1]),
      timestamp: .timestamp,
      metrics: {
        network: {
          inbound: {
            bytes: .metrics_network_inbound_bytes
          },
          outbound: {
            bytes: .metrics_network_outbound_bytes
          }
        },
        performance: {
          latency_percentiles: {
            p99: .metrics_performance_latency_percentiles_p99
          }
        }
      },
      errors: (if .errors_packets_dropped > 20 then null else {
        packets: {
          dropped: .errors_packets_dropped
        }
      } end)
    } | del(.device_info_id, .metrics_network_inbound_bytes, .metrics_network_outbound_bytes, .metrics_performance_latency_percentiles_p99, .errors_packets_dropped, ."$table")' > "$DATA_FILE"
    log_info "Generated $NUM_MESSAGES network metrics messages in $DATA_FILE"
}

# Create NATS JetStream stream and QuasarDB table
action_create() {
    log_info "Creating NATS JetStream stream and QuasarDB table..."

    # Validate environment first with enhanced error context
    log_debug "Validating environment (NATS and QuasarDB connectivity)..."
    if ! validate_environment; then
        die "Environment validation failed. Please ensure NATS and QuasarDB services are running and accessible at:
  NATS: $NATS_URL
  QuasarDB: $QDB_URI"
    fi

    # Require necessary commands with enhanced error messages
    if ! require_command nats; then
        die "NATS CLI is required but not found. Please install the NATS CLI tool.
Visit: https://docs.nats.io/using-nats/nats-tools/nats_cli"
    fi

    if ! require_command qdbsh; then
        die "QuasarDB shell (qdbsh) is required but not found. Please install QuasarDB client tools."
    fi

    # Silent cleanup: Delete existing NATS stream if it exists
    log_debug "Cleaning up existing NATS stream if present..."
    nats stream delete "$STREAM_NAME" --force 2>/dev/null || true

    # Create NATS stream with better error handling
    log_info "Creating NATS JetStream stream: $STREAM_NAME"
    if ! nats stream add "$STREAM_NAME" --subjects "network.>" --retention limits --defaults; then
        die "Failed to create NATS stream '$STREAM_NAME'. Check:
  - NATS server is running and accessible at: $NATS_URL
  - JetStream is enabled on the NATS server
  - You have permission to create streams"
    fi
    log_info "Stream $STREAM_NAME created successfully"

    # Create consumer for the connector with better error handling
    log_info "Creating NATS consumer: network-connector"
    if nats consumer info "$STREAM_NAME" network-connector >/dev/null 2>&1; then
        log_info "Consumer network-connector already exists, skipping consumer creation"
    else
        if ! nats consumer add "$STREAM_NAME" network-connector --pull --deliver all --ack explicit --defaults; then
            die "Failed to create NATS consumer 'network-connector' on stream '$STREAM_NAME'. Check:
  - Stream '$STREAM_NAME' exists and is accessible
  - You have permission to create consumers"
        fi
        log_info "Consumer network-connector created successfully"
    fi

    # Create QuasarDB table with better error handling
    log_info "Creating QuasarDB table for network metrics..."
    
    # Silent cleanup: Drop existing table if it exists
    log_debug "Cleaning up existing QuasarDB table if present..."
    drop_table_if_exists "$TABLE_NAME"

    local table_schema="(device_id STRING, bytes_in INT64, bytes_out INT64, packets_dropped INT64, latency_ms DOUBLE, device_path STRING)"

    # Create the network_metrics table
    log_debug "Creating table with schema: $TABLE_NAME $table_schema"
    if ! qdbsh -c "CREATE TABLE \"$TABLE_NAME\"$table_schema"; then
        die "Failed to create QuasarDB table '$TABLE_NAME'. Check:
  - QuasarDB server is running and accessible at: $QDB_URI
  - You have permission to create tables
  - The table schema is valid: $table_schema"
    fi
    log_info "QuasarDB table '$TABLE_NAME' created successfully"
    log_info "Setup completed - NATS stream and QuasarDB table are ready"
}

# Load generated data into NATS JetStream
action_load() {
    log_info "Loading data into NATS JetStream..."

    # Validate environment first
    validate_environment

    # Generate data if not exists
    if [[ ! -f "$DATA_FILE" ]]; then
        log_info "Data file not found, generating first..."
        action_generate
    fi

    # Check if stream exists
    if ! nats stream info "$STREAM_NAME" >/dev/null 2>&1; then
        die "Stream $STREAM_NAME does not exist. Run '$0 create' first"
    fi

    log_info "Loading data into NATS JetStream..."
    bin/qdb-data-loader --file "$DATA_FILE" --topic "$SUBJECT" --stream "$STREAM_NAME" \
                          --nats-url "$NATS_URL" --batch-size 100
    log_info "Data loaded successfully"

    nats stream info "$STREAM_NAME"
}

# Start connector in background with PID management
action_run() {
    log_info "Starting qdb-nats-connector in background..."

    # Validate environment first with enhanced error context
    log_debug "Validating environment (NATS and QuasarDB connectivity)..."
    if ! validate_environment; then
        die "Environment validation failed. Please ensure NATS and QuasarDB services are running and accessible."
    fi
    log_debug "Environment validation completed successfully"

    # Check required files with enhanced error messages
    if [[ ! -f "$CONFIG_FILE" ]]; then
        die "Config file not found: $CONFIG_FILE. Please create the configuration file first."
    fi
    log_debug "Config file verified: $CONFIG_FILE"

    # Check if connector binary exists with enhanced error message
    if [[ ! -f "$CONNECTOR_BINARY" ]]; then
        die "Connector binary not found at: $CONNECTOR_BINARY
Please build the connector first using:
  cd $(dirname "$CONNECTOR_BINARY")
  direnv exec . go build -o qdb-nats-connector main.go"
    fi
    
    # Verify the binary is executable
    if [[ ! -x "$CONNECTOR_BINARY" ]]; then
        die "Connector binary is not executable: $CONNECTOR_BINARY
Please make it executable with: chmod +x $CONNECTOR_BINARY"
    fi
    log_debug "Connector binary verified: $CONNECTOR_BINARY"

    # Check if connector is already running
    if [[ -f "$PID_FILE" ]]; then
        if read_pid_file "$PID_FILE" >/dev/null 2>&1; then
            die "Connector is already running (PID file: $PID_FILE). Stop it first with '$0 stop'"
        else
            log_info "Removing stale PID file: $PID_FILE"
            cleanup_pid_file "$PID_FILE"
        fi
    fi

    # Verify direnv is available
    log_debug "Checking for direnv command..."
    if ! command -v direnv >/dev/null 2>&1; then
        die "direnv command not found. Please install direnv or run the connector directly."
    fi
    log_debug "direnv command found"

    log_info "Starting connector with config: $CONFIG_FILE"
    log_info "Logs will be written to: $LOG_FILE"
    log_debug "Command: direnv exec . $CONNECTOR_BINARY --nats $NATS_URL --qdb $QDB_URI --stream $STREAM_NAME --consumer network-connector --workers 1 --parser yaml --parser-config $CONFIG_FILE"

    # Clear any existing log file to avoid confusion
    log_debug "Clearing previous log file..."
    if ! > "$LOG_FILE"; then
        die "Failed to clear log file: $LOG_FILE. Check permissions."
    fi

    # Flush all output before starting the connector
    exec 1>&1 2>&2

    # Start connector in background and capture PID
    log_debug "Starting connector process..."
    if ! direnv exec . "$CONNECTOR_BINARY" \
        --nats "$NATS_URL" \
        --qdb "$QDB_URI" \
        --stream "$STREAM_NAME" \
        --consumer network-connector \
        --workers 1 \
        --parser yaml \
        --parser-config "$CONFIG_FILE" \
        > "$LOG_FILE" 2>&1 & then
        echo "[ERROR] Failed to start connector with direnv" >&2
        echo "[ERROR] Command: direnv exec . $CONNECTOR_BINARY --nats $NATS_URL --qdb $QDB_URI --stream $STREAM_NAME --consumer network-connector --workers 1 --parser yaml --parser-config $CONFIG_FILE" >&2
        echo "[ERROR] Check that direnv is properly configured and the binary path is correct" >&2
        exit 1
    fi

    local connector_pid=$!
    log_debug "Started connector process with PID: $connector_pid"

    # Verify the process actually started (brief delay to allow process to initialize)
    log_debug "Waiting 1 second for process initialization..."
    sleep 1
    if ! kill -0 "$connector_pid" 2>/dev/null; then
        echo "[ERROR] Connector process (PID: $connector_pid) failed to start or crashed immediately" >&2
        echo "[ERROR] Check log file for details: $LOG_FILE" >&2
        if [[ -s "$LOG_FILE" ]]; then
            echo "[ERROR] Last log entries:" >&2
            tail -5 "$LOG_FILE" >&2
        fi
        exit 1
    fi
    log_debug "Process validation passed after 1 second"

    # Write PID file
    log_debug "Writing PID file: $PID_FILE"
    if ! write_pid_file "$PID_FILE" "$connector_pid"; then
        # If PID file creation fails, kill the process we started
        echo "[ERROR] Failed to write PID file: $PID_FILE" >&2
        echo "[ERROR] Terminating connector process..." >&2
        kill -TERM "$connector_pid" 2>/dev/null || true
        exit 1
    fi
    log_debug "PID file written successfully"

    # Wait a bit more to ensure the connector doesn't crash during initialization
    log_debug "Verifying connector startup stability (waiting 2 seconds)..."
    sleep 2
    if ! kill -0 "$connector_pid" 2>/dev/null; then
        echo "[ERROR] Connector process (PID: $connector_pid) crashed during startup" >&2
        echo "[ERROR] Check log file for errors: $LOG_FILE" >&2
        if [[ -s "$LOG_FILE" ]]; then
            echo "[ERROR] Recent log entries:" >&2
            tail -10 "$LOG_FILE" >&2
        fi
        cleanup_pid_file "$PID_FILE"
        exit 1
    fi
    log_debug "Startup stability verification passed"

    # Check if log file is being written to (indicates successful startup)
    if [[ ! -s "$LOG_FILE" ]]; then
        log_debug "Warning: Log file is empty after 3 seconds. Process may be starting slowly or may have issues."
    else
        log_debug "Log file confirmed: $(wc -l < "$LOG_FILE") lines written"
        
        # Check for immediate error messages in the log (excluding direnv loading messages)
        # Filter out false positives like "direnv: loading" which contains "error" in the path
        if grep -v "^.*direnv: loading" "$LOG_FILE" 2>/dev/null | grep -q -i "error\|fatal\|panic"; then
            log_error "Errors detected in startup logs:"
            grep -v "^.*direnv: loading" "$LOG_FILE" | grep -i "error\|fatal\|panic" | head -5 >&2
            log_error "Full log available at: $LOG_FILE"
            # Don't kill the process - let the user decide based on the error
        fi
    fi

    log_info "Connector started successfully with PID $connector_pid"
    log_info "Monitor progress with: tail -f $LOG_FILE"
    log_info "Use '$0 wait' to wait for completion or '$0 stop' to stop"
}

# Wait for processing completion via row count
action_wait() {
    log_info "Waiting for processing completion..."

    [[ -f "$LOG_FILE" ]] || die "Log file $LOG_FILE not found. Did you run '$0 run' first?"

    # Wait for expected row count with timeout
    if wait_for_row_count "$LOG_FILE" "$NUM_MESSAGES" 300 5 "$PID_FILE"; then
        log_info "Processing completed successfully!"
    else
        die "Processing did not complete within timeout"
    fi
}

# Stop connector gracefully using PID file
action_stop() {
    log_info "Stopping connector..."

    if [[ ! -f "$PID_FILE" ]]; then
        log_info "PID file not found, connector may not be running"
        return 0
    fi

    local pid
    if ! pid=$(read_pid_file "$PID_FILE" 2>/dev/null); then
        log_info "Connector is not running, cleaning up PID file"
        cleanup_pid_file "$PID_FILE"
        return 0
    fi

    log_info "Sending SIGTERM to connector (PID: $pid)"
    if kill -TERM "$pid" 2>/dev/null; then
        # Wait for graceful shutdown
        local timeout=10
        while [[ $timeout -gt 0 ]] && kill -0 "$pid" 2>/dev/null; do
            sleep 1
            timeout=$((timeout - 1))
        done

        if kill -0 "$pid" 2>/dev/null; then
            log_info "Connector did not stop gracefully, sending SIGKILL"
            kill -KILL "$pid" 2>/dev/null || true
        else
            log_info "Connector stopped gracefully"
        fi
    else
        log_info "Connector was not running"
    fi

    cleanup_pid_file "$PID_FILE"
    log_info "Connector stopped and PID file cleaned up"
}

# Export data from QuasarDB to CSV files
action_export() {
    log_info "Exporting data from QuasarDB to CSV files..."

    # Create export directory
    mkdir -p "$EXPORT_DIR"

    # Export table to CSV
    local csv_file="$EXPORT_DIR/${TABLE_NAME}.csv"
    log_info "Exporting table $TABLE_NAME to $csv_file"

    if ! qdb_export --ts "$TABLE_NAME" --start-date "2000-01-01T00:00:00" --end-date "2100-01-01T00:00:00" -f "$csv_file" -c "$QDB_URI"; then
        die "Failed to export table $TABLE_NAME"
    fi

    local row_count
    row_count=$(tail -n +2 "$csv_file" | wc -l)  # Skip header row
    log_debug "Exported $row_count rows from $TABLE_NAME"

    log_info "Export completed to directory: $EXPORT_DIR"
}

# Compare exported data with golden data
action_validate() {
    log_info "Validating exported data against golden data..."

    require_command numdiff

    [[ -d "$EXPORT_DIR" ]] || die "Export directory $EXPORT_DIR not found. Run '$0 export' first"

    local golden_dir="$TESTDATA_DIR/golden"
    [[ -d "$golden_dir" ]] || die "Golden data directory $golden_dir not found"

    local validation_errors=0

    local exported_csv="$EXPORT_DIR/${TABLE_NAME}.csv"
    local golden_csv="$golden_dir/${TABLE_NAME}.csv"

    if [[ ! -f "$exported_csv" ]]; then
        log_error "Exported CSV not found: $exported_csv"
        validation_errors=$((validation_errors + 1))
    elif [[ ! -f "$golden_csv" ]]; then
        log_error "Golden CSV not found: $golden_csv"
        validation_errors=$((validation_errors + 1))
    else
        log_debug "Comparing $exported_csv with $golden_csv"

        # Use numdiff for numeric tolerance comparison
        if numdiff --tolerance=0.001 --brief "$exported_csv" "$golden_csv" >/dev/null 2>&1; then
            log_info "✓ $TABLE_NAME validation passed"
        else
            log_error "✗ $TABLE_NAME validation failed"
            validation_errors=$((validation_errors + 1))

            # Show detailed differences
            log_debug "Detailed differences for $TABLE_NAME:"
            numdiff --tolerance=0.001 "$exported_csv" "$golden_csv" || true
        fi
    fi

    if [[ $validation_errors -eq 0 ]]; then
        log_info "All validations passed successfully!"
    else
        die "Validation failed with $validation_errors errors"
    fi
}

# Organize files for golden data packaging
action_prepare_golden() {
    log_info "Preparing golden data package..."

    [[ -d "$EXPORT_DIR" ]] || die "Export directory $EXPORT_DIR not found. Run '$0 export' first"

    local golden_dir="$TESTDATA_DIR/golden"
    mkdir -p "$golden_dir"

    # Copy exported CSV file to golden directory
    local csv_file="$EXPORT_DIR/${TABLE_NAME}.csv"
    if [[ -f "$csv_file" ]]; then
        cp "$csv_file" "$golden_dir/"
        log_debug "Copied $csv_file to golden directory"
    else
        log_error "CSV file not found: $csv_file"
    fi

    # Create metadata.json
    local metadata_file="$golden_dir/metadata.json"
    local git_hash
    git_hash=$(git rev-parse HEAD 2>/dev/null || echo "unknown")

    cat > "$metadata_file" << EOF
{
  "generated_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "scenario": "network-metrics",
  "num_messages": $NUM_MESSAGES,
  "tables": ["$TABLE_NAME"],
  "git_hash": "$git_hash",
  "connector_version": "$(cd "$SCRIPT_DIR/.." && go version || echo 'unknown')"
}
EOF

    log_info "Golden data package prepared in: $golden_dir"
    log_info "Metadata written to: $metadata_file"
}

# Main action dispatcher
if [[ $# -eq 0 ]]; then
    log_error "No action specified"
    usage
fi

case "$1" in
    create)
        action_create
        ;;
    generate)
        action_generate
        ;;
    load)
        action_load
        ;;
    run)
        action_run
        ;;
    wait)
        action_wait
        ;;
    stop)
        action_stop
        ;;
    export)
        action_export
        ;;
    validate)
        action_validate
        ;;
    prepare-golden)
        action_prepare_golden
        ;;
    *)
        log_error "Unknown action: '$1'"
        echo >&2
        usage
        ;;
esac
