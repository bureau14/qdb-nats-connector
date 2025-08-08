#!/bin/bash
# Industrial sensor monitoring example - Modular actions for golden data testing
set -euo pipefail

# Script configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source common infrastructure
source "$SCRIPT_DIR/common.sh"

# Setup standardized error trapping
setup_error_trap

# Script-specific configuration
EXAMPLE="industrial-sensor"
DATA_FILE="$SCRIPT_DIR/industrial-sensor-data.jsonl"
CONFIG_FILE="$SCRIPT_DIR/industrial-sensor.yaml"
STREAM_NAME="INDUSTRIAL_STREAM"
SUBJECT="industrial.temperature"
NUM_MESSAGES="${NUM_MESSAGES:-1000}"
PID_FILE="$SCRIPT_DIR/industrial-sensor-connector.pid"
LOG_FILE="$SCRIPT_DIR/industrial-sensor-connector.log"
CONNECTOR_BINARY="$SCRIPT_DIR/../bin/qdb-nats-connector"
EXPORT_DIR="$SCRIPT_DIR/exports"
TESTDATA_DIR="${TESTDATA_DIR:-$SCRIPT_DIR/testdata}"

# Define INPUT_FILE with row-count naming for consistent access across functions
INPUT_FILE="$TESTDATA_DIR/${EXAMPLE}-${NUM_MESSAGES}-input.data"

# Worker configuration
WORKERS=${WORKERS:-$(get_default_workers)}
CPU_COUNT=$(get_cpu_count)

# Calculate timeout based on message volume
# Base: 30s + (messages / 10000) * 3s
calculate_timeout() {
    local messages=$1
    local base_timeout=30
    local scaling_factor=$((messages / 10000 * 3))
    echo $((base_timeout + scaling_factor))
}

# Sensor configuration for data generation - map to building/floor combinations
declare -a SENSORS=(
    "sensor-01:Floor1:B1:1"
    "sensor-02:Floor2:B1:2"
    "sensor-03:Floor3:B1:3"
    "sensor-04:Basement:B2:B"
    "sensor-05:Rooftop:B2:R"
)

# Dynamic tables based on building.floor combinations from requirements
declare -a DYNAMIC_TABLES=(
    "industrial.B1.1"
    "industrial.B1.2"
    "industrial.B1.3"
    "industrial.B2.B"
    "industrial.B2.R"
)

usage() {
    echo "Usage: $0 {create|generate|load|run|wait|stop|export|validate|prepare-golden}"
    echo
    echo "Actions:"
    echo "  create         - Create NATS JetStream stream and QuasarDB tables"
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
    echo "  WORKERS        - Number of workers to use (default: auto-detected from CPU)"
    echo "  TESTDATA_DIR   - Directory for test data (default: ./testdata)"
    echo "  DEBUG          - Enable debug logging (1 = enabled)"
    echo
    echo "Requirements:"
    echo "  GNU parallel   - Required for optimized data generation and loading"
    exit 1
}

# Generate test dataset
action_generate() {
    # Verify NUM_MESSAGES is a valid number
    if ! [[ "$NUM_MESSAGES" =~ ^[0-9]+$ ]] || [[ "$NUM_MESSAGES" -eq 0 ]]; then
        die "Invalid NUM_MESSAGES value: '$NUM_MESSAGES'. Must be a positive integer."
    fi

    # Ensure testdata directory exists
    mkdir -p "$TESTDATA_DIR"
    
    log_info "Generating $NUM_MESSAGES industrial sensor messages..."
    
    # Generate data and count actual lines produced
    # First generate raw data to debug
    local temp_raw_file="${INPUT_FILE}.raw"
    ../bin/qdb-data-gen industrial-sensor-generator.yaml --count "$NUM_MESSAGES" --workers "$WORKERS" > "$temp_raw_file"
    
    # DEBUG: Check raw data
    log_info "[DEBUG] First 3 lines of raw generated data:"
    head -3 "$temp_raw_file" | while IFS= read -r line; do
        log_debug "[DEBUG] Raw: $line"
    done
    
    # Now transform with better error handling
    cat "$temp_raw_file" | \
    jq -c 'try {
      sensor: {
        id: (if .sensor_id | type == "string" then .sensor_id else "sensor-0" + ((.sensor_id - 1) % 5 + 1 | tostring) end),
        location: (if .sensor_location | type == "string" then .sensor_location else ["Floor1", "Floor2", "Floor3", "Basement", "Rooftop"][(.sensor_location - 1) % 5] end)
      },
      building: (if .building | type == "string" then .building else ["B1", "B1", "B1", "B2", "B2"][(.building - 1) % 5] end),
      floor: (if .floor | type == "string" then .floor else ["1", "2", "3", "B", "R"][(.floor - 1) % 5] end),
      timestamp: .timestamp,
      temp: (
        if .temp_error_flag <= 10 then "ERROR"
        elif .temp_error_flag <= 13 then "N/A"  
        elif .temp_error_flag <= 15 then "SENSOR_FAULT"
        else (.temp_value | tostring)
        end
      )
    } catch {
      sensor: {id: "sensor-01", location: "Floor1"},
      building: "B1",
      floor: "1", 
      timestamp: (if .timestamp then .timestamp else "2025-01-16 09:00:00" end),
      temp: "ERROR"
    }' > "$INPUT_FILE" 2>&1
    
    local jq_exit_code=$?
    if [[ $jq_exit_code -ne 0 ]]; then
        log_error "[DEBUG] jq transformation failed with exit code: $jq_exit_code"
        log_error "[DEBUG] Check the raw file: $temp_raw_file"
    fi
    
    # Clean up raw file
    rm -f "$temp_raw_file"
    
    # Keep DATA_FILE for backward compatibility during transition
    cp "$INPUT_FILE" "$DATA_FILE"
    
    # DEBUG: Verify actual number of lines generated
    local actual_lines=$(wc -l < "$INPUT_FILE")
    log_info "[DEBUG] Actually generated $actual_lines lines in $INPUT_FILE"
    
    if [[ $actual_lines -ne $NUM_MESSAGES ]]; then
        log_error "[DEBUG] WARNING: Expected $NUM_MESSAGES lines but got $actual_lines lines"
    fi
    
    log_info "Generated $NUM_MESSAGES industrial sensor messages in $INPUT_FILE"
}

# Create NATS JetStream stream and QuasarDB tables
action_create() {
    log_info "Creating NATS JetStream stream and QuasarDB tables..."

    # Validate environment first
    validate_environment

    # Require necessary commands
    require_command nats

    # Silent cleanup: Delete existing NATS stream if it exists
    nats stream delete "$STREAM_NAME" --force 2>/dev/null || true

    # Create NATS stream
    log_info "Creating NATS JetStream stream: $STREAM_NAME"
    nats stream add "$STREAM_NAME" --subjects "industrial.>" --retention limits --defaults || \
        die "Failed to create NATS stream $STREAM_NAME"
    log_info "Stream $STREAM_NAME created successfully"

    # Create consumer for the connector
    log_info "Creating NATS consumer: industrial-connector"
    if nats consumer info "$STREAM_NAME" industrial-connector >/dev/null 2>&1; then
        log_info "Consumer industrial-connector already exists, skipping consumer creation"
    else
        nats consumer add "$STREAM_NAME" industrial-connector --pull --deliver all --ack explicit --defaults || \
            die "Failed to create NATS consumer industrial-connector"
        log_info "Consumer industrial-connector created successfully"
    fi

    # Create QuasarDB dynamic tables for each building.floor combination
    log_info "Creating QuasarDB dynamic tables for industrial sensor routing..."

    # Silent cleanup: Drop existing tables if they exist
    for table_name in "${DYNAMIC_TABLES[@]}"; do
        drop_table_if_exists "$table_name"
    done

    local table_schema="(sensor_id STRING, measurement DOUBLE, sensor_tag STRING)"

    # Create each dynamic table
    for table_name in "${DYNAMIC_TABLES[@]}"; do
        log_debug "Creating table: $table_name"
        qdbsh -c "CREATE TABLE \"$table_name\"$table_schema" || \
            die "Failed to create table $table_name"
    done

    # Show worker configuration
    log_info "CPU cores detected: $CPU_COUNT"
    log_info "Workers configured: $WORKERS"

    log_info "QuasarDB dynamic table creation completed"
}

# Load generated data into NATS JetStream
action_load() {
    log_info "Loading data into NATS JetStream..."

    # Validate environment first
    validate_environment
    require_command nats

    # Determine which data file to use
    local data_to_load=""
    if [[ -n "$INPUT_FILE" ]] && [[ -f "$INPUT_FILE" ]]; then
        data_to_load="$INPUT_FILE"
        log_debug "Using INPUT_FILE: $INPUT_FILE"
    elif [[ -f "$DATA_FILE" ]]; then
        data_to_load="$DATA_FILE"
        log_debug "Using DATA_FILE: $DATA_FILE"
    else
        die "Input data not found. Run '$0 generate' or 'make generate-golden EXAMPLE=$EXAMPLE SIZE=<size>' first"
    fi

    # DEBUG: Check actual number of lines in data file
    local line_count=$(wc -l < "$data_to_load")
    log_info "[DEBUG] Data file contains $line_count lines"

    # Check if stream exists
    if ! nats stream info "$STREAM_NAME" >/dev/null 2>&1; then
        die "Stream $STREAM_NAME does not exist. Run '$0 create' first"
    fi

    # Calculate appropriate timeout
    local load_timeout=$(calculate_timeout "$NUM_MESSAGES")
    log_info "Using timeout of ${load_timeout}s for loading $NUM_MESSAGES messages"
    
    log_info "Loading data from $data_to_load into NATS JetStream..."
    
    # Run loader with calculated timeout
    timeout "$load_timeout" ../bin/qdb-data-loader --file "$data_to_load" --topic "$SUBJECT" --stream "$STREAM_NAME" \
                        --nats-url "$NATS_URL" --batch-size 500 --workers "$WORKERS"
    local loader_exit_code=$?
    
    if [[ $loader_exit_code -eq 124 ]]; then
        log_error "[DEBUG] Loader timed out after ${load_timeout} seconds!"
        log_info "[DEBUG] Checking NATS stream state:"
        nats stream info "$STREAM_NAME" | grep -E "(Messages:|State:)" || true
    else
        log_info "[DEBUG] Loader exit code: $loader_exit_code"
    fi
    
    log_info "Data loaded successfully"
    
    log_info "[DEBUG] Stream info after loading:"
    nats stream info "$STREAM_NAME"
}

# Start connector in background with PID management
action_run() {
    log_info "Starting qdb-nats-connector in background..."

    # Validate environment first
    validate_environment

    # Check required files
    [[ -f "$CONFIG_FILE" ]] || die "Config file $CONFIG_FILE not found"

    # Check if connector binary exists
    if [[ ! -f "$CONNECTOR_BINARY" ]]; then
        die "Connector binary not found at $CONNECTOR_BINARY. Please build it first with 'direnv exec . go build -o qdb-nats-connector main.go'"
    fi

    # Check if connector is already running
    if [[ -f "$PID_FILE" ]]; then
        if read_pid_file "$PID_FILE" >/dev/null 2>&1; then
            die "Connector is already running (PID file: $PID_FILE)"
        else
            log_info "Removing stale PID file: $PID_FILE"
            cleanup_pid_file "$PID_FILE"
        fi
    fi

    log_info "Starting connector with config: $CONFIG_FILE"
    log_info "Logs will be written to: $LOG_FILE"

    # DEBUG: Show the full command being executed
    log_info "[DEBUG] Full command: direnv exec . $CONNECTOR_BINARY --nats $NATS_URL --qdb $QDB_URI --stream $STREAM_NAME --consumer industrial-connector --workers $WORKERS --parser yaml --parser-config $CONFIG_FILE"

    # Start connector in background and capture PID
    direnv exec . "$CONNECTOR_BINARY" \
        --nats "$NATS_URL" \
        --qdb "$QDB_URI" \
        --stream "$STREAM_NAME" \
        --consumer industrial-connector \
        --workers "$WORKERS" \
        --parser yaml \
        --parser-config "$CONFIG_FILE" \
        > "$LOG_FILE" 2>&1 &

    local connector_pid=$!

    log_info "[DEBUG] Background process started with PID: $connector_pid"

    # Write PID file
    write_pid_file "$PID_FILE" "$connector_pid"

    # DEBUG: Verify process is still running after startup
    sleep 2
    if kill -0 "$connector_pid" 2>/dev/null; then
        log_info "[DEBUG] Connector process $connector_pid is running"
    else
        log_error "[DEBUG] Connector process $connector_pid exited immediately!"
        log_info "[DEBUG] Last 10 lines of log file:"
        tail -10 "$LOG_FILE" 2>/dev/null || log_error "Could not read log file"
        die "Connector failed to start"
    fi

    log_info "Connector started with PID $connector_pid"
    log_info "Use '$0 wait' to wait for completion or '$0 stop' to stop"
}

# Wait for processing completion via row count
action_wait() {
    log_info "Waiting for processing completion..."

    [[ -f "$LOG_FILE" ]] || die "Log file $LOG_FILE not found. Did you run '$0 run' first?"

    local wait_timeout=$(calculate_timeout "$NUM_MESSAGES")
    if wait_for_row_count "$LOG_FILE" "$NUM_MESSAGES" "$wait_timeout" 5 "$PID_FILE"; then
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

    # Check for existing exports to prevent accidental overwrites
    for table_name in "${DYNAMIC_TABLES[@]}"; do
        local csv_file="$EXPORT_DIR/${EXAMPLE}-${NUM_MESSAGES}-${table_name}.csv"
        if [[ -f "$csv_file" ]]; then
            die "Export file already exists: $csv_file. Clean exports/ directory before running: rm -rf $EXPORT_DIR"
        fi
    done

    # Export each table to CSV
    for table_name in "${DYNAMIC_TABLES[@]}"; do
        local csv_file="$EXPORT_DIR/${EXAMPLE}-${NUM_MESSAGES}-${table_name}.csv"
        log_info "Exporting table $table_name to $csv_file"

        if ! qdb_export --ts "$table_name" --start-date "2000-01-01T00:00:00" --end-date "2100-01-01T00:00:00" -f "$csv_file" -c "$QDB_URI"; then
            die "Failed to export table $table_name"
        fi

        local row_count
        row_count=$(tail -n +2 "$csv_file" | wc -l)  # Skip header row
        log_debug "Exported $row_count rows from $table_name"
    done

    log_info "Export completed to directory: $EXPORT_DIR"
}

# Compare exported data with golden data
action_validate() {
    log_info "Validating exported data against golden data..."

    require_command numdiff

    [[ -d "$EXPORT_DIR" ]] || die "Export directory $EXPORT_DIR not found. Run '$0 export' first"

    # Check if golden data has been extracted
    local golden_marker="$TESTDATA_DIR/$EXAMPLE-$NUM_MESSAGES/.extracted"
    [[ -f "$golden_marker" ]] || die "Golden data not extracted. Run 'make extract EXAMPLE=$EXAMPLE SIZE=<size>' first"

    local golden_dir="$TESTDATA_DIR/$EXAMPLE-$NUM_MESSAGES/expected"
    [[ -d "$golden_dir" ]] || die "Golden data directory $golden_dir not found"

    local validation_errors=0

    for table_name in "${DYNAMIC_TABLES[@]}"; do
        local exported_csv="$EXPORT_DIR/${EXAMPLE}-${NUM_MESSAGES}-${table_name}.csv"
        local golden_csv="$golden_dir/${EXAMPLE}-${NUM_MESSAGES}-${table_name}.csv"

        if [[ ! -f "$exported_csv" ]]; then
            log_error "Exported CSV not found: $exported_csv"
            validation_errors=$((validation_errors + 1))
            continue
        fi

        if [[ ! -f "$golden_csv" ]]; then
            log_error "Golden CSV not found: $golden_csv"
            validation_errors=$((validation_errors + 1))
            continue
        fi

        log_debug "Comparing $exported_csv with $golden_csv"

        # Use numdiff for numeric tolerance comparison
        if numdiff --tolerance=0.001 --brief "$exported_csv" "$golden_csv" >/dev/null 2>&1; then
            log_info "✓ $table_name validation passed"
        else
            log_error "✗ $table_name validation failed"
            validation_errors=$((validation_errors + 1))

            # Show first 10 differences
            log_debug "First 10 differences for $table_name:"
            numdiff --tolerance=0.001 "$exported_csv" "$golden_csv" 2>&1 | head -10 || true
            log_debug "Run for full diff: numdiff --tolerance=0.001 $exported_csv $golden_csv"
        fi
    done

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
    
    [[ -f "$INPUT_FILE" ]] || die "Input data file $INPUT_FILE not found. Run '$0 generate' first"

    local golden_dir="$TESTDATA_DIR/golden"
    mkdir -p "$golden_dir"

    # Copy input data with proper naming
    cp "$INPUT_FILE" "$golden_dir/${EXAMPLE}-${NUM_MESSAGES}-input.data"
    log_debug "Copied input data to golden directory"

    # Copy exported CSV files to golden directory (they already have row-count prefixes)
    for table_name in "${DYNAMIC_TABLES[@]}"; do
        local csv_file="$EXPORT_DIR/${EXAMPLE}-${NUM_MESSAGES}-${table_name}.csv"
        if [[ -f "$csv_file" ]]; then
            cp "$csv_file" "$golden_dir/"
            log_debug "Copied $csv_file to golden directory"
        else
            log_error "CSV file not found: $csv_file"
        fi
    done

    # Create metadata.json
    local metadata_file="$golden_dir/metadata.json"
    local git_hash
    git_hash=$(git rev-parse HEAD 2>/dev/null || echo "unknown")

    cat > "$metadata_file" << EOF
{
  "generated_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "scenario": "industrial-sensor",
  "num_messages": $NUM_MESSAGES,
  "tables": $(printf '%s\n' "${DYNAMIC_TABLES[@]}" | jq -R . | jq -s .),
  "git_hash": "$git_hash",
  "connector_version": "$(cd "$SCRIPT_DIR/.." && go version || echo 'unknown')"
}
EOF

    log_info "Golden data package prepared in: $golden_dir"
    log_info "Metadata written to: $metadata_file"
}

# Main action dispatcher
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
        usage
        ;;
esac
