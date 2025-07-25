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
DATA_FILE="$SCRIPT_DIR/industrial-sensor-data.jsonl"
CONFIG_FILE="$SCRIPT_DIR/industrial-sensor.yaml"
STREAM_NAME="INDUSTRIAL_STREAM"
SUBJECT="industrial.temperature"
NUM_MESSAGES="${NUM_MESSAGES:-1000}"
PID_FILE="$SCRIPT_DIR/industrial-sensor-connector.pid"
LOG_FILE="$SCRIPT_DIR/industrial-sensor-connector.log"
CONNECTOR_BINARY="$SCRIPT_DIR/../qdb-nats-connector"
EXPORT_DIR="$SCRIPT_DIR/exports"
TESTDATA_DIR="${TESTDATA_DIR:-$SCRIPT_DIR/testdata}"

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
    echo "  TESTDATA_DIR   - Directory for test data (default: ./testdata)"
    echo "  DEBUG          - Enable debug logging (1 = enabled)"
    echo
    echo "Requirements:"
    echo "  GNU parallel   - Required for optimized data generation and loading"
    exit 1
}

# Generate test dataset
action_generate() {
    log_info "Generating $NUM_MESSAGES industrial sensor messages..."

    # Require GNU parallel for optimization
    require_command parallel

    local start_timestamp=1737018000  # 2025-01-16T09:00:00Z

    # Clear the data file
    cat > "$DATA_FILE" << 'EOF'
EOF

    # Generate all messages with a single gawk script (GNU awk required for strftime)
    TZ=UTC gawk -v num_messages="$NUM_MESSAGES" \
        -v start_timestamp="$start_timestamp" \
        -v sensors="sensor-01:Floor1:B1:1,sensor-02:Floor2:B1:2,sensor-03:Floor3:B1:3,sensor-04:Basement:B2:B,sensor-05:Rooftop:B2:R" \
        'BEGIN {
            # Initialize random seed
            srand()

            # Parse sensor configurations
            split(sensors, sensor_array, ",")

            # Error values for 10% error rate
            error_values[0] = "ERROR"
            error_values[1] = "N/A"
            error_values[2] = "SENSOR_FAULT"

            # Generate all messages
            for (i = 0; i < num_messages; i++) {
                # Calculate timestamp for this message
                timestamp_epoch = start_timestamp + i * 10

                # Format timestamp properly using strftime (GNU awk)
                # This handles all date/month/year transitions automatically
                formatted_timestamp = strftime("%Y-%m-%d %H:%M:%S", timestamp_epoch)

                # Sensor rotation (i % 5) + 1 for 1-based indexing
                sensor_idx = (i % 5) + 1
                sensor_info = sensor_array[sensor_idx]

                # Parse sensor configuration: sensor_id:location:building:floor
                split(sensor_info, sensor_parts, ":")
                sensor_id = sensor_parts[1]
                location = sensor_parts[2]
                building = sensor_parts[3]
                floor_val = sensor_parts[4]

                # Generate temperature readings with 10% error rate
                error_chance = int(rand() * 100)
                if (error_chance < 10) {
                    # Generate error values (rotate through the 3 error types)
                    temp_val = error_values[error_chance % 3]
                } else {
                    # Generate temperature in 15-35°C range
                    temp_val = sprintf("%.1f", 15 + rand() * 20)
                }

                # Format JSON message (maintaining exact same structure)
                printf "{\"sensor\": {\"id\": \"%s\", \"location\": \"%s\"}, \"building\": \"%s\", \"floor\": \"%s\", \"timestamp\": \"%s\", \"temp\": \"%s\"}\n",
                       sensor_id, location, building, floor_val, formatted_timestamp, temp_val
            }
        }' >> "$DATA_FILE"

    # Check if awk processing succeeded
    if [[ $? -ne 0 ]]; then
        die "Failed to generate sensor messages"
    fi

    log_info "Generated $NUM_MESSAGES industrial sensor messages in $DATA_FILE"
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

    log_info "QuasarDB dynamic table creation completed"
}

# Load generated data into NATS JetStream
action_load() {
    log_info "Loading data into NATS JetStream..."

    # Validate environment first
    validate_environment
    require_command nats
    require_command parallel

    # Generate data if not exists
    if [[ ! -f "$DATA_FILE" ]]; then
        log_info "Data file not found, generating first..."
        action_generate
    fi

    # Check if stream exists
    if ! nats stream info "$STREAM_NAME" >/dev/null 2>&1; then
        die "Stream $STREAM_NAME does not exist. Run '$0 create' first"
    fi

    local lines_count
    lines_count=$(wc -l < "$DATA_FILE")
    log_info "Publishing $lines_count industrial sensor messages to $SUBJECT using parallel processing..."

    # Create a temporary file to track publishing results
    local temp_results
    temp_results=$(mktemp)

    # Parallel publishing function to be executed by GNU parallel
    export -f log_debug
    export SUBJECT
    export NATS_URL

    # Use parallel to process messages in batches
    # -j+0: use all available CPU cores
    # --pipe: read from stdin
    # -N100: process 100 lines per job
    # --halt now,fail=1: stop on first failure
    cat "$DATA_FILE" | parallel -j+0 --pipe -N100 --halt now,fail=1 "
        batch_num=\$((PARALLEL_SEQ))
        published_in_batch=0
        failed_in_batch=0

        while IFS= read -r line; do
            if [[ -n \"\$line\" ]]; then
                # Publish JSON message directly (no base64 decoding needed)
                if echo \"\$line\" | nats pub \"$SUBJECT\" --force-stdin -q 2>/dev/null; then
                    published_in_batch=\$((published_in_batch + 1))
                else
                    failed_in_batch=\$((failed_in_batch + 1))
                fi
            fi
        done

        # Report batch results
        echo \"Batch \$batch_num: published \$published_in_batch, failed \$failed_in_batch\"

        # Exit with error if any failures in this batch
        if [[ \$failed_in_batch -gt 0 ]]; then
            exit 1
        fi
    " > "$temp_results"

    # Check if parallel processing succeeded
    local parallel_exit_code=$?
    if [[ $parallel_exit_code -ne 0 ]]; then
        log_error "Parallel publishing failed. Batch results:"
        cat "$temp_results" >&2
        die "Failed to publish messages in parallel"
    fi

    # Count total published messages from batch results
    local total_published
    total_published=$(awk '/^Batch [0-9]+:/ { sum += $4 } END { print sum+0 }' "$temp_results")

    # Validate we actually published messages
    if [[ $total_published -eq 0 ]]; then
        log_error "CRITICAL: No messages were published!"
        log_error "Parallel output:"
        cat "$temp_results" >&2
        die "Failed to publish any messages"
    fi

    # Also validate against expected count
    if [[ $total_published -ne $lines_count ]]; then
        log_error "WARNING: Published count mismatch - expected $lines_count, got $total_published"
        log_error "Some messages may have failed to publish"
    fi

    log_info "Parallel publishing completed successfully"
    log_debug "Batch processing results:"
    cat "$temp_results" | head -10  # Show first 10 batch results
    if [[ $(wc -l < "$temp_results") -gt 10 ]]; then
        log_debug "... and $(($(wc -l < "$temp_results") - 10)) more batches"
    fi

    log_info "Data loaded successfully ($total_published messages published)"

    # Clean up temporary file
    rm -f "$temp_results"

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

    # Start connector in background and capture PID
    direnv exec . "$CONNECTOR_BINARY" \
        --nats "$NATS_URL" \
        --qdb "$QDB_URI" \
        --stream "$STREAM_NAME" \
        --consumer industrial-connector \
        --workers 2 \
        --parser yaml \
        --parser-config "$CONFIG_FILE" \
        > "$LOG_FILE" 2>&1 &

    local connector_pid=$!


    # Write PID file
    write_pid_file "$PID_FILE" "$connector_pid"

    log_info "Connector started with PID $connector_pid"
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

    # Export each table to CSV
    for table_name in "${DYNAMIC_TABLES[@]}"; do
        local csv_file="$EXPORT_DIR/${table_name}.csv"
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

    local golden_dir="$TESTDATA_DIR/golden"
    [[ -d "$golden_dir" ]] || die "Golden data directory $golden_dir not found"

    local validation_errors=0

    for table_name in "${DYNAMIC_TABLES[@]}"; do
        local exported_csv="$EXPORT_DIR/${table_name}.csv"
        local golden_csv="$golden_dir/${table_name}.csv"

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

            # Show detailed differences
            log_debug "Detailed differences for $table_name:"
            numdiff --tolerance=0.001 "$exported_csv" "$golden_csv" || true
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

    local golden_dir="$TESTDATA_DIR/golden"
    mkdir -p "$golden_dir"

    # Copy exported CSV files to golden directory
    for table_name in "${DYNAMIC_TABLES[@]}"; do
        local csv_file="$EXPORT_DIR/${table_name}.csv"
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
