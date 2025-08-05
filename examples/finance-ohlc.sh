#!/bin/bash
# Finance OHLC market data example - Modular actions for golden data testing
set -euo pipefail

# Script configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source common infrastructure
source "$SCRIPT_DIR/common.sh"

# Setup standardized error trapping
setup_error_trap

# Script-specific configuration
DATA_FILE="$SCRIPT_DIR/finance-ohlc-data.jsonl"
CONFIG_FILE="$SCRIPT_DIR/finance-ohlc.yaml"
STREAM_NAME="FINANCE_STREAM"
SUBJECT="finance.ohlc"
NUM_MESSAGES="${NUM_MESSAGES:-1000}"
PID_FILE="$SCRIPT_DIR/finance-ohlc-connector.pid"
LOG_FILE="$SCRIPT_DIR/finance-ohlc-connector.log"
CONNECTOR_BINARY="$SCRIPT_DIR/../bin/qdb-nats-connector"
EXPORT_DIR="$SCRIPT_DIR/exports"
TESTDATA_DIR="${TESTDATA_DIR:-$SCRIPT_DIR/testdata}"

# Stock configuration for data generation
declare -a STOCKS=("AAPL" "GOOGL" "MSFT" "AMZN" "TSLA")
declare -a BASE_PRICES=(185.50 175.25 425.75 195.80 245.30)
declare -a DYNAMIC_TABLES=(
    "finance.NASDAQ.AAPL"
    "finance.NASDAQ.GOOGL"
    "finance.NASDAQ.MSFT"
    "finance.NASDAQ.AMZN"
    "finance.NASDAQ.TSLA"
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
    exit 1
}

action_generate() {
    # Verify NUM_MESSAGES is a valid number
    if ! [[ "$NUM_MESSAGES" =~ ^[0-9]+$ ]] || [[ "$NUM_MESSAGES" -eq 0 ]]; then
        die "Invalid NUM_MESSAGES value: '$NUM_MESSAGES'. Must be a positive integer."
    fi

    log_info "Generating $NUM_MESSAGES OHLC messages..."
    ../bin/qdb-data-gen finance-ohlc-generator.yaml --count "$NUM_MESSAGES" > "$DATA_FILE"
    log_info "Generated $NUM_MESSAGES messages in $DATA_FILE"
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
    nats stream add "$STREAM_NAME" --subjects "finance.>" --retention limits --defaults || \
        die "Failed to create NATS stream $STREAM_NAME"
    log_info "Stream $STREAM_NAME created successfully"

    # Create consumer for the connector
    log_info "Creating NATS consumer: finance-connector"
    if nats consumer info "$STREAM_NAME" finance-connector >/dev/null 2>&1; then
        log_info "Consumer finance-connector already exists, skipping consumer creation"
    else
        nats consumer add "$STREAM_NAME" finance-connector --pull --deliver all --ack explicit --defaults || \
            die "Failed to create NATS consumer finance-connector"
        log_info "Consumer finance-connector created successfully"
    fi

    # Create QuasarDB dynamic tables for each exchange.symbol combination
    log_info "Creating QuasarDB dynamic tables for OHLC data routing..."

    # Silent cleanup: Drop existing tables if they exist
    for table_name in "${DYNAMIC_TABLES[@]}"; do
        drop_table_if_exists "$table_name"
    done

    local table_schema="(stock_id STRING, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume INT64, trading_pair STRING)"

    # Create each dynamic table
    for table_name in "${DYNAMIC_TABLES[@]}"; do
        log_debug "Creating table: $table_name"
        qdbsh -c "CREATE TABLE \"$table_name\"$table_schema" || \
            die "Failed to create table $table_name"
    done

    log_info "QuasarDB dynamic table creation completed"
}

action_load() {
    log_info "Loading data into NATS JetStream..."

    # Validate environment first
    validate_environment
    require_command nats

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
    ../bin/qdb-data-loader --file "$DATA_FILE" --topic "$SUBJECT" --stream "$STREAM_NAME" \
                        --nats-url "$NATS_URL" --batch-size 100
    log_info "Data loaded successfully"
    
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
        --consumer finance-connector \
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
  "scenario": "finance-ohlc",
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
