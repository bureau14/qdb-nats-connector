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
    echo
    echo "Requirements:"
    echo "  GNU parallel   - Required for optimized data generation and loading"
    exit 1
}

# Function to compress individual messages using gzip and encode as base64
compress_message() {
    echo -n "$1" | gzip -c | base64 -w 0
}

# Generate test dataset
action_generate() {
    log_info "Generating $NUM_MESSAGES OHLC market messages..."

    # Require GNU parallel for optimization
    require_command parallel

    local start_timestamp=1736948200  # 2025-01-16T14:30:00Z

    # Create temporary file for atomic operation
    local temp_file
    temp_file=$(mktemp "${DATA_FILE}.tmp.XXXXXX")

    # Set up cleanup trap for temporary file
    trap 'rm -f "${temp_file:-}"' EXIT INT TERM

    # Generate all messages with a single awk script, then parallelize compression
    # Use > instead of >> to fix race condition - parallel outputs complete stream
    awk -v num_messages="$NUM_MESSAGES" \
        -v start_timestamp="$start_timestamp" \
        -v stocks="AAPL,GOOGL,MSFT,AMZN,TSLA" \
        -v base_prices="185.50,175.25,425.75,195.80,245.30" \
        'BEGIN {
            # Initialize random seed
            srand()

            # Parse stocks and prices arrays
            split(stocks, stock_array, ",")
            split(base_prices, price_array, ",")

            # Generate all messages
            for (i = 0; i < num_messages; i++) {
                timestamp = start_timestamp + i * 60
                stock_idx = (i % 5) + 1  # awk arrays are 1-indexed
                stock_id = stock_array[stock_idx]
                base_price = price_array[stock_idx]

                # Generate OHLC with constraints: high >= max(open,close), low <= min(open,close)
                open_price = base_price + (rand() - 0.5) * base_price * 0.1
                movement = open_price * 0.04
                high_price = open_price + rand() * movement
                low_price = open_price - rand() * movement
                close_price = low_price + rand() * (high_price - low_price)
                volume = int(10000 + rand() * 4990000)

                # Map stocks to exchanges for realistic data
                exchange = "NASDAQ"  # All these stocks are NASDAQ

                # Format JSON message (maintaining exact same structure)
                printf "{\"market\": {\"exchange\": \"%s\", \"symbol\": \"%s\"}, \"timestamp\": %d, \"o\": %.2f, \"h\": %.2f, \"l\": %.2f, \"c\": %.2f, \"v\": %d}\n",
                       exchange, stock_id, timestamp, open_price, high_price, low_price, close_price, volume
            }
        }' | parallel -j+0 --pipe -N1 'gzip -c | base64 -w 0' > "$temp_file"

    # Check if parallel processing succeeded
    if [[ ${PIPESTATUS[0]} -ne 0 || ${PIPESTATUS[1]} -ne 0 ]]; then
        rm -f "$temp_file"  # Clean up temporary file
        die "Failed to generate and compress messages"
    fi

    # Validate generated data before moving
    local generated_lines
    generated_lines=$(wc -l < "$temp_file" 2>/dev/null || echo 0)
    if [[ $generated_lines -eq 0 ]]; then
        rm -f "$temp_file"  # Clean up temporary file
        die "Generated data file is empty"
    fi

    if [[ $generated_lines -ne $NUM_MESSAGES ]]; then
        rm -f "$temp_file"  # Clean up temporary file
        die "Generated $generated_lines lines, expected $NUM_MESSAGES"
    fi

    # Atomically move temporary file to final location
    if ! mv "$temp_file" "$DATA_FILE"; then
        rm -f "$temp_file"  # Clean up temporary file
        die "Failed to move generated data file"
    fi

    log_info "Generated $NUM_MESSAGES OHLC messages in $DATA_FILE"
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

    # Validate data file for streaming
    if [[ ! -r "$DATA_FILE" ]]; then
        die "Data file $DATA_FILE is not readable"
    fi

    # Check file size for memory-efficient processing
    local file_size_bytes
    file_size_bytes=$(wc -c < "$DATA_FILE" 2>/dev/null || echo 0)
    if [[ $file_size_bytes -gt $((100 * 1024 * 1024)) ]]; then  # 100MB threshold
        log_info "Large data file detected (${file_size_bytes} bytes) - using streaming mode"
    fi

    # Check if stream exists
    if ! nats stream info "$STREAM_NAME" >/dev/null 2>&1; then
        die "Stream $STREAM_NAME does not exist. Run '$0 create' first"
    fi

    local lines_count
    lines_count=$(wc -l < "$DATA_FILE")
    log_info "Publishing $lines_count compressed OHLC messages to $SUBJECT using streaming parallel processing..."

    # Create a temporary file to track publishing results
    local temp_results
    temp_results=$(mktemp)

    # Set up cleanup trap for temporary file
    trap 'rm -f "${temp_results:-}"' EXIT INT TERM

    # Parallel publishing function to be executed by GNU parallel
    export -f log_debug
    export SUBJECT
    export NATS_URL

    # Use parallel to process messages in batches with streaming
    # -j+0: use all available CPU cores
    # --pipe: read from stdin in streaming mode
    # -N100: process 100 lines per job for optimal batching
    # --halt now,fail=1: stop on first failure
    # Use < instead of cat for more efficient streaming
    parallel -j+0 --pipe -N100 --halt now,fail=1 < "$DATA_FILE" "
        batch_num=\$((PARALLEL_SEQ))
        batch_start=\$(((batch_num - 1) * 100 + 1))
        published_in_batch=0
        failed_in_batch=0

        while IFS= read -r line; do
            if [[ -n \"\$line\" ]]; then
                # Decode base64 and publish raw gzipped bytes
                if echo \"\$line\" | base64 -d | nats pub \"$SUBJECT\" --force-stdin -q 2>/dev/null; then
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
