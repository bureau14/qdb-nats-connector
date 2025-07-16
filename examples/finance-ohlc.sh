#!/bin/bash
# Finance OHLC market data example
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_FILE="$SCRIPT_DIR/finance-ohlc-data.jsonl"
CONFIG_FILE="$SCRIPT_DIR/finance-ohlc.yaml"
STREAM_NAME="FINANCE_STREAM"
SUBJECT="finance.ohlc"
NUM_MESSAGES=100

usage() {
    echo "Usage: $0 {create|load|run}"
    echo "  create  - Create NATS JetStream stream and QuasarDB table"
    echo "  load    - Generate and load data into NATS JetStream"
    echo "  run     - Run qdb-nats-connector and show all data"
    exit 1
}

generate() {
    echo "Generating $NUM_MESSAGES OHLC market messages..."
    START_TIMESTAMP=1736948200  # 2025-01-16T14:30:00Z
    declare -a STOCKS=("AAPL" "GOOGL" "MSFT" "AMZN" "TSLA")
    declare -a BASE_PRICES=(185.50 175.25 425.75 195.80 245.30)
    cat > "$DATA_FILE" << 'EOF'
EOF
    for ((i=0; i<NUM_MESSAGES; i++)); do
        timestamp=$((START_TIMESTAMP + i * 60))
        iso_timestamp=$(date -u -r $timestamp "+%Y-%m-%dT%H:%M:%SZ")
        stock_idx=$((i % 5))
        stock_id="${STOCKS[$stock_idx]}"
        base_price="${BASE_PRICES[$stock_idx]}"
        # Generate OHLC with constraints: high >= max(open,close), low <= min(open,close)
        open=$(awk "BEGIN {printf \"%.2f\", $base_price + (rand() - 0.5) * $base_price * 0.1}")
        movement=$(awk "BEGIN {printf \"%.2f\", $open * 0.04}")
        high=$(awk "BEGIN {printf \"%.2f\", $open + rand() * $movement}")
        low=$(awk "BEGIN {printf \"%.2f\", $open - rand() * $movement}")
        close=$(awk "BEGIN {printf \"%.2f\", $low + rand() * ($high - $low)}")
        volume=$(awk "BEGIN {printf \"%.0f\", 10000 + rand() * 4990000}")
        echo "{\"timestamp\": \"$iso_timestamp\", \"stock_id\": \"$stock_id\", \"open\": $open, \"high\": $high, \"low\": $low, \"close\": $close, \"volume\": $volume}" >> "$DATA_FILE"
    done
    echo "Generated $NUM_MESSAGES OHLC messages in $DATA_FILE"
}

create() {
    echo "Creating NATS JetStream stream and QuasarDB table..."
    
    # Create NATS stream
    echo "Creating NATS JetStream stream: $STREAM_NAME"
    if nats stream info "$STREAM_NAME" >/dev/null 2>&1; then
        echo "Warning: Stream $STREAM_NAME already exists, skipping NATS stream creation."
    else
        nats stream add "$STREAM_NAME" --subjects "finance.>" --retention limits --defaults
        echo "Stream $STREAM_NAME created successfully."
    fi
    
    # Create QuasarDB table
    echo "Creating QuasarDB table: finance_ohlc"
    qdbsh -c "CREATE TABLE finance_ohlc(stock_id STRING, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume INT64)" 2>&1 | grep -v "already exists" || true
    echo "QuasarDB table creation completed."
}

load() {
    echo "Generating and loading data into NATS JetStream..."

    # First generate the data
    generate

    # Then load it
    echo "Loading data from $DATA_FILE into subject $SUBJECT..."
    if [[ ! -f "$DATA_FILE" ]]; then
        echo "Error: Data file $DATA_FILE not found."
        exit 1
    fi

    # Check if stream exists
    if ! nats stream info "$STREAM_NAME" >/dev/null 2>&1; then
        echo "Error: Stream $STREAM_NAME does not exist. Run '$0 create' first."
        exit 1
    fi

    lines_count=$(wc -l < "$DATA_FILE")
    echo "Publishing $lines_count OHLC messages..."
    while IFS= read -r line; do
        echo "$line" | nats pub "$SUBJECT" --force-stdin -q
    done < "$DATA_FILE"
    echo "Data loaded successfully. Stream info:"
    nats stream info "$STREAM_NAME"
}

run() {
    echo "Running qdb-nats-connector..."
    if [[ ! -f "$CONFIG_FILE" ]]; then
        echo "Error: Config file $CONFIG_FILE not found"
        exit 1
    fi
    if [[ ! -f "$SCRIPT_DIR/../qdb-nats-connector" ]]; then
        echo "Building qdb-nats-connector..."
        cd "$SCRIPT_DIR/.." && direnv exec . go build -o qdb-nats-connector main.go && cd "$SCRIPT_DIR"
    fi
    echo "Starting connector with config: $CONFIG_FILE"
    "$SCRIPT_DIR/../qdb-nats-connector" \
        --nats nats://localhost:4222 \
        --qdb qdb://127.0.0.1:2836 \
        --stream "$STREAM_NAME" \
        --topic "$SUBJECT" \
        --parser yaml \
        --parser-config "$CONFIG_FILE"
}

case "${1:-}" in
    create) create ;;
    load) load ;;
    run) run ;;
    *) usage ;;
esac
