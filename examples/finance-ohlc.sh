#!/bin/bash
# Finance OHLC market data example
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_FILE="$SCRIPT_DIR/finance-ohlc-data.jsonl"
CONFIG_FILE="$SCRIPT_DIR/finance-ohlc.yaml"
STREAM_NAME="FINANCE_STREAM"
SUBJECT="finance.ohlc"
NUM_MESSAGES=1000

usage() {
    echo "Usage: $0 {create|load|run}"
    echo "  create  - Create NATS JetStream stream and QuasarDB table"
    echo "  load    - Generate and load data into NATS JetStream"
    echo "  run     - Run qdb-nats-connector and show all data"
    exit 1
}

# Function to compress individual messages using gzip and encode as base64
compress_message() {
    echo -n "$1" | gzip -c | base64 -w 0
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
        # Map stocks to exchanges for realistic data
        case "$stock_id" in
            "AAPL"|"MSFT"|"AMZN") exchange="NASDAQ" ;;
            "GOOGL"|"TSLA") exchange="NASDAQ" ;;
            *) exchange="NYSE" ;;
        esac
        json_message="{\"market\": {\"exchange\": \"$exchange\", \"symbol\": \"$stock_id\"}, \"timestamp\": $timestamp, \"o\": $open, \"h\": $high, \"l\": $low, \"c\": $close, \"v\": $volume}"
        compressed_message=$(compress_message "$json_message")
        echo "$compressed_message" >> "$DATA_FILE"
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

    # Create consumer for the connector
    echo "Creating NATS consumer: finance-connector"
    if nats consumer info "$STREAM_NAME" finance-connector >/dev/null 2>&1; then
        echo "Warning: Consumer finance-connector already exists, skipping consumer creation."
    else
        nats consumer add "$STREAM_NAME" finance-connector --pull --deliver all --ack explicit --defaults
        echo "Consumer finance-connector created successfully."
    fi

    # Create QuasarDB dynamic tables for each exchange.symbol combination
    echo "Creating QuasarDB dynamic tables for OHLC data routing..."
    
    # Define the tables needed based on the stock symbols and their exchanges
    declare -a DYNAMIC_TABLES=(
        "finance.NASDAQ.AAPL"
        "finance.NASDAQ.GOOGL"
        "finance.NASDAQ.MSFT"
        "finance.NASDAQ.AMZN"
        "finance.NASDAQ.TSLA"
    )
    
    # Schema for all OHLC tables
    TABLE_SCHEMA="(stock_id STRING, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume INT64, trading_pair STRING)"
    
    # Create each dynamic table
    for table_name in "${DYNAMIC_TABLES[@]}"; do
        echo "Creating table: $table_name"
        direnv exec . qdbsh -c "CREATE TABLE \"$table_name\"$TABLE_SCHEMA" 2>&1 | grep -v "already exists" || true
    done
    
    echo "QuasarDB dynamic table creation completed."
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
    echo "Publishing $lines_count compressed OHLC messages..."
    while IFS= read -r line; do
        # Decode base64 and publish raw gzipped bytes
        echo "$line" | base64 -d | nats pub "$SUBJECT" --force-stdin -q
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
        --consumer finance-connector \
        --workers 2 \
        --parser yaml \
        --parser-config "$CONFIG_FILE"
}

case "${1:-}" in
    create) create ;;
    load) load ;;
    run) run ;;
    *) usage ;;
esac
