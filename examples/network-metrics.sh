#!/bin/bash
# Network device metrics monitoring example
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_FILE="$SCRIPT_DIR/network-metrics-data.jsonl"
CONFIG_FILE="$SCRIPT_DIR/network-metrics.yaml"
STREAM_NAME="NETWORK_STREAM"
SUBJECT="network.metrics"
NUM_MESSAGES=100

usage() {
    echo "Usage: $0 {create-stream|load|run}"
    echo "  create-stream  - Create NATS JetStream stream"
    echo "  load          - Generate and load data into NATS JetStream"
    echo "  run           - Run qdb-nats-connector and show all data"
    exit 1
}

generate() {
    echo "Generating $NUM_MESSAGES network metrics messages..."
    START_TIMESTAMP=1736931600  # 2025-01-16T10:00:00Z
    declare -a DEVICES=("router-01" "router-02" "switch-01" "switch-02" "firewall-01")
    # Device type multipliers: [router, switch, firewall] for [bytes_base, drop_max, latency_base]
    declare -a ROUTER_PARAMS=(1000000 20 0.5)
    declare -a SWITCH_PARAMS=(500000 10 0.2)
    declare -a FIREWALL_PARAMS=(2000000 100 1.0)
    cat > "$DATA_FILE" << 'EOF'
EOF
    for ((i=0; i<NUM_MESSAGES; i++)); do
        timestamp=$((START_TIMESTAMP + i * 30))
        iso_timestamp=$(date -u -r $timestamp "+%Y-%m-%dT%H:%M:%SZ")
        device_idx=$((i % 5))
        device_id="${DEVICES[$device_idx]}"
        # Set device-specific parameters
        if [[ "$device_id" =~ ^router ]]; then
            bytes_base=${ROUTER_PARAMS[0]}; drop_max=${ROUTER_PARAMS[1]}; lat_base=${ROUTER_PARAMS[2]}
            bytes_range=50000000; lat_range=5.0
        elif [[ "$device_id" =~ ^switch ]]; then
            bytes_base=${SWITCH_PARAMS[0]}; drop_max=${SWITCH_PARAMS[1]}; lat_base=${SWITCH_PARAMS[2]}
            bytes_range=25000000; lat_range=2.0
        else  # firewall
            bytes_base=${FIREWALL_PARAMS[0]}; drop_max=${FIREWALL_PARAMS[1]}; lat_base=${FIREWALL_PARAMS[2]}
            bytes_range=30000000; lat_range=10.0
        fi
        bytes_in=$(awk "BEGIN {printf \"%.0f\", $bytes_base + rand() * $bytes_range}")
        bytes_out=$(awk "BEGIN {printf \"%.0f\", $bytes_base * 0.8 + rand() * $bytes_range * 0.8}")
        packets_dropped=$(awk "BEGIN {printf \"%.0f\", rand() * $drop_max}")
        latency_ms=$(awk "BEGIN {printf \"%.1f\", $lat_base + rand() * $lat_range}")
        echo "{\"timestamp\": \"$iso_timestamp\", \"device_id\": \"$device_id\", \"bytes_in\": $bytes_in, \"bytes_out\": $bytes_out, \"packets_dropped\": $packets_dropped, \"latency_ms\": $latency_ms}" >> "$DATA_FILE"
    done
    echo "Generated $NUM_MESSAGES network metrics messages in $DATA_FILE"
}

create_stream() {
    echo "Creating NATS JetStream stream: $STREAM_NAME"
    if nats stream info "$STREAM_NAME" >/dev/null 2>&1; then
        echo "Stream $STREAM_NAME already exists, deleting..."
        nats stream delete "$STREAM_NAME" --force
    fi
    nats stream add "$STREAM_NAME" --subjects "network.>" --retention limits --defaults
    echo "Stream $STREAM_NAME created successfully."
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
        echo "Error: Stream $STREAM_NAME does not exist. Run '$0 create-stream' first."
        exit 1
    fi
    
    lines_count=$(wc -l < "$DATA_FILE")
    echo "Publishing $lines_count network metrics messages..."
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
    create-stream) create_stream ;;
    load) load ;;
    run) run ;;
    *) usage ;;
esac