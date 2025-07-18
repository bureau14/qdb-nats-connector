#!/bin/bash
# Network device metrics monitoring example
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_FILE="$SCRIPT_DIR/network-metrics-data.jsonl"
CONFIG_FILE="$SCRIPT_DIR/network-metrics.yaml"
STREAM_NAME="NETWORK_STREAM"
SUBJECT="network.metrics"
NUM_MESSAGES=1000

usage() {
    echo "Usage: $0 {create|load|run}"
    echo "  create  - Create NATS JetStream stream and QuasarDB table"
    echo "  load    - Generate and load data into NATS JetStream"
    echo "  run     - Run qdb-nats-connector and show all data"
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
        # Generate random nanoseconds for high-precision timestamps
        nanos=$(awk "BEGIN {printf \"%09d\", rand() * 1000000000}")
        nano_timestamp=$(date -u -r $timestamp "+%Y-%m-%dT%H:%M:%S.${nanos}Z")
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
        # Map devices to datacenter and rack
        case "$device_id" in
            "router-01"|"switch-01") datacenter="DC1"; rack="R42" ;;
            "router-02"|"switch-02") datacenter="DC1"; rack="R43" ;;
            "firewall-01") datacenter="DC2"; rack="R01" ;;
            *) datacenter="DC1"; rack="R44" ;;
        esac
        # Randomly omit errors section 20% of the time for testing error handling
        missing_field_chance=$(awk "BEGIN {srand(); printf \"%.0f\", rand() * 100}")
        if [ "$missing_field_chance" -lt 20 ]; then
            # Omit errors section entirely
            echo "{\"device\": {\"info\": {\"id\": \"$device_id\"}}, \"datacenter\": \"$datacenter\", \"rack\": \"$rack\", \"timestamp\": \"$nano_timestamp\", \"metrics\": {\"network\": {\"inbound\": {\"bytes\": $bytes_in}, \"outbound\": {\"bytes\": $bytes_out}}, \"performance\": {\"latency_percentiles\": {\"p99\": $latency_ms}}}}" >> "$DATA_FILE"
        else
            # Include errors section with deeply nested structure
            echo "{\"device\": {\"info\": {\"id\": \"$device_id\"}}, \"datacenter\": \"$datacenter\", \"rack\": \"$rack\", \"timestamp\": \"$nano_timestamp\", \"metrics\": {\"network\": {\"inbound\": {\"bytes\": $bytes_in}, \"outbound\": {\"bytes\": $bytes_out}}, \"performance\": {\"latency_percentiles\": {\"p99\": $latency_ms}}}, \"errors\": {\"packets\": {\"dropped\": $packets_dropped}}}" >> "$DATA_FILE"
        fi
    done
    echo "Generated $NUM_MESSAGES network metrics messages in $DATA_FILE"
}

create() {
    echo "Creating NATS JetStream stream and QuasarDB table..."

    # Create NATS stream
    echo "Creating NATS JetStream stream: $STREAM_NAME"
    if nats stream info "$STREAM_NAME" >/dev/null 2>&1; then
        echo "Warning: Stream $STREAM_NAME already exists, skipping NATS stream creation."
    else
        nats stream add "$STREAM_NAME" --subjects "network.>" --retention limits --defaults
        echo "Stream $STREAM_NAME created successfully."
    fi

    # Create consumer for the connector
    echo "Creating NATS consumer: network-connector"
    if nats consumer info "$STREAM_NAME" network-connector >/dev/null 2>&1; then
        echo "Warning: Consumer network-connector already exists, skipping consumer creation."
    else
        nats consumer add "$STREAM_NAME" network-connector --pull --deliver all --ack explicit --defaults
        echo "Consumer network-connector created successfully."
    fi

    # Create QuasarDB table
    echo "Creating QuasarDB table: network_metrics"
    qdbsh -c "CREATE TABLE network_metrics(device_id STRING, bytes_in INT64, bytes_out INT64, packets_dropped INT64, latency_ms DOUBLE, device_path STRING)" 2>&1 | grep -v "already exists" || true
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
        --consumer network-connector \
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
