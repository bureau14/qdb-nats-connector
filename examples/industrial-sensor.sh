#!/bin/bash
# Industrial sensor monitoring example
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_FILE="$SCRIPT_DIR/industrial-sensor-data.jsonl"
CONFIG_FILE="$SCRIPT_DIR/industrial-sensor.yaml"
STREAM_NAME="INDUSTRIAL_STREAM"
SUBJECT="industrial.temperature"
NUM_MESSAGES=1000

usage() {
    echo "Usage: $0 {create|load|run}"
    echo "  create  - Create NATS JetStream stream and QuasarDB table"
    echo "  load    - Generate and load data into NATS JetStream"
    echo "  run     - Run qdb-nats-connector and show all data"
    exit 1
}

generate() {
    echo "Generating $NUM_MESSAGES industrial sensor messages..."
    START_TIMESTAMP=1736928000  # 2025-01-16T09:00:00Z
    cat > "$DATA_FILE" << 'EOF'
EOF
    for ((i=0; i<NUM_MESSAGES; i++)); do
        timestamp=$((START_TIMESTAMP + i * 10))
        custom_timestamp=$(date -u -r $timestamp "+%Y-%m-%d %H:%M:%S")
        sensor_num=$((i % 5 + 1))
        sensor_id="sensor-$(printf "%02d" $sensor_num)"
        # Generate temperature readings with 10% error rate for testing safe_parse_number
        error_chance=$(awk "BEGIN {srand(); printf \"%.0f\", rand() * 100}")
        if [ "$error_chance" -lt 10 ]; then
            # Generate error values
            case $((error_chance % 3)) in
                0) temp="ERROR" ;;
                1) temp="N/A" ;;
                2) temp="SENSOR_FAULT" ;;
            esac
        else
            temp=$(awk "BEGIN {srand(); printf \"%.1f\", 15 + rand() * 20}")  # 15-35°C range
        fi
        # Map sensors to locations for realistic data
        case "$sensor_num" in
            1) location="Floor1"; building="B1"; floor="1" ;;
            2) location="Floor2"; building="B1"; floor="2" ;;
            3) location="Floor3"; building="B1"; floor="3" ;;
            4) location="Basement"; building="B2"; floor="B" ;;
            5) location="Rooftop"; building="B2"; floor="R" ;;
            *) location="Unknown"; building="B0"; floor="0" ;;
        esac
        echo "{\"sensor\": {\"id\": \"$sensor_id\", \"location\": \"$location\"}, \"building\": \"$building\", \"floor\": \"$floor\", \"timestamp\": \"$custom_timestamp\", \"temp\": \"$temp\"}" >> "$DATA_FILE"
    done
    echo "Generated $NUM_MESSAGES messages in $DATA_FILE"
}

create() {
    echo "Creating NATS JetStream stream and QuasarDB table..."

    # Create NATS stream
    echo "Creating NATS JetStream stream: $STREAM_NAME"
    if nats stream info "$STREAM_NAME" >/dev/null 2>&1; then
        echo "Warning: Stream $STREAM_NAME already exists, skipping NATS stream creation."
    else
        nats stream add "$STREAM_NAME" --subjects "industrial.>" --retention limits --defaults
        echo "Stream $STREAM_NAME created successfully."
    fi

    # Create consumer for the connector
    echo "Creating NATS consumer: industrial-connector"
    if nats consumer info "$STREAM_NAME" industrial-connector >/dev/null 2>&1; then
        echo "Warning: Consumer industrial-connector already exists, skipping consumer creation."
    else
        nats consumer add "$STREAM_NAME" industrial-connector --pull --deliver all --ack explicit --defaults
        echo "Consumer industrial-connector created successfully."
    fi

    # Create QuasarDB table
    echo "Creating QuasarDB table: industrial_sensors"
    qdbsh -c "CREATE TABLE industrial_sensors(sensor_id STRING, measurement DOUBLE, sensor_tag STRING)" 2>&1 | grep -v "already exists" || true
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
    echo "Publishing $lines_count messages..."
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
        --consumer industrial-connector \
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
