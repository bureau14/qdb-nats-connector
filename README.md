# qdb-nats-connector

A high-performance NATS JetStream to QuasarDB connector with pull-based message consumption, automatic batching, and error recovery.

## Overview

The qdb-nats-connector subscribes to NATS JetStream subjects and writes the received messages to QuasarDB timeseries tables. It uses a pull-based consumer model for better flow control and exactly-once message processing semantics.

> **Migration Note**: Version 2.0 introduces breaking changes with the move to JetStream pull consumers. See the [Migration Guide](docs/MIGRATION.md) for upgrading from v1.

## Features

- **Pull-based JetStream consumption** with configurable batch sizes
- **Worker-based processing** - one worker per topic filter for parallel processing
- **Automatic consumer management** with sequence tracking and recovery
- **Selective acknowledgment** for efficient error handling
- **Circuit breaker pattern** to prevent cascade failures
- **JSON parser** with required `$table` field routing
- **Configurable batching** for optimal performance
- **Graceful shutdown** with proper connection draining
- **Persistent progress tracking** across restarts
- **TTL-based error tracking** with automatic cleanup

## Architecture

The connector uses a three-phase processing pipeline:

1. **Fetch Phase**: Pull messages from JetStream in configurable batches
2. **Parse Phase**: Transform messages to QuasarDB tables (currently JSON only)
3. **Write Phase**: Batch write to QuasarDB with selective ACK/NACK

### Components

- **Source**: JetStream pull consumer with circuit breaker and sequence tracking
- **Parser**: Pluggable message transformation (JSON parser included)
- **Sink**: QuasarDB batch writer with connection pooling
- **Worker**: Orchestrates the pipeline for a single topic filter
- **Connector**: Manages multiple workers and graceful shutdown

## Quick Start

### Prerequisites

1. NATS Server with JetStream enabled:
   ```bash
   # Start NATS with JetStream
   nats-server -js
   ```

2. Create a JetStream stream:
   ```bash
   nats stream create EVENTS \
     --subjects "sensors.>,metrics.>" \
     --retention limits \
     --max-age 7d
   ```

3. QuasarDB cluster running

### Installation

```bash
go build -o qdb-nats-connector
```

## Configuration

The connector supports configuration through:
1. Configuration file (YAML)
2. Environment variables (prefix: `QDB_NATS_`)
3. Command-line flags

Precedence: CLI flags > Environment variables > Config file > Defaults

### Command-line Options

```bash
# NATS JetStream options
--nats <url>                    # NATS endpoint (default: nats://127.0.0.1:4222)
--stream <name>                 # JetStream stream name (required)
--topic <filter>                # Topic filter, can be repeated (required)
--consumer-prefix <prefix>      # Consumer name prefix (default: qdb-connector)
--batch-size <size>             # Messages per fetch (default: 100)
--batch-timeout <duration>      # Max wait for batch (default: 1s)
--fetch-timeout <duration>      # Total fetch timeout (default: 5s)
--ack-wait <duration>          # Message ACK timeout (default: 30s)
--max-deliver <count>          # Max delivery attempts (default: 3)
--max-retries <count>          # Poison message threshold (default: 3)

# QuasarDB options
--qdb <uri>                    # QuasarDB endpoint (required)
--qdb-pubkey-file <path>       # Cluster public key file
--qdb-user-sec-file <path>     # User security file
--qdb-compression <mode>       # Compression: none|fast|balanced
--qdb-encryption <mode>        # Encryption: none|aes
--qdb-push-mode <mode>         # Push mode: transactional|async|fast
--qdb-client-max-parallelism <n> # Max parallel operations
--qdb-client-inbuf-size <size>   # Input buffer size

# Error handling options
--error-ttl <duration>         # Error tracking TTL (default: 1h)

# Other options
--config <path>                # Configuration file path
--pid <path>                   # PID file path
--help                         # Show help message
```

### Configuration File Example

```yaml
nats:
  endpoint: nats://localhost:4222
  stream: DATA_STREAM
  topics:
    - sensors.>
    - metrics.>
  consumer_prefix: qdb-connector
  batch_size: 100
  batch_timeout: 1s
  fetch_timeout: 5s
  ack_wait: 30s
  max_deliver: 3

qdb:
  cluster_uri: qdb://localhost:2836
  compression: none
  encryption: none
  push_mode: async

max_retries: 3
error_ttl: 1h
```

### Environment Variables

All configuration options can be set via environment variables with the `QDB_NATS_` prefix:

```bash
export QDB_NATS_NATS_ENDPOINT=nats://localhost:4222
export QDB_NATS_NATS_STREAM=DATA_STREAM
export QDB_NATS_QDB_CLUSTER_URI=qdb://localhost:2836
```

## Message Format

Messages must be valid JSON with the following structure:

```json
{
  "$table": "sensor_data",           // Required: target table name
  "$timestamp": "2024-01-01T12:00:00.000000000Z", // Optional: RFC3339 timestamp
  "temperature": 23.5,               // Numeric fields → Double columns
  "location": "room1",               // String fields → Blob columns
  "active": true                     // Boolean fields → Blob columns ("true"/"false")
}
```

### Type Mapping

- **Numbers** (int, float64) → QuasarDB Double columns
- **Strings** → QuasarDB Blob columns (UTF-8 bytes)
- **Booleans** → QuasarDB Blob columns ("true" or "false")
- **Arrays/Objects** → Not supported (parsing error)
- **Null values** → Skipped

## Usage Example

```bash
# Start connector with single topic
./qdb-nats-connector \
  --nats nats://localhost:4222 \
  --stream DATA_STREAM \
  --topic "sensors.>" \
  --qdb qdb://localhost:2836

# Start with multiple topics and configuration
./qdb-nats-connector \
  --config /etc/qdb-nats/config.yaml \
  --topic "sensors.>" \
  --topic "metrics.>" \
  --topic "logs.>"

# With security
./qdb-nats-connector \
  --nats nats://localhost:4222 \
  --stream SECURE_STREAM \
  --topic "data.>" \
  --qdb qdb://localhost:2836 \
  --qdb-pubkey-file /path/to/cluster_public.key \
  --qdb-user-sec-file /path/to/user_private.key \
  --qdb-encryption aes
```

## JetStream Setup

### Creating a Stream

```bash
# Create a stream for your data
nats stream create DATA_STREAM \
  --subjects "sensors.>,metrics.>,logs.>" \
  --retention limits \
  --max-age 30d \
  --max-bytes 1TB \
  --storage file \
  --replicas 3
```

### Stream Configuration Options

- **Retention Policy**: `limits` (default), `interest`, or `workqueue`
- **Max Age**: How long to keep messages (e.g., `7d`, `24h`)
- **Max Bytes**: Maximum storage size
- **Storage**: `file` (persistent) or `memory` (faster but not durable)
- **Replicas**: Number of copies for high availability

## Consumer Management

The connector creates durable pull consumers with automatic recovery:

- **Consumer naming**: `<prefix>-<topic-hash>` (e.g., `qdb-connector-a1b2c3d4`)
- **Sequence tracking**: Persisted to `.sequences/` directory
- **Automatic recreation**: On configuration mismatch or missing consumer
- **Delivery semantics**: At-least-once with selective acknowledgment
- **Consumer per topic**: Each topic filter gets its own consumer for isolation

### Monitoring Consumers

```bash
# List all consumers
nats consumer list DATA_STREAM

# Check consumer info
nats consumer info DATA_STREAM qdb-connector-12345678

# View pending messages
nats consumer report DATA_STREAM
```

## Error Handling

- **Parse errors**: Messages are NACK'd for redelivery up to `max-retries`
- **Write errors**: Entire batch is NACK'd for retry
- **Poison messages**: After `max-retries`, messages are ACK'd to prevent blocking
- **Circuit breaker**: Prevents repeated connection attempts during outages
- **Worker health monitoring**: Automatic detection of stalled workers

## Performance Tuning

### Batch Processing
- `--batch-size`: Larger batches improve throughput but increase latency
- `--batch-timeout`: Balance between latency and batch efficiency
- `--fetch-timeout`: Should be larger than batch-timeout

### QuasarDB Performance
- `--qdb-push-mode async`: Best throughput (default)
- `--qdb-push-mode fast`: Lower latency, less reliability
- `--qdb-push-mode transactional`: Highest reliability, lower throughput
- `--qdb-client-max-parallelism`: Increase for better concurrency

### JetStream Tuning
- `--ack-wait`: Set based on expected processing time
- `--max-deliver`: Balance between retry attempts and poison message handling

## Monitoring

The connector logs key events:
- Worker startup and shutdown
- Message processing statistics
- Parse and write errors
- Consumer recreation events
- Circuit breaker state changes

## Troubleshooting

### Common Issues

1. **Consumer already exists error**
   ```bash
   # Delete existing consumer
   nats consumer rm DATA_STREAM old-consumer-name
   ```

2. **Messages not being processed**
   ```bash
   # Check consumer status
   nats consumer info DATA_STREAM qdb-connector-12345678
   
   # Look for "Pending Messages" and "Redelivered Messages"
   ```

3. **High redelivery rate**
   - Increase `--ack-wait` if processing is slow
   - Check logs for parse errors
   - Verify QuasarDB connectivity

4. **Memory usage growing**
   - Reduce `--batch-size`
   - Lower `--error-ttl` to clean up error tracking faster

## Documentation

- [Architecture Guide](docs/ARCHITECTURE.md) - Internal design and components
- [API Reference](docs/API.md) - Parser and component interfaces
- [Configuration Guide](docs/CONFIGURATION.md) - Detailed configuration options
- [Migration Guide](docs/MIGRATION.md) - Upgrading from v1 to v2

## Development

### Project Structure

```
main.go                 # Entry point and CLI
connector/
  connector.go         # Main orchestrator
  options.go          # Configuration handling
  worker.go           # Worker implementation
internal/
  source/
    source.go         # JetStream pull consumer
    options.go        # Source configuration
  parser/
    parser.go         # Parser interface
    json.go           # JSON parser implementation
  sink/
    sink.go           # QuasarDB writer
    options.go        # Sink configuration
  errors/
    errors.go         # Custom error types
docs/
  ARCHITECTURE.md      # Architecture documentation
  API.md              # API reference
  CONFIGURATION.md    # Configuration guide
  MIGRATION.md        # Migration guide
```

### Building

```bash
# Development build
direnv exec . go build

# Run tests
direnv exec . go test ./...

# Vendor dependencies
direnv exec . go mod vendor
```

## License

Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
