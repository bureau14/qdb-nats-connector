# qdb-nats-connector

A high-performance NATS JetStream to QuasarDB connector with pull-based message consumption, automatic batching, and error recovery.

## Overview

The qdb-nats-connector subscribes to NATS JetStream subjects and writes the received messages to QuasarDB timeseries tables. It uses a pull-based consumer model for better flow control and exactly-once message processing semantics.

> **Migration Note**: Version 2.0 introduces breaking changes with the move to JetStream pull consumers. See the [Migration Guide](docs/MIGRATION.md) for upgrading from v1.

## Features

- **Pull-based JetStream consumption** with configurable batch sizes
- **Worker-based processing** - worker pool for parallel processing
- **Automatic consumer management** with sequence tracking and recovery
- **Selective acknowledgment** for efficient error handling
- **Circuit breaker pattern** to prevent cascade failures
- **Configurable batching** for optimal performance
- **Graceful shutdown** with proper connection draining
- **Persistent progress tracking** across restarts
- **TTL-based error tracking** with automatic cleanup

## Architecture

The connector uses a three-phase processing pipeline:

1. **Fetch Phase**: Pull messages from JetStream in configurable batches
2. **Parse Phase**: Transform messages to QuasarDB tables (YAML and noop parsers)
3. **Write Phase**: Batch write to QuasarDB with selective ACK/NACK

### Components

- **Source**: JetStream pull consumer with circuit breaker and sequence tracking
- **Parser**: Pluggable message transformation (YAML and noop parsers included)
- **Sink**: QuasarDB batch writer with connection pooling
- **Worker**: Processes messages from the shared work queue
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
make build
```

## Configuration

The connector supports configuration through:

1. Environment variables (prefix: `QDB_NATS_`)
2. Command-line flags

Precedence: CLI flags > Environment variables > Defaults

### Command-line Options

```bash
# NATS JetStream options
--nats <url>                    # NATS endpoint (default: nats://127.0.0.1:4222)
--stream <name>                 # JetStream stream name (required)
--consumer <name>               # Consumer name (default: qdb-connector)
--workers <count>               # Number of concurrent workers (default: 1)
--batch-size <size>             # Messages per fetch (default: 100)
--batch-timeout <duration>      # Max wait for batch (default: 1s)
--fetch-timeout <duration>      # Total fetch timeout (default: 5s)
--max-retries <count>          # Poison message threshold (default: 3)

# QuasarDB options
--qdb <uri>                    # QuasarDB endpoint (required)
--qdb-pubkey-file <path>       # Cluster public key file
--qdb-user-sec-file <path>     # User security file
--qdb-compression <mode>       # Compression: none|balanced
--qdb-encryption <mode>        # Encryption: none|aes
--qdb-push-mode <mode>         # Push mode: transactional|async|fast
--qdb-client-max-parallelism <n> # Max parallel operations
--qdb-client-inbuf-size <size>   # Input buffer size

# Parser options
--parser <type>                # Parser type: yaml|noop (default: noop)
--parser-config <path>         # YAML parser configuration file

# Error handling options
--error-ttl <duration>         # Error tracking TTL (default: 1h)

# Other options
--pid <path>                   # PID file path
--help                         # Show help message
```

### Environment Variables

All configuration options can be set via environment variables with the `QDB_NATS_` prefix:

```bash
export QDB_NATS_NATS_ENDPOINT=nats://localhost:4222
export QDB_NATS_NATS_STREAM=DATA_STREAM
export QDB_NATS_QDB_CLUSTER_URI=qdb://localhost:2836
```

## Parsers

### Noop Parser (Default)

The noop parser is a pass-through parser that accepts any message format and performs no transformation. Messages are validated but returned as empty tables. This parser is useful for testing and scenarios where message processing is handled elsewhere.

### YAML Parser

The YAML parser provides a flexible, high-performance alternative with <5% overhead vs hardcoded parsers. It uses a building-block architecture for declarative message transformation.

```bash
# Use YAML parser
./qdb-nats-connector \
  --parser yaml \
  --parser-config /path/to/parser.yaml \
  --nats nats://localhost:4222 \
  --stream DATA_STREAM \
  --qdb qdb://localhost:2836
```

Key features:

- **Building blocks**: Pre-compiled transformation functions (decompress, parse_json, extract_field, extract_index, extract_timestamp, etc.)
- **Column types**: timestamp, double, int64, blob, string, symbol
- **Timestamp formats**: rfc3339, rfc3339nano, unix, unix_ms, unix_us, unix_ns, or custom Go layouts -- for both the index and timestamp value columns
- **Parallel processing**: Optional worker pools for increased throughput
- **Error handling**: Configurable drop/fail modes
- **Type safety**: Automatic conversions with overflow protection
- **Performance**: Object pooling and zero-allocation design

See [YAML Parser Documentation](docs/YAML_PARSER.md) for complete configuration guide and examples.

## Usage Example

```bash
# Start connector with default worker
./qdb-nats-connector \
  --nats nats://localhost:4222 \
  --stream DATA_STREAM \
  --consumer qdb-connector \
  --qdb qdb://localhost:2836

# Start with multiple workers
./qdb-nats-connector \
  --nats nats://localhost:4222 \
  --stream DATA_STREAM \
  --consumer qdb-connector \
  --workers 4 \
  --qdb qdb://localhost:2836

# With security
./qdb-nats-connector \
  --nats nats://localhost:4222 \
  --stream SECURE_STREAM \
  --consumer secure-connector \
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

- **Consumer naming**: Uses the specified consumer name (e.g., `qdb-connector`)
- **Sequence tracking**: Persisted to `.sequences/` directory
- **Automatic recreation**: On configuration mismatch or missing consumer
- **Delivery semantics**: At-least-once with selective acknowledgment
- **Single shared consumer**: All workers pull from the same consumer for better load distribution

### Monitoring Consumers

```bash
# List all consumers
nats consumer list DATA_STREAM

# Check consumer info
nats consumer info DATA_STREAM qdb-connector

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

Redelivery policy (AckWait, MaxDeliver) is owned by the operator-created
durable consumer, not the connector -- configure it when creating the
durable (e.g. `nats consumer add --ack-wait 30s --max-deliver 3 ...`):

- AckWait: set based on expected processing time
- MaxDeliver: balance between retry attempts and poison message handling

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
   - Increase the durable's AckWait if processing is slow
   - Check logs for parse errors
   - Verify QuasarDB connectivity

4. **Memory usage growing**
   - Reduce `--batch-size`
   - Lower `--error-ttl` to clean up error tracking faster

## Documentation

- [Architecture Guide](docs/ARCHITECTURE.md) - Internal design and components
- [API Reference](docs/API.md) - Parser and component interfaces
- [Migration Guide](docs/MIGRATION.md) - Upgrading from v1 to v2
- [YAML Parser Guide](docs/YAML_PARSER.md) - Comprehensive YAML parser documentation
- [YAML Parser Examples](examples/) - Example configurations for common scenarios

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
    yaml.go           # YAML parser implementation
    noop.go           # Noop parser implementation
  sink/
    sink.go           # QuasarDB writer
    options.go        # Sink configuration
  errors/
    errors.go         # Custom error types
docs/
  ARCHITECTURE.md      # Architecture documentation
  API.md              # API reference
  MIGRATION.md        # Migration guide
```

### Building

```bash
# Build all binaries (release mode with debug symbols)
direnv exec . make build

# Debug build (for debugging with gdb/dlv)
direnv exec . BUILD_MODE=debug make build

# Minimal release build (stripped binaries)
direnv exec . BUILD_MODE=release-min make build

# Build individual binary
direnv exec . BIN=connector make build-single

# Run tests
direnv exec . make test

# Run linter
direnv exec . make lint

# Clean build artifacts
direnv exec . make clean
```

## License

Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
