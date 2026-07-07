# NATS to QuasarDB Connector Examples

This directory contains example configurations and test scenarios demonstrating various use cases for the qdb-nats-connector.

## Available Examples

### 1. Finance OHLC Market Data (`finance-ohlc`)

- **Use Case**: Real-time financial market data ingestion
- **Features**: Dynamic table routing based on exchange/symbol, compressed data handling, symbol column (`stock_id`), timestamp value column (`event_time` via `extract_timestamp`)
- **Tables**: `finance.NASDAQ.AAPL`, `finance.NASDAQ.GOOGL`, etc.

### 2. Industrial Sensor Monitoring (`industrial-sensor`)

- **Use Case**: IoT sensor data collection with error handling
- **Features**: Safe number parsing for faulty sensors, building/floor based routing
- **Tables**: `industrial.B1.1`, `industrial.B1.2`, `industrial.B2.B`, etc.

### 3. Network Metrics Collection (`network-metrics`)

- **Use Case**: Network device monitoring and performance tracking
- **Features**: High-precision timestamps, deeply nested JSON parsing
- **Table**: `network_metrics` (single table, no routing)

## Quick Start

Each example can be run in demo mode:

```bash
# Run complete demo for an example
./finance-ohlc.sh demo
./industrial-sensor.sh demo
./network-metrics.sh demo
```

## Modular Actions

All examples support the same set of modular actions:

- `create` - Create NATS stream and QuasarDB tables
- `generate` - Generate test dataset
- `load` - Load data into NATS
- `run` - Start connector in background
- `wait` - Wait for processing completion
- `stop` - Stop connector gracefully
- `export` - Export data from QuasarDB
- `validate` - Compare with golden data
- `prepare-golden` - Prepare golden data package

## Golden Data Testing

The examples framework includes comprehensive golden data testing:

```bash
# Run quick tests (10000 messages)
make test-quick

# Run full tests (1000000 messages)
make test-full

# Generate golden data
make generate-golden EXAMPLE=finance-ohlc NUM_MESSAGES=10000
```

See [GOLDEN_DATA_GENERATION.md](GOLDEN_DATA_GENERATION.md) for detailed documentation.

## Environment Variables

All examples support these environment variables:

- `NUM_MESSAGES` - Number of messages to generate (default: 1000)
- `QDB_URI` - QuasarDB connection URI (default: qdb://127.0.0.1:2836)
- `NATS_URL` - NATS server URL (default: nats://localhost:4222)
- `DEBUG` - Enable debug logging (set to 1)
- `DATASETS_DIR` - Directory for dataset archives and extracted data

## Prerequisites

- Running NATS JetStream server
- Running QuasarDB cluster
- Built qdb-nats-connector binary
- Tools: nats, qdbsh

## Directory Structure

```
examples/
├── common.sh                    # Shared utilities
├── Makefile                     # Test orchestration
├── finance-ohlc.sh             # Finance example script
├── finance-ohlc.yaml           # Parser configuration
├── industrial-sensor.sh        # Industrial example script
├── industrial-sensor.yaml      # Parser configuration
├── network-metrics.sh          # Network example script
├── network-metrics.yaml        # Parser configuration
└── datasets/                  # Dataset archives and extracted data (gitignored)
    ├── finance-ohlc-10000/    # Per-(example, count) dataset directory
    │   ├── input.data         # Input data
    │   ├── expected/          # Golden CSVs (generic table-only names)
    │   └── metadata.json      # Per-dataset metadata
    └── *.tar.gz               # Dated archives (one per dataset)
```
