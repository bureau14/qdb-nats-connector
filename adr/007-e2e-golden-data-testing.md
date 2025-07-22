# ADR 007: End-to-End Testing with Golden Data

## Status

**Proposed** - 2025-01-22

## Context

The NATS to QuasarDB connector requires comprehensive end-to-end testing to ensure data flows correctly through the entire pipeline. Current examples demonstrate functionality but lack automated validation. We need a testing approach that:

1. **Validates real pipeline execution**: Uses actual binaries and services
2. **Handles large datasets**: Tests with 1M+ records to catch scale issues
3. **Ensures deep correctness**: Validates data transformations are accurate
4. **Runs in CI**: Completes within ~15 minutes per commit
5. **Doubles as documentation**: Examples remain executable by users

### Current State

The `examples/` directory contains three scenarios with shell scripts that:
- Generate data inline (slow: ~10s for 1000 rows)
- Configure YAML parsers with various transformations
- Load data through NATS → Connector → QuasarDB pipeline
- Lack automated validation of output correctness

## Decision

Implement a **golden data testing approach** using pre-generated input/output datasets with simple `diff`-based validation.

### Architecture Overview

```
+-----------------------------------------------------+
|              Golden Data Test Flow                  |
+-----------------------------------------------------+
|                                                     |
|  1. Pre-generated Test Data (S3)                    |
|     +---------------------+                         |
|     | finance-ohlc.tar.gz |                         |
|     +---------------------+                         |
|     | • input.jsonl.gz    |                         |
|     | • nasdaq_AAPL.csv   |                         |
|     | • nasdaq_GOOGL.csv  |                         |
|     | • ...               |                         |
|     +---------------------+                         |
|                v                                    |
|  2. Test Execution                                  |
|     +---------------------+                         |
|     | Load > NATS > Conn  |                         |
|     |         v           |                         |
|     |     QuasarDB        |                         |
|     +---------------------+                         |
|                v                                    |
|  3. Export & Compare                                |
|     +---------------------+                         |
|     | qdb_export > CSV    |                         |
|     |         v           |                         |
|     | diff expected actual|                         |
|     +---------------------+                         |
|                                                     |
+-----------------------------------------------------+
```

### Test Data Structure

```
examples/
├── finance-ohlc.sh              # Enhanced with test mode
├── finance-ohlc.yaml            # Parser config (unchanged)
├── finance-ohlc-data.jsonl      # Small demo data (unchanged)
└── testdata/
    └── .gitignore               # Ignore downloaded packages

S3 Structure:
https://public.quasar.ai/test-data/
├── finance-ohlc-2025-01-22.tar.gz
│   ├── input.jsonl.gz           # 1M records
│   ├── nasdaq_AAPL.csv.gz       # Expected output
│   ├── nasdaq_GOOGL.csv.gz      # Expected output
│   └── ...
├── industrial-sensor-2025-01-22.tar.gz
└── network-metrics-2025-01-22.tar.gz
```

### Implementation

#### Enhanced Example Script

(Example)

```bash
#!/bin/bash
# finance-ohlc.sh - Works for both demo and test modes

# Test data URL - explicit version for reproducibility
TEST_DATA_URL="https://data.quasardb.net/test-data/finance-ohlc-2025-01-22.tar.gz"

if [ "$1" = "test" ]; then
    # Download test data if not cached
    mkdir -p testdata
    if [ ! -f "testdata/finance-ohlc-2025-01-22.tar.gz" ]; then
        echo "Downloading test data..."
        curl -L "$TEST_DATA_URL" -o testdata/finance-ohlc-2025-01-22.tar.gz
    fi

    # Extract test data
    cd testdata && tar -xzf finance-ohlc-2025-01-22.tar.gz && cd ..

    # Load 1M records into NATS
    zcat testdata/input.jsonl.gz | \
        direnv exec . nats pub finance.ohlc

    # Run connector (same as demo mode)
    direnv exec . qdb-nats-connector \
        --stream finance-stream \
        --topic "finance.*" \
        --parser yaml \
        --parser-config finance-ohlc.yaml

    # Export and validate each table
    for symbol in AAPL GOOGL MSFT AMZN TSLA; do
        # Export current data
        qdb_export -c qdb://127.0.0.1:2836 \
            -t "nasdaq_$symbol" \
            -o "/tmp/nasdaq_$symbol.csv"

        # Compare with expected output
        zcat "testdata/nasdaq_$symbol.csv.gz" > "/tmp/expected_$symbol.csv"

        if ! diff -q "/tmp/expected_$symbol.csv" "/tmp/nasdaq_$symbol.csv"; then
            echo "ERROR: Output mismatch for nasdaq_$symbol"
            exit 1
        fi
    done

    echo "✓ All validations passed"
else
    # Original demo mode - generate small dataset
    generate_ohlc_data | head -n 1000 > finance-ohlc-data.jsonl
    # ... rest of demo logic
fi
```

#### CI Integration
```bash
#!/bin/bash
# scripts/teamcity/40.examples.sh

set -e

# Run all examples in test mode
for example in finance-ohlc industrial-sensor network-metrics; do
    echo "Testing $example..."
    cd examples
    ./${example}.sh test
    cd ..
done

echo "All example tests passed!"
```

### Golden Data Generation Process

One-time process for creating test datasets:

```
+----------------+     +----------------+     +----------------+
| Generate 1M+   |---->| Run Through    |---->| Export with    |
| Records        |     | Full Pipeline  |     | qdb_export     |
+----------------+     +----------------+     +----------------+
         |                       |                       |
         v                       v                       v
   input.jsonl.gz         Stored in QDB         nasdaq_*.csv.gz
                                                         |
                          +------------------------------+
                          v
                    Package & Upload
                 finance-ohlc.tar.gz
```

## Consequences

### Positive

- **Simplicity**: Just `curl`, `diff`, and `qdb_export` - no custom validation code
- **Transparency**: CSV files are human-readable for debugging
- **Speed**: Downloads are cached, `qdb_export` handles millions of rows/sec
- **Reproducibility**: Fixed URLs ensure consistent test results
- **User Access**: Examples remain executable standalone

### Negative

- **Storage**: Requires hosting test data packages (~500MB total compressed)
- **Maintenance**: New golden data needed when parser logic changes
- **Network Dependency**: Initial download requires internet access

### Neutral

- **Version Management**: URLs include dates for explicit versioning
- **Binary Format**: CSV format locks in `qdb_export` as dependency

## Alternatives Considered

### Complex Validation Framework
Build Go/Python tools for deep data validation.
- **Rejected**: Adds unnecessary complexity when `diff` suffices

### Property-Based Testing
Use `rapid` for generating test data on-the-fly.
- **Rejected**: Better suited for unit/integration tests, not E2E validation

### Test Containers
Containerize test environments with pre-loaded data.
- **Rejected**: Adds complexity without proportional benefit for this use case

---

*This ADR documents the decision to use golden data testing for end-to-end validation of the NATS to QuasarDB connector pipeline.*
