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

Implement a **golden data testing approach** using pre-generated input/output datasets with tolerance-aware comparison for floating-point values.

### Architecture Overview

```
+--------------------------------------------------------+
|               Golden Data Test Flow                    |
+--------------------------------------------------------+
|                                                        |
|  1. Pre-generated Test Data (S3)                       |
|     +-------------------------+                        |
|     | finance-ohlc.tar.gz     |                        |
|     +-------------------------+                        |
|     | • input.jsonl.gz        |                        |
|     | • nasdaq_AAPL.csv       |                        |
|     | • nasdaq_GOOGL.csv      |                        |
|     | • ...                   |                        |
|     +-------------------------+                        |
|                v                                       |
|  2. Test Execution                                     |
|     +-------------------------+                        |
|     | Load > NATS > Conn      |                        |
|     |         v               |                        |
|     |     QuasarDB            |                        |
|     +-------------------------+                        |
|                v                                       |
|  3. Export & Compare                                   |
|     +-------------------------+                        |
|     | qdb_export > CSV        |                        |
|     |         v               |                        |
|     | numdiff expected actual |                        |
|     +-------------------------+                        |
|                                                        |
+--------------------------------------------------------+
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
https://data.quasardb.net/test-data/
├── finance-ohlc-2025-01-22-10k.tar.gz    # Quick test data
│   ├── input.jsonl.gz                    # 10k records
│   └── expected/
│       ├── nasdaq_AAPL.csv.gz
│       ├── nasdaq_GOOGL.csv.gz
│       └── ...
├── finance-ohlc-2025-01-22-1m.tar.gz     # Full test data
│   ├── input.jsonl.gz                    # 1M records
│   └── expected/
│       ├── nasdaq_AAPL.csv.gz
│       ├── nasdaq_GOOGL.csv.gz
│       └── ...
├── industrial-sensor-2025-01-22-10k.tar.gz
├── industrial-sensor-2025-01-22-1m.tar.gz
├── network-metrics-2025-01-22-10k.tar.gz
└── network-metrics-2025-01-22-1m.tar.gz
```

### Key Design Decisions

#### Modular Script Architecture

We adopt a modular approach where shell scripts provide atomic, single-purpose functions:

1. **Separation of Concerns**: Scripts handle operations, Makefile handles orchestration
2. **Composability**: Individual actions can be combined into different workflows
3. **Debuggability**: Each step can be tested and debugged in isolation
4. **Reusability**: Same primitives serve development, CI, and golden data generation

Scripts are refactored from monolithic implementations into discrete, composable actions that can be invoked independently. This enables flexible workflow composition while maintaining the ability to debug individual steps in isolation.

#### Deterministic Completion Detection

We choose **row count verification** over idle-based detection for determining when the connector has finished processing:

1. **Determinism**: Row counts provide exact completion detection, eliminating race conditions
2. **Industry Alignment**: Follows patterns from Kafka Connect, Debezium, and Flink
3. **Performance**: Tests complete immediately when processing finishes, no arbitrary waits
4. **Diagnostic Value**: Failures clearly indicate progress ("expected 1M rows, processed 500k")

The alternative of waiting for "X seconds of silence" is explicitly rejected as it introduces non-deterministic behavior incompatible with reliable CI/CD environments.

#### Process Lifecycle Management

Each example script maintains its own process state through PID files:

1. **Isolation**: Each test manages its own connector instance independently
2. **Safety**: PID files prevent concurrent executions and orphaned processes
3. **Simplicity**: No centralized process manager or service discovery needed
4. **Fail-Fast**: Missing or stale PID files cause immediate, clear failures

#### Why No Canonicalization Script

We explicitly reject adding a data canonicalization layer because:

1. **QuasarDB guarantees deterministic output**: Data is always returned in consistent order within each table
2. **Timestamps are consistently formatted**: QuasarDB's export format is stable and predictable
3. **Adding canonicalization masks bugs**: If ordering or formatting changes unexpectedly, that's a bug we want to catch
4. **Simplicity over complexity**: Standard Unix tools can handle our needs without custom scripts

#### Floating-Point Comparison Strategy

The only legitimate concern is floating-point precision differences. We address this with `numdiff`:

- **Tool choice**: `numdiff` is a standard Unix utility designed for numeric comparisons with tolerance
- **Default tolerance**: 1e-9 relative error handles typical IEEE 754 rounding differences
- **No data modification**: We compare actual outputs rather than modifying test data
- **Transparency**: Developers can see exactly what precision issues exist

#### Makefile-Based Orchestration

Test execution is orchestrated through a Makefile that:

- Downloads and caches test data with versioned filenames
- Tracks extraction state to avoid redundant operations
- Composes atomic script actions into complete workflows
- Enables parallel execution with `make -j`
- Provides separate targets for quick (10k) and full (1M) tests

This design leverages Make's strengths:
- **Dependency management**: Downloads happen only when needed
- **Parallel execution**: `make -j` runs tests concurrently
- **Extraction tracking**: `.extracted` marker files prevent redundant extractions
- **Fast re-runs**: Extracted files are kept until explicit cleanup
- **Universal availability**: Make is present on all Unix systems
- **Simplicity**: No custom scripts or additional languages required

#### Why Make Over Modern Alternatives

While tools like DVC (Data Version Control) or Snakemake offer advanced features for data pipeline management, we chose Make because:

1. **Universal availability**: Every developer and CI system has Make installed
2. **Zero learning curve**: All Unix developers understand Makefiles
3. **No additional dependencies**: Avoids Python environments or specialized tools
4. **Sufficient for our needs**: Our test data workflow is simple enough for Make's capabilities
5. **Industry standard**: Proven reliable for decades of software builds

The trade-offs (verbose syntax, limited data awareness) are acceptable given our straightforward download → extract → test workflow.

### Implementation Philosophy

The implementation follows Unix philosophy principles:

1. **Single Responsibility**: Each component does one thing well
2. **Composability**: Simple tools combine into complex workflows
3. **Transparency**: Operations are observable and debuggable
4. **Fail-Fast**: Errors propagate immediately and visibly
5. **Idempotency**: Operations can be safely retried

#### Service Dependency Model

The testing framework assumes QuasarDB and NATS run as persistent background services:

1. **Separation of Concerns**: Test scripts only manage the connector lifecycle
2. **Environment Stability**: Services are expected to be available throughout test execution
3. **No Recovery Logic**: Service failures are infrastructure issues, not test concerns
4. **Simplified State**: Tests don't need to manage service startup/shutdown complexity

#### CI Integration

The modular design enables efficient CI integration:

- **Quick tests** (10k records) run on every commit for rapid feedback
- **Full tests** (1M records) run on nightly/release builds for comprehensive validation
- **Parallel execution** via `make -j` reduces overall test time
- **Granular failure reporting** identifies exactly which step failed
- **Cache-friendly** structure avoids redundant downloads and extractions

### Golden Data Generation Process

One-time process for creating test datasets:

```
+----------------+     +----------------+     +----------------+
| Generate Data  |---->| Run Through    |---->| Export with    |
| (10k/1M rows)  |     | Full Pipeline  |     | qdb_export     |
+----------------+     +----------------+     +----------------+
         |                       |                       |
         v                       v                       v
   input.jsonl.gz         Stored in QDB         expected/*.csv.gz
                                                         |
                          +------------------------------+
                          v
                    Package & Upload
            finance-ohlc-2025-01-22-10k.tar.gz
            finance-ohlc-2025-01-22-1m.tar.gz
```

Archives contain both input data and expected outputs in a structured format, with compression applied to minimize storage requirements while maintaining accessibility for debugging.

## Consequences

### Positive

- **Simplicity**: Standard Unix tools only (`make`, `curl`, `numdiff`, `qdb_export`) - no custom validation code
- **Transparency**: CSV files are human-readable for debugging
- **Speed**: Downloads are cached, extractions tracked with `.extracted` markers, `qdb_export` handles millions of rows/sec
- **Fast re-runs**: Extracted files persist between runs until explicit cleanup
- **Parallel execution**: Make enables concurrent test execution with `-j` flag
- **Reproducibility**: Dated filenames ensure version clarity
- **User Access**: Examples remain executable standalone
- **Two-tier testing**: Quick tests (10k rows) for rapid development feedback, full tests (1M rows) for comprehensive validation
- **Dependency management**: Make handles downloads, extraction, and execution dependencies automatically

### Negative

- **Storage**: Requires hosting test data packages (~500MB total compressed)
- **Maintenance**: New golden data needed when parser logic changes
- **Network Dependency**: Initial download requires internet access
- **Floating-point sensitivity**: Must ensure test data uses appropriate precision to avoid false positives
- **Log Format Coupling**: Row count detection depends on stable log output format

### Neutral

- **Version Management**: URLs include dates for explicit versioning
- **Binary Format**: CSV format locks in `qdb_export` as dependency

## Alternatives Considered

### Complex Validation Framework
Build Go/Python tools for deep data validation.
- **Rejected**: Adds unnecessary complexity when `numdiff` suffices for our needs

### Custom Canonicalization Script
Create scripts to normalize CSV output (sort rows, round floats, format timestamps).
- **Rejected**: QuasarDB already provides deterministic output; canonicalization would mask bugs rather than catch them

### Property-Based Testing
Use `rapid` for generating test data on-the-fly.
- **Rejected**: Better suited for unit/integration tests, not E2E validation

### Test Containers
Containerize test environments with pre-loaded data.
- **Rejected**: Adds complexity without proportional benefit for this use case

### Raw `diff` Without Numeric Tolerance
Use standard `diff` for all comparisons.
- **Rejected**: Would fail on legitimate floating-point precision differences

### Skip Floating-Point Columns
Use `diff --ignore-matching-lines` to skip float columns entirely.
- **Rejected**: Would miss actual bugs in floating-point calculations

### Idle-Based Completion Detection
Wait for "X seconds of no output" to determine completion.
- **Rejected**: Non-deterministic, causes flaky tests, wastes CI time, incompatible with concurrent workers

### Direct Database Querying
Query QuasarDB directly to verify row counts instead of parsing logs.
- **Rejected**: Adds complexity of database client in test scripts, though could be reconsidered as future enhancement

---

## Rationale for Standard Unix Tools

This design philosophy prioritizes:

1. **Minimal dependencies**: Every developer and CI system has `make`, `curl`, and standard diff utilities
2. **Transparency**: Shell scripts and Makefiles are immediately understandable
3. **Composability**: Unix tools work together naturally via pipes and files
4. **Longevity**: These tools have been stable for decades and will remain so
5. **Industry alignment**: This approach mirrors successful data infrastructure projects like ClickHouse and DuckDB

By avoiding custom frameworks or scripts, we ensure the testing infrastructure remains maintainable by any engineer familiar with Unix development practices.

---

*This ADR documents the decision to use golden data testing with Makefile orchestration and numeric-aware comparison for end-to-end validation of the NATS to QuasarDB connector pipeline.*
