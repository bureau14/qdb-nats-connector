# ADR 005: YAML-Based Parser Architecture with Building Blocks

## Status

**Accepted** - 2025-07-14

## Context

The NATS to QuasarDB connector requires a pluggable parser architecture to handle diverse data formats from multiple customers without including customer-specific code in the main binary. The system operates in a high-throughput streaming environment where parsing performance is critical—any parser architecture must minimize latency impact on the message processing pipeline.

### Requirements

1. **Performance**: Near-native performance in the hot path (target <5% overhead)
2. **Pluggability**: External parsers without recompiling main binary
3. **Customer-friendly**: Non-Go developers can create parsers
4. **Security**: Safe execution of customer-provided parser logic
5. **Maintainability**: Simple deployment and configuration management

### Current State

The existing system uses a hardcoded JSON parser with direct Go implementation, providing optimal performance but zero flexibility for customer-specific requirements. Analysis of existing customer parsers reveals common patterns:
- JSON parsing with field extraction and mapping
- Timestamp format conversions (various formats like "MM/dd/yyyy HH:mm:ss.SSS")
- GZIP decompression for compressed payloads
- Computed fields (concatenation, type conversion)
- Graceful error handling with null values for parsing failures

## Decision

We will implement a **YAML-based parser architecture with pre-compiled building blocks** that provides near-native performance while maintaining configurability and ease of use. This will be implemented as a new parser type ("yaml") alongside existing parsers, with runtime configuration through `--parser yaml --parser-config /path/to/parser.yaml`.

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Parser Pipeline                                │
├─────────────────────────────────────────────────────────────────────────┤
│  Input Data    │   YAML Config   │   Building Blocks   │   Output       │
│  ───────────   │   ────────────   │   ──────────────   │   ──────────   │
│  Raw Message   │   Transformation │   Native Go Code   │   Timeseries   │
│  (JSON/CSV/    │   Rules          │   (Pre-compiled)   │   Data         │
│   Binary)      │   (Declarative)  │   (High perf)      │   (QDB)        │
└─────────────────────────────────────────────────────────────────────────┘
```

### Building Block Architecture

```yaml
# Parser Configuration Example (parser.yaml)
# This file ONLY defines data transformation logic, NOT runtime behavior
# Note: error_handling controlled via --parse-error-action flag
# Note: input_format inferred from transformation steps
compression: gzip  # Optional: none, gzip

# Output schema definition
output:
  table_name: "sensor_data"
  columns:
    - name: "timestamp"
      type: "timestamp"
    - name: "temperature"
      type: "double"
    - name: "tag_id"
      type: "string"

# Transformation pipeline
transformations:
  - step: "decompress"
    config:
      algorithm: "gzip"
      
  - step: "parse_json"
    config: {}
    
  - step: "extract_index"
    config:
      source: "T"  # Compact field name in JSON
      format: "MM/dd/yyyy HH:mm:ss.SSS"

  - step: "extract_field"
    config:
      source: "V"
      target: "temperature"
      type: "float64"
      on_error: "skip"  # Field-level error handling

  - step: "compute_field"
    config:
      operation: "concat"
      target: "tag_id"
      fields: ["facility_code", ":", "tagname"]
```

### Core Components

1. **Building Block Library**: Pre-compiled Go functions for common operations
   - **Field Extraction**: JSON path navigation, compact field aliases
   - **Type Conversion**: Safe parsing with configurable error handling
   - **Timestamp Parsing**: Multiple format support (Unix ms, RFC3339, custom)
   - **String Operations**: Concatenation, formatting, case conversion
   - **Compression**: GZIP decompression, future support for zstd
   - **Computed Fields**: Combine multiple fields, conditional logic
   - **Batch Operations**: Process arrays within JSON messages

2. **YAML Configuration Engine**: Declarative parser definition
   - References building blocks by name
   - Configures parameters for each block
   - Defines execution pipeline order

3. **Runtime Pipeline**: Efficient execution engine
   - Pre-compiles YAML to optimized execution plan at startup
   - Direct function calls between blocks (no reflection)
   - Optional parallel execution for independent transformations
   - Pure functions only - no I/O operations allowed

## Performance Characteristics

The building block approach achieves near-native performance through:

```
Traditional Plugin Architecture:
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Message   │───▶│   Plugin    │───▶│   Output    │
│  (Overhead) │    │ (Overhead)  │    │ (Overhead)  │
└─────────────┘    └─────────────┘    └─────────────┘
     ~20%              ~50%               ~10%

Building Block Architecture:
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Message   │───▶│   Compiled  │───▶│   Output    │
│  (Minimal)  │    │   Pipeline  │    │ (Minimal)   │
└─────────────┘    └─────────────┘    └─────────────┘
     ~2%              ~3%               ~1%
```

**Performance Benefits:**
- **No external process overhead**: Everything runs in-process
- **Minimal abstraction cost**: Direct function calls between blocks
- **Optimized data flow**: The parser prepares data for a batch write, which is then executed in a single, efficient operation.
- **Simplified State Management**: Because each worker process handles a batch synchronously, the parser's internal state (`ParseState`) does not need to be goroutine-safe. This eliminates the need for complex concurrent buffering strategies or `sync.Pool` for state objects, leading to a simpler and more maintainable implementation.

## Consequences

### Positive

- **Excellent Performance**: 2-5% overhead vs. hardcoded implementation
- **Developer Friendly**: YAML configuration familiar to most developers
- **Secure**: No arbitrary code execution - only pre-approved building blocks
- **Maintainable**: Building blocks tested and optimized by core team
- **Extensible**: New building blocks can be added without breaking existing parsers
- **Debuggable**: Clear execution pipeline with structured logging

### Negative

- **Limited Flexibility**: Cannot implement arbitrary logic beyond available blocks
- **Block Development Overhead**: New requirements may need new building blocks
- **Configuration Complexity**: Complex transformations require lengthy YAML files
- **Validation Complexity**: YAML config validation and error reporting challenges

### Risk Mitigation

- **Comprehensive Block Library**: Cover 95% of common parsing scenarios
- **Fallback Strategy**: Complex edge cases handled by custom Go development
- **Configuration Tools**: IDE support and validation tooling for YAML configs
- **Migration Path**: Gradual transition from hardcoded to building block approach

## Alternatives Considered

### WebAssembly (WASM) - Rejected

**Evaluation:**
- **Performance Impact**: Good (10-20% overhead)
- **Developer Experience**: Good (Multi-language support)
- **Security**: Excellent (Excellent sandboxing)

**Rejection Reasons:**
- Performance overhead unacceptable for high-throughput requirements
- Additional complexity of WASM runtime integration
- Memory copying costs for large batch operations
- Debugging complexity across language boundaries

### Embedded Scripting (Lua/JavaScript) - Rejected

**Evaluation:**
- **Performance Impact**: Moderate (Interpreted overhead)
- **Developer Experience**: Excellent
- **Security**: Good (Configurable sandboxing)

**Rejection Reasons:**
- Interpreted execution too slow for batch processing requirements
- JIT compilation warmup time impacts first-message latency
- Memory management overhead for large datasets
- Variable performance characteristics under load

### Go Plugin System - Rejected

**Evaluation:**
- **Performance Impact**: Excellent (Native performance)
- **Developer Experience**: Poor (Requires Go knowledge)
- **Security**: Poor (No sandboxing)

**Rejection Reasons:**
- Requires customers to write Go code (violates requirement)
- Linux-only platform support
- No security isolation for customer code
- Runtime loading complexity and reliability concerns

### Expression Languages (CEL, JSONata) - Rejected

**Evaluation:**
- **Performance Impact**: Good (Good for simple operations)
- **Developer Experience**: Good (Declarative approach)
- **Security**: Excellent (Safe by design)

**Rejection Reasons:**
- Insufficient expressiveness for complex transformations
- Cannot handle binary data processing requirements
- Limited support for batch operations and compression
- Lack of debugging and testing infrastructure

## Implementation Plan

### Phase 1: Core Building Blocks (Week 1-2)
- Essential building blocks based on customer patterns:
  - `extract_fields`: JSON field extraction with path support
  - `parse_timestamp`: Multi-format timestamp parsing
  - `decompress`: GZIP decompression
  - `compute_field`: String concatenation and simple expressions
  - `map_fields`: Field-to-column mapping with type conversion
  - `safe_parse_number`: Numeric parsing with null on error
- YAML configuration parser and validator
- Pipeline compilation engine

### Phase 2: Runtime Integration (Week 3-4)
- Add `--parser` option to connector (json|yaml|noop)
- Implement `--parser-config` flag for YAML file path
- Add `--parse-error-action` flag (drop|fail) - controls runtime behavior
- Add `--parser-parallel` flag for parallel execution
- Add `--parser-worker-pool-size` flag for worker count
- Performance benchmarking vs JSON parser

### Phase 3: Production Readiness (Week 5-6)
- Comprehensive error messages and debugging
- Configuration validation at startup
- Example configurations for common patterns
- Migration guide from JSON parser

### Phase 4: Future Enhancements (Week 7+)
- Additional building blocks as needed
- Hot-reload capability for parser configs
- Parser composition (reusable sub-pipelines)
- Performance optimizations

## Monitoring and Success Metrics

- **Performance**: Less than 5% overhead vs. hardcoded implementation
- **Developer Experience**: Customer parser development time less than 2 days
- **Reliability**: 99.9% parser execution success rate
- **Flexibility**: 95% of customer requirements met with building blocks
- **Security**: Zero security incidents from parser execution

## Runtime Configuration

The YAML parser integrates with the NATS connector through command-line options:

```bash
# Parser selection and configuration
qdb-nats-connector \
  --stream "sensor-stream" \
  --topic "sensors.*" \
  --parser yaml \
  --parser-config /etc/qdb/parsers/sensor_parser.yaml \
  --parse-error-action drop \
  --parser-parallel \
  --parser-worker-pool-size 10
```

### Configuration Separation

**YAML Parser Config File** (defines WHAT to parse):
- `compression`: Data compression type
- `output`: Table schema definition
- `transformations`: Transformation step pipeline
- Note: `input_format` inferred from transformation steps
- Note: `error_handling` controlled via runtime flags

**Runtime Flags** (control HOW to parse):
- `--parse-error-action`: drop|fail - behavior on parsing errors
- `--parser-parallel`: Enable parallel processing
- `--parser-worker-pool-size`: Number of parallel workers

This separation ensures:
1. Parser logic is reusable across environments
2. Runtime behavior can be adjusted without file changes
3. Clear separation of concerns between data transformation and execution

## Migration Strategy

1. **Coexistence**: YAML parser lives alongside existing JSON/noop parsers
2. **Gradual Adoption**: Customers can migrate at their own pace
3. **Future Consolidation**: Eventually deprecate hardcoded parsers in favor of YAML-based configurations

## Terminology Update

**Note**: As of 2025-07-15, the YAML configuration terminology has been updated from "building blocks" to "transformation steps" to better reflect the sequential nature of the pipeline processing. The implementation uses:
- `step:` field in YAML configurations (replacing `block:`)
- `TransformationStep` type in Go code (replacing `BuildingBlock`)
- `stepRegistry` variable (replacing `blockRegistry`)

This change maintains backward compatibility - both `step:` and `block:` fields are supported in YAML configurations.

---

*This ADR documents the architectural decision for pluggable parser implementation in the NATS to QuasarDB connector system.*
