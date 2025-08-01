# ADR-009: Go-Based Data Generation with YAML Templates

## Status
Accepted - 2025-07-31

## Context
Current shell-based data generation spawns a new process for each message, creating massive overhead that prevents realistic performance testing. We need flexible data generation that supports diverse formats and complex patterns while maintaining high throughput for load testing.

## Decision
We will implement Go-based data generation utilities within the qdb-nats-connector project using pure YAML templates for configuration, designed for flexibility and extensibility rather than peak performance.

## Rationale

### Shell Script Performance Problem

**Current Per-Message Overhead:**
```
Message 1: [spawn shell] → [exec commands] → [publish] → [cleanup]
Message 2: [spawn shell] → [exec commands] → [publish] → [cleanup]
Message 3: [spawn shell] → [exec commands] → [publish] → [cleanup]
...
Process overhead: ~10ms/message × 100k = 16+ minutes for test data
```

**Go Solution with YAML Templates:**
```
Generator:
[Initialize] → [Load YAML] → [Stream Generation] → [Output]
     ↓             ↓               ↓               ↓
[1x Setup]   [Parse Templates]  [Memory Ops]   [File/NATS]
```

### YAML Template Architecture

Pure YAML templates provide declarative data generation without text templating complexity:

```yaml
# Example template structure (one generator per YAML file)
name: "sensor_data"
table: "industrial_sensors"
pattern: "time_series"
fields:
  timestamp:
    type: "timestamp"
    start: "2024-01-01T00:00:00Z"
    interval: "1s"
    mode: "relative"     # For continuous mode: "relative", "now", "sliding_window"
    offset: "-1h"        # Start 1 hour ago (continuous mode)
    window: "5m"         # Keep within 5 minute window (continuous mode)
  sensor_id:
    type: "pattern_composite"
    cardinality: 2000000  # Target number of unique IDs
    patterns:
      - weight: 40
        template: "{location}/{equipment}-{model}.{sensor}.{suffix}"
        components:
          location: ["ZONE01", "ZONE02", "AREA_A", "AREA_B", "SECTOR1", "SECTOR2"]
          equipment: ["UNIT", "DEVICE", "SYSTEM", "MODULE", "EQUIP"]
          model: ["MODEL1", "MODEL2", "TYPE_A", "TYPE_B", "V1", "V2"]
          sensor:
            type: "pattern"
            prefix: ["10", "20", "30", "40"]
            base: ["TEMP", "PRESS", "FLOW", "LEVEL", "SENSOR", "METER"]
            digits: 2-4
          suffix: ["VALUE", "READING", "STATUS", "METRIC"]

      - weight: 30
        template: "{location}/{equipment}-{equipment}-{model}.{sensor}.{measurement}"
        # Handles the repeated "UNIT-UNIT" pattern

      - weight: 20
        template: "{equipment}-{sensor}"
        # Simple format like "UNIT-20SENSOR01"

      - weight: 10
        template: "{location}/{subsystem}/{equipment}.{sensor}.{long_description}"
        components:
          long_description:
            type: "weighted_choice"
            options:
              - "Capacity Utilization"
              - "Temperature Reading"
              - "Status Alert"
              - "Flow Rate Metric"
  temperature:
    type: "brownian_motion"
    base: 22.5
    volatility: 0.1
    bounds: [15.0, 35.0]
  network_latency:
    type: "network_burst"
    baseline: 1.2
    burst_probability: 0.05
    burst_multiplier: 10
```

### Design for Flexibility Over Performance

**Generator Utility (`qdb-data-gen`):**
- **Purpose**: Create diverse, realistic test datasets using YAML templates
- **Output**: Stdout-only design using Unix I/O redirection (no --output flag)
- **Formats**: Multiple formats (JSON Lines, Parquet, compressed data) with base64 encoding for binary content
- **Modes**: Batch generation and continuous streaming for stress testing
- **Focus**: Data variety, complex patterns, extensibility, Unix philosophy
- **Performance**: Cloud parallelization compensates for moderate single-instance speed

**Loader Utility (`qdb-data-load`):**
- **Purpose**: Publish data to NATS with batching and streaming support
- **Input**: Files or stdin streams in various formats (Unix `--file -` convention)
- **Binary Handling**: Automatic detection and decoding of base64-encoded binary messages
- **Focus**: Format flexibility, reliable delivery, intelligent buffering
- **Scalability**: Horizontal scaling across cloud instances
- **Streaming**: Smart batching for 100k+ messages/second through stdin

**Architecture Benefits:**
1. **Unix Philosophy**: Stdout-only design enables flexible composition with shell redirection
2. **Format Extensibility**: Support JSON, Parquet, compressed formats with binary message encoding
3. **Pattern Flexibility**: Advanced generators (Brownian motion, network bursts, signal synthesis)
4. **Cloud Scaling**: Multiple instances handle performance requirements
5. **Template Reusability**: Individual YAML templates shared across projects and teams
6. **Binary Safety**: Base64 encoding preserves newline-delimited format for binary payloads

### YAML Template Processing Architecture

```
                    Generator Architecture
                    =====================

┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   YAML Template │    │   Data Pipeline  │    │   Stdout-Only   │
│   Engine        │───▶│                  │───▶│   Output        │
│                 │    │ ┌──────────────┐ │    │                 │
│ • Pure YAML     │    │ │ Generator    │ │    │ ┌─────────────┐ │
│ • No Templating │    │ │ Registry     │ │    │ │ JSON Lines  │ │
│ • Pattern Types │    │ └──────────────┘ │    │ │ Parquet     │ │
│   - time_series │    │ ┌──────────────┐ │    │ │ Compressed  │ │
│   - brownian    │    │ │ Field        │ │    │ │ Base64 Enc  │ │
│   - network     │    │ │ Generators   │ │    │ └─────────────┘ │
│   - signal      │    │ │ (pluggable)  │ │    │ ┌─────────────┐ │
│   - gzipped_json│    │ └──────────────┘ │    │ │ Unix Redir  │ │
│                 │    │                  │    │ │ (> file)    │ │
└─────────────────┘    └──────────────────┘    │ └─────────────┘ │
                                               └─────────────────┘

                      Loader Architecture
                      ===================

┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Multi-Format  │    │   Data Pipeline  │    │   NATS Client   │
│   Input         │───▶│                  │───▶│                 │
│                 │    │                  │    │                 │
│ ┌─────────────┐ │    │ ┌──────────────┐ │    │ ┌─────────────┐ │
│ │ JSON Lines  │ │    │ │ Format       │ │    │ │ Batch Pub   │ │
│ │ Parquet     │ │    │ │ Detector     │ │    │ │ (adaptive)  │ │
│ │ Compressed  │ │    │ │ Base64 Dec   │ │    │ └─────────────┘ │
│ └─────────────┘ │    │ └──────────────┘ │    │ ┌─────────────┐ │
│ ┌─────────────┐ │    │ ┌──────────────┐ │    │ │ Topic       │ │
│ │   Stream    │ │    │ │ Streaming    │ │    │ │ Routing     │ │
│ │Decompression│ │    │ │ Parser       │ │    │ │ (flexible)  │ │
│ └─────────────┘ │    │ │ (pluggable)  │ │    │ └─────────────┘ │
└─────────────────┘    │ └──────────────┘ │    └─────────────────┘
                       └──────────────────┘
```

### Advanced Generator Types

**Supported Pattern Types:**
- **time_series**: Configurable intervals, realistic timestamp sequences with continuous mode support
- **brownian_motion**: Financial/sensor data with volatility and bounds
- **network_burst**: Baseline values with periodic burst events
- **signal_synthesis**: Electrical waveforms with harmonics and noise
- **pattern_composite**: Complex hierarchical identifiers for high-cardinality industrial sensors using weighted pattern templates
- **gzipped_json**: JSON data compressed with gzip and base64-encoded for binary-safe transport
- **sequence**: Rotating through predefined value sets
- **random**: Various probability distributions (normal, uniform, exponential)
- **stress_pattern**: Burst simulation for load testing with configurable intensity
- **chaos**: Failure injection patterns for reliability testing

### Pattern-Based Sensor ID Generation

**High-Cardinality Industrial Sensors:**
The `pattern_composite` generator addresses real-world industrial manufacturing scenarios where sensor IDs follow complex hierarchical naming conventions. Industrial IoT deployments often require millions of unique sensor identifiers that match existing vendor patterns and organizational structures.

**Key Capabilities:**
- **Deterministic Generation**: Weighted pattern templates ensure consistent distribution across realistic ID formats
- **Memory-Efficient**: Iterator-based implementation avoids pre-generating millions of IDs
- **Hierarchical Support**: Multi-level naming conventions (`location/equipment-model.sensor.measurement`)
- **Pattern Flexibility**: Simple string substitution with component arrays or dynamic generators
- **Cardinality Control**: Target unique ID count with automatic combination validation

**Generic Industrial ID Examples:**
```
ZONE01/UNIT-MODEL1.10TEMP001.VALUE                    # Location/Equipment-Model.Sensor.Measurement
AREA_A/DEVICE-DEVICE-TYPE_A.20SENSOR02.Capacity Utilization   # Repeated equipment pattern
UNIT-20SENSOR01                                       # Simple equipment-sensor format
SECTOR1/subsystem/SYSTEM.30PRESS05.Temperature Reading  # Deep hierarchy
```

**Template Processing:**
- **Weighted Distribution**: Pattern templates control ID format variety (40% hierarchical, 30% repeated, 20% simple, 10% deep)
- **Component Types**: Fixed arrays (`location: ["ZONE01", "AREA_A"]`) or pattern generators (`sensor: {type: "pattern", prefix: ["10", "20"], base: ["TEMP", "PRESS"], digits: 2-4}`)
- **String Substitution**: Template placeholders replaced with component values using simple `{key}` syntax
- **Combination Validation**: Cardinality parameter ensures sufficient unique combinations across all weighted patterns

**Implementation Architecture:**
```
Pattern Composite Generator
============================

┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Template      │    │   Component      │    │   ID Generator  │
│   Engine        │───▶│   Resolution     │───▶│                 │
│                 │    │                  │    │                 │
│ ┌─────────────┐ │    │ ┌──────────────┐ │    │ ┌─────────────┐ │
│ │ Weight Sel  │ │    │ │ Array Lookup │ │    │ │ Iterator    │ │
│ │ Template    │ │    │ │ Pattern Gen  │ │    │ │ Memory Eff  │ │
│ └─────────────┘ │    │ │ Choice Logic │ │    │ │ Cardinality │ │
│ ┌─────────────┐ │    │ └──────────────┘ │    │ └─────────────┘ │
│ │ Parse {key} │ │    │ ┌──────────────┐ │    │ ┌─────────────┐ │
│ │ Placeholders│ │    │ │ Substitution │ │    │ │Deterministic│ │
│ └─────────────┘ │    │ │ Engine       │ │    │ │ Sequencing  │ │
└─────────────────┘    │ └──────────────┘ │    │ └─────────────┘ │
                       └──────────────────┘    └─────────────────┘
```

**Continuous Mode Integration:**
Pattern-composite generators work seamlessly with continuous streaming mode, providing deterministic sensor ID sequences that cycle through the full cardinality space. This enables long-running stress tests with realistic industrial sensor naming patterns while maintaining memory efficiency through iterator-based generation.

### Binary Message Encoding Design

**Problem Statement:**
When individual messages contain binary data (such as gzipped JSON payloads), the raw binary bytes can contain newline characters (`\n`, `0x0A`) that break the newline-delimited format used by both generator and loader utilities. This is particularly critical for customer use cases where each message is a compressed JSON payload sent over bandwidth-constrained networks.

**Solution Architecture:**
```
Binary Data Flow with Base64 Encoding
======================================

Generator Side:
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ JSON Data   │───▶│ Gzip        │───▶│ Base64      │───▶│ Stdout      │
│ {"temp":22} │    │ Compress    │    │ Encode      │    │ Line        │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘

Loader Side:
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Stdin Line  │───▶│ Base64      │───▶│ Gzip        │───▶│ NATS        │
│ (detected)  │    │ Decode      │    │ Decompress  │    │ Publish     │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

**Implementation Details:**
- **Generator (`qdb-data-gen`)**: `gzipped_json` field type automatically compresses JSON data and base64-encodes the result
- **Loader (`qdb-data-load`)**: Automatic detection of base64-encoded content using pattern recognition or metadata flags
- **Format Preservation**: Newline-delimited format maintained while supporting arbitrary binary content
- **Performance**: Base64 encoding adds ~33% size overhead but enables reliable streaming transport

**Customer Use Case Support:**
```yaml
# Example template for gzipped JSON messages
name: "compressed_sensor_data"
fields:
  message:
    type: "gzipped_json"
    content:
      timestamp: { type: "timestamp", mode: "now" }
      sensor_id: { type: "pattern", template: "DEVICE_{id}", id: { type: "sequence", range: [1, 1000] } }
      temperature: { type: "brownian_motion", base: 22.5, volatility: 0.1 }
      pressure: { type: "brownian_motion", base: 101.3, volatility: 0.05 }
```

This design ensures that compressed binary payloads can be safely transported through the newline-delimited pipeline while maintaining the Unix philosophy of simple, composable tools.

### Stdout-Only Design Philosophy

**Design Decision:**
The `qdb-data-gen` utility follows the Unix philosophy by writing exclusively to stdout, with no `--output` flag or direct file writing capabilities. Users rely on shell I/O redirection (`>`, `>>`, `|`) for all file operations and pipeline composition.

**Rationale:**
```
Traditional Approach (Rejected):
  qdb-data-gen --template sensor.yaml --output data.jsonl
  qdb-data-gen --template sensor.yaml --output-dir /tmp/data/

Unix Philosophy Approach (Adopted):
  qdb-data-gen sensor.yaml > data.jsonl
  qdb-data-gen sensor.yaml >> existing_data.jsonl
  qdb-data-gen sensor.yaml | qdb-data-load --file - --topic sensors
  qdb-data-gen sensor.yaml | gzip > compressed_data.jsonl.gz
```

**Benefits of Stdout-Only Design:**
- **Simplicity**: Single responsibility - generate data, let shell handle I/O
- **Composability**: Natural pipeline integration with other Unix tools
- **Flexibility**: Shell redirection more powerful than custom flags
- **Testability**: Easy to capture and verify output in tests
- **Debugging**: Simple to inspect output with standard tools (`head`, `tail`, `wc`)
- **Streaming**: Natural support for continuous mode with pipes
- **Parallelization**: Easy to combine with GNU parallel and process substitution

**Implementation Examples:**
```bash
# Basic file output
qdb-data-gen sensor_template.yaml > sensor_data.jsonl

# Append to existing file
qdb-data-gen additional_data.yaml >> sensor_data.jsonl

# Direct pipeline to loader
qdb-data-gen continuous_template.yaml --mode continuous | qdb-data-load --file - --topic live_sensors

# Compress output
qdb-data-gen large_dataset.yaml | gzip > compressed_dataset.jsonl.gz

# Parallel generation
parallel 'qdb-data-gen templates/{}.yaml > output/{}.jsonl' ::: sensor1 sensor2 sensor3

# Split output
qdb-data-gen massive_dataset.yaml | split -l 100000 - data_chunk_

# Monitor generation
qdb-data-gen --mode continuous template.yaml | tee monitoring.log | qdb-data-load --file - --topic sensors
```

This approach embraces the Unix philosophy of "do one thing and do it well" while enabling powerful composition through standard shell mechanisms.

**Complex Data Format Support:**
- **Parquet**: Efficient columnar storage for analytical workloads
- **Gzipped JSON**: Compressed structured data with base64 encoding for safe newline-delimited transport
- **Binary formats**: Extensible for custom data representations with automatic base64 encoding
- **Mixed formats**: Each template generates multiple output formats via stdout
- **Newline Safety**: Binary data automatically base64-encoded to preserve line-based format integrity

**Cloud Scaling Strategy:**
```
Performance through Parallelization:
  qdb-data-gen sensor_data.yaml > output_a.jsonl &
  qdb-data-gen electrical_data.yaml > output_b.jsonl &
  qdb-data-gen network_metrics.yaml > output_c.jsonl &

Continuous Mode Scaling (Unix Philosophy):
  qdb-data-gen --mode continuous --rate 10000 template.yaml | qdb-data-load --file - --topic sensors
  Named pipes: mkfifo stress_pipe && qdb-data-gen --mode continuous template.yaml > stress_pipe &
  GNU parallel: parallel 'qdb-data-gen --template {} --mode continuous > {.}.jsonl' ::: *.yaml

Binary Data Handling:
  qdb-data-gen --type gzipped_json template.yaml > compressed.jsonl  # Auto base64-encoded
  qdb-data-load --file compressed.jsonl --topic data                # Auto detected & decoded

Aggregate throughput = N × Single-instance performance
Cost optimization = Pay per generation time
Flexibility = Different instance types for different data complexity
```

### Implementation Milestones

**Milestone 1: Core Generator Framework**
1. Create `tools/qdb-data-gen/` with YAML template engine
2. Implement basic field generators (timestamp, sequence, random)
3. Support JSON Lines output with stdout-only streaming (no --output flag)
4. Add CLI interface with template file input and `--mode continuous` flag
5. Implement base64 encoding for binary field types to maintain newline-delimited format

**Milestone 2: Advanced Generators & Continuous Mode**
1. Implement Brownian motion generator for realistic sensor data
2. Add network burst generator for realistic network patterns
3. Implement signal synthesis for electrical waveform data
4. Implement pattern_composite generator for high-cardinality industrial sensor IDs with weighted templates
5. Add gzipped_json field type for compressed binary payloads with automatic base64 encoding
6. Add continuous mode with rate limiting (`--rate` parameter)
7. Implement relative timestamp modes (now/sliding_window) for streaming
8. Add stress_pattern and chaos generators for load testing
9. Add pluggable generator registry for extensibility

**Milestone 3: Multi-Format Support & Streaming Optimization**
1. Add Parquet output support for analytical workloads (stdout-only with Unix redirection)
2. Implement gzip compression with base64 encoding for bandwidth optimization
3. Create format detection and base64 decoding in loader utility
4. Support mixed-format output from individual templates via stdout
5. Implement Unix-standard stdin support (`--file -`) in qdb-data-load with base64 detection
6. Add intelligent buffering with `--batch-size` and `--batch-timeout`
7. Optimize for 100k+ messages/second through smart batching

**Milestone 4: Cloud Integration & Parallel Generation**
1. Optimize for horizontal scaling across instances with stdout redirection patterns
2. Add template validation and error reporting
3. Create example templates for common use cases including continuous mode and binary data
4. Document template authoring best practices including Unix I/O redirection patterns
5. Add support for named pipes and GNU parallel scaling patterns with stdout design
6. Implement compressed stream handling with base64 encoding for network optimization

**Backward Compatibility:**
- Existing example scripts maintain same CLI interface
- Golden data format preserved where used
- Shell script fallback during transition period

## Consequences

### Benefits
- **Unix Philosophy**: Stdout-only design enables flexible composition with shell redirection
- **Binary Safety**: Base64 encoding preserves newline-delimited format for binary payloads
- **Flexibility**: YAML templates support diverse data patterns and formats
- **Extensibility**: Pluggable generator architecture for new data types
- **Maintainability**: Go code easier to test and debug than shell scripts
- **Scalability**: Cloud parallelization overcomes single-instance performance limits
- **Reusability**: Individual templates shareable across projects and teams
- **Format Support**: Native handling of Parquet, compression, and binary formats with encoding
- **Stress Testing**: Continuous mode enables long-term reliability testing
- **Unix Compliance**: Standard stdin/stdout patterns for seamless toolchain integration
- **High-Throughput Streaming**: Smart batching achieves 100k+ messages/second

### Tradeoffs
- **Single-Instance Performance**: Moderate speed compensated by horizontal scaling
- **Template Complexity**: YAML templates require learning for advanced patterns
- **Go Dependency**: Go toolchain required for development and generation
- **Cloud Costs**: Multiple instances for high-throughput scenarios
- **Development Time**: More sophisticated architecture vs simple shell scripts
- **Memory Usage**: Continuous mode requires buffering for optimal batch performance
- **Encoding Overhead**: Base64 encoding adds ~33% size overhead for binary data
- **Format Constraints**: Stdout-only design requires shell redirection for file output

## Implementation

1. Create `tools/qdb-data-gen/` with YAML template processing engine, stdout-only output, and continuous mode support
2. Create `tools/qdb-data-load/` with multi-format input support, Unix stdin handling, and base64 decoding
3. Implement pluggable field generator registry (Brownian motion, network bursts, signal synthesis, gzipped_json, pattern_composite for industrial sensors, stress patterns)
4. Add Parquet and compression output support with streaming optimization and base64 encoding for binary content
5. Implement intelligent buffering and batching for high-throughput streaming (100k+ msgs/sec)
6. Add relative timestamp modes and rate limiting for continuous stress testing
7. Update example Makefiles to use Go utilities with template files, Unix redirection patterns, and continuous mode examples
8. Create comprehensive example templates for common use cases including binary data handling and stress testing scenarios

## Technical Requirements

**qdb-data-gen Requirements:**
- Pure YAML template parsing without text templating engines
- Stdout-only output design (no --output flag) with Unix I/O redirection
- Pluggable field generator architecture (timestamp, sequence, brownian, network, signal, gzipped_json, pattern_composite, stress_pattern, chaos)
- Pattern-based generation for high-cardinality industrial sensor IDs with weighted templates and iterator-based memory efficiency
- Multi-format output (JSON Lines, Parquet, gzipped formats) with base64 encoding for binary content
- Streaming output to stdout to handle large datasets efficiently
- Continuous mode with `--mode continuous` flag and rate limiting (`--rate` parameter)
- Relative timestamp support (now/sliding_window/relative modes) for streaming
- Template validation with clear error reporting
- Dynamic value anchoring for evolving data patterns in long-running tests
- Base64 encoding for binary field types to maintain newline-delimited format integrity

**qdb-data-load Requirements:**
- Multi-format input detection and parsing (JSON Lines, Parquet, compressed)
- Automatic base64 detection and decoding for binary message content
- Unix-standard stdin support with `--file -` parameter
- Streaming input processing for large files with intelligent buffering
- Smart batching (configurable `--batch-size` and `--batch-timeout`) for 100k+ msgs/sec
- Batched NATS publishing with adaptive batch sizes
- Format-aware topic routing and message transformation
- Compatible with existing NATS JetStream configuration
- Memory-efficient streaming processing (not row-by-row)
- Transparent handling of base64-encoded binary payloads from qdb-data-gen

**Shared Requirements:**
- Error handling using `internal/errors` constructors
- Context-based cancellation and timeout support
- CLI interface using spf13/cobra for consistency
- Cloud-friendly design for horizontal scaling
- direnv compatibility for development environment
