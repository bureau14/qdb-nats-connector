# ADR 006: Dynamic Table Routing via Extract Table Transformation

## Status

**Accepted** - 2025-07-21

## Context

The NATS to QuasarDB connector currently assumes one connector instance writes to exactly one table specified in the `output.table_name` configuration. This static approach limits use cases where messages need to be routed to different tables based on their content.

### Current Limitations

1. **Static Configuration**: Each connector instance is hardcoded to write to a single table
2. **No Content-Based Routing**: Cannot route messages to different tables based on message fields
3. **Inflexible Deployment**: Requires multiple connector instances for multi-table scenarios

### Requirements

1. **Dynamic Routing**: Route messages to tables based on message content
2. **Flexible Table Naming**: Support static values, field extraction, and computed names
3. **API Compatibility**: The Parser interface already returns `[]qdb.WriterTable`, supporting multiple tables per message
4. **Consistency**: Align with existing transformation patterns (like `extract_index`)

### Use Cases

- **Multi-tenant Systems**: Route data to tenant-specific tables based on tenant ID
- **Exchange/Symbol Routing**: Create tables like `stocks.nasdaq.apple` from exchange and symbol fields
- **Time-based Partitioning**: Route to tables like `metrics.2025.01` based on timestamp
- **Environment Separation**: Route to different tables based on environment tags

## Decision

Replace the static `output.table_name` configuration with a required `extract_table` transformation step in the YAML parser. This transformation will set `state.Fields["$table"]` similar to how `extract_index` sets `$timestamp`.

### Configuration Examples

**Static Table Name** (equivalent to current behavior):
```yaml
output:
  # table_name: REMOVED - now defined via extract_table
  columns:
    - name: "ingest_ts"
      type: "timestamp"
    - name: "value"
      type: "double"

transformations:
  - step: "extract_table"
    config:
      value: "stocks.apple"  # Static value

  - step: "extract_index"
    config:
      source: "ingest_ts"
      format: "RFC3339"
```

**Field-Based Table Name**:
```yaml
transformations:
  - step: "parse_json"

  - step: "extract_table"
    config:
      source: "table_name"  # Extract from message field

  - step: "extract_index"
    config:
      source: "timestamp"
```

**Computed Table Name with Concat**:
```yaml
transformations:
  - step: "parse_json"
  
  - step: "extract_field"
    config:
      source: "exchange"
      target: "exchange_id"
  
  - step: "extract_field"
    config:
      source: "symbol"
      target: "stock_id"

  - step: "compute_field"
    config:
      operation: "concat"
      target: "$table"
      fields: ["stocks", ".", "exchange_id", ".", "stock_id"]

  - step: "extract_table"
    # Uses default source: "$table"

  - step: "extract_index"
    config:
      source: "timestamp"
```

### Implementation Details

1. **Remove Static Configuration**: Delete `output.table_name` from YAML schema
2. **Add Extract Table Step**: Implement as a transformation step that:
   - Supports `value` for static strings
   - Supports `source` for field extraction (defaults to `$table`)
   - Sets internal table name for downstream processing
   - Works composably with `compute_field` for complex table names
3. **Validation**: Ensure `extract_table` is present in transformation pipeline
4. **Error Handling**: Follow existing patterns - invalid table names cause message to be NACKed

### Special Field Convention

Similar to `$timestamp` for the index field, `$table` becomes the special field for table routing:
- Set by `extract_table` transformation
- Consumed by the sink to determine target table
- Not written as a column to QuasarDB

## Consequences

### Positive

- **Consistency**: Follows established patterns from `extract_index`
- **Flexibility**: Supports static, dynamic, and computed table names
- **Explicit Data Flow**: Users see exactly how table names are determined
- **Single Connector**: One instance can write to multiple tables
- **Simpler Implementation**: No special cases - just another transformation step
- **Composable**: Can combine with other transformations in the pipeline

### Negative

- **Breaking Change**: Existing YAML configs must add `extract_table` step
- **Required Step**: All parsers must include table extraction
- **Learning Curve**: Users must understand the new transformation
- **Migration Effort**: Existing deployments need configuration updates

### Neutral

- **Performance Impact**: Minimal - one additional transformation step
- **Debugging**: Table routing visible in transformation pipeline logs
- **Validation Complexity**: Must ensure `extract_table` exists and produces valid names

## Migration Strategy

### Phase 1: Backward Compatibility (Optional)
```yaml
# If output.table_name exists, inject implicit extract_table step
output:
  table_name: "legacy_table"  # Deprecated

# Automatically prepends:
# - step: "extract_table"
#   config:
#     value: "legacy_table"
```

### Phase 2: Deprecation Warning
- Log warnings when `output.table_name` is used
- Provide migration examples in documentation

### Phase 3: Removal
- Remove `output.table_name` support entirely
- All configurations must use `extract_table`

## Alternative Approaches Considered

### Special Table Name Syntax - Rejected
```yaml
output:
  table_name: "${exchange_id}.${stock_id}"  # Special syntax
```
**Rejection Reasons:**
- Introduces special parsing logic outside transformation pipeline
- Inconsistent with existing patterns
- Hidden behavior not visible in pipeline

### Separate Router Component - Rejected
```yaml
router:
  type: "field-based"
  config:
    field: "table_name"
```
**Rejection Reasons:**
- Adds unnecessary abstraction layer
- Complicates configuration schema
- Diverges from transformation pipeline model

### Multiple Output Sections - Rejected
```yaml
outputs:
  - condition: "exchange_id == 'nasdaq'"
    table_name: "stocks.nasdaq"
  - condition: "exchange_id == 'nyse'"
    table_name: "stocks.nyse"
```
**Rejection Reasons:**
- Complex conditional logic
- Difficult to validate and debug
- Doesn't scale for computed names

## Implementation Plan

1. **Week 1**: Implement `extract_table` transformation step
   - Static value support
   - Field extraction support (with default to `$table`)
   - Integration with existing `compute_field` for complex names

2. **Week 2**: Integration and validation
   - Update YAML schema validation
   - Add configuration examples
   - Implement migration warnings

3. **Week 3**: Testing and documentation
   - Multi-table routing tests
   - Performance benchmarks
   - Migration guide

## Success Metrics

- **Adoption**: 50% of new deployments use dynamic routing within 3 months
- **Performance**: <1% overhead for table name extraction
- **Reliability**: No increase in message processing errors
- **Simplicity**: Configuration complexity remains manageable

---

*This ADR documents the decision to implement dynamic table routing through the transformation pipeline for the NATS to QuasarDB connector.*
