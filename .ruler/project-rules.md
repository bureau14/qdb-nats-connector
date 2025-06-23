# qdb-nats-connector

NATS -> QuasarDB connector with pluggable parser architecture for flexible message transformation.

## Core Responsibilities
- Subscribe to configured NATS subjects
- Route messages through configurable parser pipeline
- Transform parsed data to QuasarDB timeseries data
- Handle connection failures and reconnection for both systems
- Handle checkpointing and recovery
- Batch writes for performance

## Parser Architecture
- Plugin-based parser system loaded at runtime
- Built-in parsers for common formats (JSON, CSV, Protobuf)
- Parser interface: `Parse([]byte) -> (map[string]interface{}, error)`
- Chain multiple parsers for complex transformations
- Per-subject parser configuration

## Key Dependencies
- `github.com/nats-io/nats.go` - NATS client
- `github.com/bureau14/qdb-api-go` - QuasarDB Go API
- `plugin` package for dynamic parser loading


## Project Structure
```
main.go            # entrypoint
connector/         # bridge logic
internal/          # private utilities
qdb/               # QuasarDB C headers (read-only)
scripts/           # automation
```

## Test execution
```bash
direnv exec . go test -v ./...                          # Run full test suite
direnv exec . go test -v ./... -run '<TestNamePattern>' # Run individual or prefixed tests
```
