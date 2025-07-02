# qdb-nats-connector

NATS→QuasarDB connector with pluggable parsers.

## Core
- Subscribe NATS subjects
- Route through parser pipeline
- Transform to QuasarDB timeseries
- Handle reconnection/checkpointing
- Batch writes

## Parsers
- Runtime plugin loading
- Interface: `Parse([]byte) → (map[string]any, error)`
- Chain for complex transforms
- Per-subject config

## Structure
```
.golangci.yml # Linting, **NEVER CHANGE**
main.go       # entry
connector/    # bridge
internal/     # utils
insecure/     # Always ignore
qdb/          # C headers (readonly)
scripts/      # bash scripts
secure/       # Always ignore
```


## Environment
**ALWAYS prefix commands**: `direnv exec . <command>`

Dependencies: `nats.go`, `qdb-api-go`, `plugin`
