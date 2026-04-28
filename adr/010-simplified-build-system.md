# ADR-010: Simplified Variable-Driven Build System

## Status
Accepted

## Context
Our current Makefile had evolved into a complex system with competing patterns:
- Both `make debug` targets AND `BUILD_MODE=debug` variable approaches
- Proliferation of targets for each binary × build mode combination
- Confusion about build modes and their purposes
- Similarity to our `examples/Makefile` which had grown unwieldy with too many targets

We need:
- Simple, maintainable build system
- Production builds with debug symbols for crash analysis (matching standard RelWithDebInfo approach)
- Clear separation between debug, release, and minimal builds
- Support for 3 binaries without combinatorial explosion of targets

## Decision
We will adopt a purely variable-driven Makefile approach with minimal verb targets, eliminating all redundant patterns.

### Build Modes
We define three clear build modes via `BUILD_MODE` variable:

1. **release** (default) - Production-ready with debug symbols
   - Optimized code with `-trimpath`
   - Retains DWARF debug information
   - Suitable for RPM packaging with separate debuginfo packages
   - Equivalent to CMake's RelWithDebInfo

2. **debug** - Local development debugging
   - Unoptimized with `-gcflags="all=-N -l"`
   - Full debug symbols for dlv/gdb
   - No trimpath for easier debugging

3. **release-min** - Minimal size binaries
   - Strips all debug info with `-ldflags="-s -w"`
   - Only use when debuginfo is captured separately
   - For special deployments where size matters

### Target Structure
Minimal verb-based targets only:
- `make build` - Build all binaries
- `make build-single` - Build single binary (with `BIN=name`)
- `make test` - Run tests
- `make lint` - Run linters
- `make clean` - Clean artifacts
- `make help` - Show usage

### Key Design Principles
1. **No target proliferation** - Avoid `build-debug-connector` style targets
2. **Single source of truth** - One place maps BUILD_MODE to compiler flags
3. **Variable-driven** - Configuration via variables, not target names
4. **CI-friendly** - Variables work well with CI matrix builds
5. **Default to production** - Safe, debuggable builds by default

## Consequences

### Positive
- **Simplicity** - One way to specify build configuration
- **Maintainability** - Adding binaries requires minimal changes
- **Clarity** - Clear separation of concerns between targets (verbs) and configuration (variables)
- **Scalability** - Pattern scales without complexity growth
- **CI Integration** - Easy to parameterize builds in CI/CD pipelines
- **Consistency** - All binaries built with identical flag logic

### Negative
- **Learning curve** - Developers must learn variable-based approach
- **Less discoverable** - Can't tab-complete build modes (mitigated by `make help`)
- **Breaking change** - Existing scripts using `make debug` must update

### Neutral
- Aligns with Go community practices (Kubernetes, HashiCorp tools use similar patterns)
- Matches Unix philosophy of composable tools with parameters

## Implementation Notes

### Flag Mappings
```makefile
# release (default): Production with debuginfo
GOFLAGS := -trimpath
LDFLAGS := $(LDFLAGS_VERSION)

# debug: Unoptimized for debugging
GOFLAGS :=
LDFLAGS := $(LDFLAGS_VERSION)
GCFLAGS := all=-N -l

# release-min: Stripped binaries
GOFLAGS := -trimpath
LDFLAGS := -s -w $(LDFLAGS_VERSION)
```

### Version Injection
All modes inject version metadata:
```makefile
LDFLAGS_VERSION := -X main.version=$(VERSION) -X main.commit=$(GIT_SHA) -X main.buildTime=$(BUILD_TIME)
```
