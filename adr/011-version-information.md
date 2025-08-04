# ADR-011: Version Information and Build Metadata

## Status
Accepted

## Context
Our binaries currently don't expose version information, making it difficult to identify which build is running in production, what optimizations were used, or when it was built. This is problematic for debugging, support, and audit trails.

Our C++ products display comprehensive version information:
```
quasardb daemon version: 3.15.0.dev0
build: 3a37b406e32c482b260c16e544210f23da184ebd
date: 2025-07-15 20:03:10 +0300
target: arm64-Darwin-24.5.0
compiler: Clang (17.0.6)
build type: Release
cpu: haswell
```

We need equivalent information for Go binaries to maintain consistency and operational visibility.

## Decision
We will implement `--version` flags for all binaries that display comprehensive build metadata captured at compile time.

### Version Output Format
```
quasardb nats connector version: 1.0.0.dev0
build: 3a37b406e32c482b260c16e544210f23da184ebd
date: 2025-08-04 15:30:00 +0000

target: amd64-linux-5.15.0
compiler: go1.21.5
arch level: v3

build type: release

Copyright (c) 2009-2025, quasardb SAS. All rights reserved.
```

### Build-Time Information Capture

All information will be injected at compile time via `-ldflags`:

1. **version** - Semantic version (e.g., "1.0.0.dev0")
2. **commit** - Git SHA (short form)
3. **buildTime** - RFC3339 timestamp
4. **buildMode** - release/debug/release-min
5. **goamd64** - Microarchitecture level (v1/v3)

### Microarchitecture Levels

We align with our C++ build targets:

| C++ Target | Go Equivalent | Features        | Usage                 |
|------------|---------------|-----------------|-----------------------|
| core2      | GOAMD64=v1    | SSE2 (baseline) | Maximum compatibility |
| haswell    | GOAMD64=v3    | AVX2, FMA, BMI2 | Production default    |

**Default**: Non-ARM release builds will use GOAMD64=v3 (haswell equivalent), matching our C++ defaults.

### Runtime vs Compile-Time Information

All displayed information is compile-time only:
- **compiler**: `runtime.Version()` - Go version used to compile
- **target**: `runtime.GOOS`, `runtime.GOARCH` - Target OS/arch
- **arch level**: GOAMD64 setting - Microarchitecture optimization level

We explicitly avoid runtime CPU detection as it would show where the binary is running, not how it was built.

### Implementation Requirements

1. **Main Package Variables**
```go
var (
    version   = "dev0"
    commit    = "unknown"
    buildTime = "unknown"
    buildMode = "unknown"
    goamd64   = "v3"
)
```

2. **Version Flag Handler**
```go
if showVersion {
    fmt.Printf("quasardb %s version: %s\n", appName, version)
    fmt.Printf("build: %s\n", commit)
    fmt.Printf("date: %s\n\n", buildTime)
    fmt.Printf("target: %s-%s\n", runtime.GOARCH, runtime.GOOS)
    fmt.Printf("compiler: %s\n", runtime.Version())
    if runtime.GOARCH == "amd64" && goamd64 != "" {
        fmt.Printf("arch level: %s\n", goamd64)
    }
    fmt.Printf("\nbuild type: %s\n\n", buildMode)
    fmt.Println("Copyright (c) 2009-2025, quasardb SAS. All rights reserved.")
    os.Exit(0)
}
```

3. **Makefile Integration**
```makefile
# Architecture optimization
ifeq ($(BUILD_MODE),release)
    GOAMD64 ?= v3  # Default to haswell-equivalent for production
else
    GOAMD64 ?= v1  # Baseline for debug builds
endif

LDFLAGS_VERSION := -X main.version=$(VERSION) \
                   -X main.commit=$(GIT_SHA) \
                   -X main.buildTime=$(BUILD_TIME) \
                   -X main.buildMode=$(BUILD_MODE) \
                   -X main.goamd64=$(GOAMD64)
```

## Consequences

### Positive
- **Debugging**: Easy identification of binary version in production
- **Support**: Clear understanding of optimization levels and build flags
- **Consistency**: Matches our C++ tooling output format
- **Audit**: Build timestamp and commit SHA provide traceability
- **Performance**: GOAMD64=v3 enables AVX2 optimizations by default

### Negative
- **Binary size**: Small increase due to embedded strings (~1KB)
- **Compatibility**: GOAMD64=v3 binaries won't run on pre-Haswell CPUs (2013), mitigated by having multiple targets
  - Mitigated by providing v1 builds when needed

### Neutral
- Requires updating CI/CD to set VERSION for releases
- Aligns with Go community practices for version information

## Notes

### GOAMD64 Levels Explained
- **v1**: Baseline x86-64 + SSE2 (all x86-64 CPUs)
- **v2**: Adds SSE3, SSSE3, SSE4.1, SSE4.2, POPCNT
- **v3**: Adds AVX, AVX2, BMI1, BMI2, FMA (Haswell+)
- **v4**: Adds AVX-512 (Skylake-X+)

### Why v3 as Default?
- Haswell was released in 2013 (11+ years ago)
- Provides significant performance improvements via AVX2
- Matches our C++ default target
- Reasonable baseline for modern server deployments

### ARM Considerations
- ARM64 doesn't have GOAMD64 levels
- Go assumes ARMv8-A baseline for all arm64
- No additional configuration needed for Apple Sillicon Macs or ARM-based Linux

## References
- [Go 1.18 Release Notes - GOAMD64](https://go.dev/doc/go1.18#amd64)
- [Go Build Constraints](https://pkg.go.dev/cmd/go#hdr-Build_constraints)
- [runtime.Version() Documentation](https://pkg.go.dev/runtime#Version)
