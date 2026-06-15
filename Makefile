.PHONY: all build test lint clean help

# Binary definitions
BINARIES := qdb-nats-connector qdb-data-gen qdb-data-loader
BIN_DIR := bin

# Version info (can be overridden)
VERSION ?= $(shell cat VERSION 2>/dev/null || echo "3.15.0.dev0")
GIT_SHA ?= $(shell git rev-parse HEAD 2>/dev/null || echo "unknown")
BUILD_TIME ?= $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')
KERNEL_VERSION ?= $(shell uname -r 2>/dev/null || echo "unknown")

# Build configuration
# Default: production-ready with debug symbols (equivalent to CMake's RelWithDebInfo)
# Options: release (optimized with debuginfo), release-min (stripped), debug (unoptimized)
BUILD_MODE ?= release

# Go toolchain
GO := go

# Version injection flags (always applied)
LDFLAGS_VERSION := -X main.version=$(VERSION) \
                   -X main.commit=$(GIT_SHA) \
                   -X main.buildTime=$(BUILD_TIME) \
                   -X main.buildMode=$(BUILD_MODE) \
                   -X main.goamd64=$(GOAMD64) \
                   -X main.kernelVersion=$(KERNEL_VERSION)

# Mode-specific flags
ifeq ($(BUILD_MODE),release)
	# Production build: optimized with debug symbols for debugging crashes
	GOFLAGS := -trimpath -mod=vendor
	LDFLAGS := $(LDFLAGS_VERSION)
	GCFLAGS :=
else ifeq ($(BUILD_MODE),release-min)
	# Minimal release: stripped binaries (only if debuginfo captured separately)
	GOFLAGS := -trimpath -mod=vendor
	LDFLAGS := -s -w $(LDFLAGS_VERSION)
	GCFLAGS :=
else ifeq ($(BUILD_MODE),debug)
	# Debug build: unoptimized for debugging with dlv/gdb
	GOFLAGS := -mod=vendor
	LDFLAGS := $(LDFLAGS_VERSION)
	GCFLAGS := all=-N -l
else
	$(error Unknown BUILD_MODE '$(BUILD_MODE)'. Valid options: release, release-min, debug)
endif

# Architecture-specific optimization (GOAMD64)
# Only set for amd64 architecture to avoid errors on ARM
GOARCH := $(shell go env GOARCH)
ifeq ($(GOARCH),amd64)
ifeq ($(BUILD_MODE),release)
	GOAMD64 ?= v3  # AVX2 (haswell equivalent)
else ifeq ($(BUILD_MODE),release-min)
	GOAMD64 ?= v3  # AVX2 (haswell equivalent)
else ifeq ($(BUILD_MODE),debug)
	GOAMD64 ?= v1  # Baseline SSE2
endif
endif

# Default target
all: lint test build

# Build all binaries
build: $(BIN_DIR)
	@echo "Building in $(BUILD_MODE) mode..."
	@echo "  GOFLAGS: $(GOFLAGS)"
	@echo "  LDFLAGS: $(LDFLAGS)"
	@echo ""
	GOFLAGS="$(GOFLAGS)" GOAMD64=$(GOAMD64) $(GO) build -gcflags="$(GCFLAGS)" -ldflags "$(LDFLAGS)" \
		-o $(BIN_DIR)/qdb-nats-connector ./cmd/qdb-nats-connector
	GOFLAGS="$(GOFLAGS)" GOAMD64=$(GOAMD64) $(GO) build -gcflags="$(GCFLAGS)" -ldflags "$(LDFLAGS)" \
		-o $(BIN_DIR)/qdb-data-gen ./tools/generator
	GOFLAGS="$(GOFLAGS)" GOAMD64=$(GOAMD64) $(GO) build -gcflags="$(GCFLAGS)" -ldflags "$(LDFLAGS)" \
		-o $(BIN_DIR)/qdb-data-loader ./tools/loader
	@echo ""
	@echo "Build complete. Binaries in $(BIN_DIR)/"

# Individual binary builds (using BIN variable)
# Usage: BIN=loader make build-single
build-single: $(BIN_DIR)
	@if [ -z "$(BIN)" ]; then \
		echo "Error: BIN variable not set. Usage: BIN=loader make build-single"; \
		exit 1; \
	fi
	@case "$(BIN)" in \
		connector) PKG=./cmd/qdb-nats-connector; OUT=qdb-nats-connector ;; \
		generator) PKG=./tools/generator; OUT=qdb-data-gen ;; \
		loader) PKG=./tools/loader; OUT=qdb-data-loader ;; \
		*) echo "Error: Unknown binary '$(BIN)'. Valid: connector, generator, loader"; exit 1 ;; \
	esac; \
	echo "Building $$OUT in $(BUILD_MODE) mode..."; \
	GOFLAGS="$(GOFLAGS)" GOAMD64=$(GOAMD64) $(GO) build -gcflags="$(GCFLAGS)" -ldflags "$(LDFLAGS)" \
		-o $(BIN_DIR)/$$OUT $$PKG

# Run tests with race detector and CGO checks
test:
	GOEXPERIMENT=cgocheck2 $(GO) test -mod=vendor -race ./...

# Run golangci-lint
lint:
	golangci-lint run

# Remove build artifacts
clean:
	rm -rf $(BIN_DIR)

# Create bin directory if it doesn't exist
$(BIN_DIR):
	mkdir -p $(BIN_DIR)

# Help target
help:
	@echo "qdb-nats-connector Makefile"
	@echo ""
	@echo "Usage:"
	@echo "  make                    # Run lint, test, and build (default: release mode)"
	@echo "  make build              # Build all binaries"
	@echo "  make test               # Run tests with race detector"
	@echo "  make lint               # Run golangci-lint"
	@echo "  make clean              # Remove build artifacts"
	@echo ""
	@echo "Build modes (via BUILD_MODE variable):"
	@echo "  make build                           # Default: release (optimized with debuginfo)"
	@echo "  BUILD_MODE=debug make build          # Debug (unoptimized, for debugging)"
	@echo "  BUILD_MODE=release-min make build    # Minimal (stripped, small size)"
	@echo ""
	@echo "Building individual binaries:"
	@echo "  BIN=connector make build-single      # Build only the connector"
	@echo "  BIN=generator make build-single      # Build only the generator"
	@echo "  BIN=loader make build-single         # Build only the loader"
	@echo ""
	@echo "Version overrides:"
	@echo "  VERSION=1.2.3 make build             # Set version string"
	@echo "  GIT_SHA=abc123 make build            # Set git commit"
	@echo ""
	@echo "Current configuration:"
	@echo "  BUILD_MODE: $(BUILD_MODE)"
	@echo "  VERSION: $(VERSION)"
	@echo "  GIT_SHA: $(GIT_SHA)"