.PHONY: all build build-tools test lint clean

# Default target
all: lint test build

# Build the main qdb-nats-connector binary
build:
	go build -o bin/qdb-nats-connector ./cmd/qdb-nats-connector

# Build all tools
build-tools:
	go build -o bin/qdb-data-gen ./tools/generator

# Run tests with race detector
test:
	GOEXPERIMENT=cgocheck2 go test -race ./...

# Run golangci-lint
lint:
	golangci-lint run

# Remove bin directory
clean:
	rm -rf bin/

# Create bin directory if it doesn't exist
bin:
	mkdir -p bin
