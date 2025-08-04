.PHONY: build clean test

# Build the loader binary
build:
	direnv exec . go build -o ../../bin/qdb-data-load ./

# Remove the binary
clean:
	rm -f ../../bin/qdb-data-load

# Run tests
test:
	direnv exec . go test ./...