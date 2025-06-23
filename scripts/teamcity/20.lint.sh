#!/usr/bin/env bash

set -eu

SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
source "$SCRIPT_DIR/common.sh"

# Function to install linting tools if not available
install_linter() {
    local tool=$1
    local package=$2

    if ! command -v "$tool" &> /dev/null; then
        echo "Installing $tool..."
        ${GO} install "$package"
    else
        echo "$tool is already available"
    fi
}

# Install required linting tools
tc_open_block "Install Linting Tools"
install_linter "golangci-lint" "github.com/golangci/golangci-lint/cmd/golangci-lint@latest"
install_linter "staticcheck" "honnef.co/go/tools/cmd/staticcheck@latest"
install_linter "ineffassign" "github.com/gordonklaus/ineffassign@latest"
tc_close_block "Install Linting Tools"

# Run linting checks
(
    pushd ${BASE_DIR}

    # Go fmt check
    tc_open_block "Go Format Check"
    if [ "$(${GO} fmt ./... | wc -l)" -gt 0 ]; then
        echo "Code is not properly formatted. Run 'go fmt ./...' to fix."
        ${GO} fmt ./...
        tc_build_problem "Code formatting issues found"
        exit 1
    else
        echo "Code formatting is correct"
    fi
    tc_close_block "Go Format Check"

    # Go vet check
    tc_open_block "Go Vet Check"
    if ! ${GO} vet ./...; then
        tc_build_problem "Go vet found issues"
        exit 1
    else
        echo "Go vet passed"
    fi
    tc_close_block "Go Vet Check"

    # Staticcheck
    tc_open_block "Staticcheck"
    if ! staticcheck ./...; then
        tc_build_problem "Staticcheck found issues"
        exit 1
    else
        echo "Staticcheck passed"
    fi
    tc_close_block "Staticcheck"

    # Inefficient assignment check
    tc_open_block "Ineffassign Check"
    if ! ineffassign ./...; then
        tc_build_problem "Ineffassign found issues"
        exit 1
    else
        echo "Ineffassign check passed"
    fi
    tc_close_block "Ineffassign Check"

    # golangci-lint (comprehensive linting)
    tc_open_block "GolangCI-Lint"
    if ! golangci-lint run ./...; then
        tc_build_problem "GolangCI-Lint found issues"
        exit 1
    else
        echo "GolangCI-Lint passed"
    fi
    tc_close_block "GolangCI-Lint"

    popd
)
