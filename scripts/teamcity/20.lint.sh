#!/usr/bin/env bash

set -e

SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
source "$SCRIPT_DIR/common.sh"

# Initialize error tracking
MISSING_TOOLS=()
LINT_ERRORS=()
OVERALL_EXIT_CODE=0

# Function to check if tool is available
check_tool() {
    local tool=$1
    if ! command -v "$tool" &> /dev/null; then
        echo "$tool is not installed, skipping..."
        MISSING_TOOLS+=("$tool")
        return 1
    fi
    return 0
}

# Run linting checks
(
    pushd ${BASE_DIR}

    # Go fmt check
    tc_open_block "Go Format Check"
    if [ "$(${GO} fmt ./... | wc -l)" -gt 0 ]; then
        echo "Code is not properly formatted. Run 'go fmt ./...' to fix."
        ${GO} fmt ./...
        LINT_ERRORS+=("Code formatting issues found")
        OVERALL_EXIT_CODE=1
    else
        echo "Code formatting is correct"
    fi
    tc_close_block "Go Format Check"

    # Go vet check
    tc_open_block "Go Vet Check"
    if ! ${GO} vet ./...; then
        echo "Go vet found issues"
        LINT_ERRORS+=("Go vet found issues")
        OVERALL_EXIT_CODE=1
    else
        echo "Go vet passed"
    fi
    tc_close_block "Go Vet Check"

    # Staticcheck
    tc_open_block "Staticcheck"
    if check_tool "staticcheck"; then
        if ! staticcheck ./...; then
            echo "Staticcheck found issues"
            LINT_ERRORS+=("Staticcheck found issues")
            OVERALL_EXIT_CODE=1
        else
            echo "Staticcheck passed"
        fi
    fi
    tc_close_block "Staticcheck"

    # Inefficient assignment check
    tc_open_block "Ineffassign Check"
    if check_tool "ineffassign"; then
        if ! ineffassign ./...; then
            echo "Ineffassign found issues"
            LINT_ERRORS+=("Ineffassign found issues")
            OVERALL_EXIT_CODE=1
        else
            echo "Ineffassign check passed"
        fi
    fi
    tc_close_block "Ineffassign Check"

    # golangci-lint (comprehensive linting)
    tc_open_block "GolangCI-Lint"
    if check_tool "golangci-lint"; then
        if ! golangci-lint run ./...; then
            echo "GolangCI-Lint found issues"
            LINT_ERRORS+=("GolangCI-Lint found issues")
            OVERALL_EXIT_CODE=1
        else
            echo "GolangCI-Lint passed"
        fi
    fi
    tc_close_block "GolangCI-Lint"

    popd
)

# Final error reporting
tc_open_block "Lint Summary"

###
# XXX(leon): temporarily disable linting tools as we wait for all teamcity
#            agents to have it installed by default.
###
# if [ ${#MISSING_TOOLS[@]} -gt 0 ]; then
#     echo "Missing tools: ${MISSING_TOOLS[*]}"
#     tc_build_problem "Missing linting tools: ${MISSING_TOOLS[*]}"
#     OVERALL_EXIT_CODE=1
# fi

if [ ${#LINT_ERRORS[@]} -gt 0 ]; then
    echo "Linting errors found:"
    for error in "${LINT_ERRORS[@]}"; do
        echo "  - $error"
    done
    tc_build_problem "Multiple linting issues found: ${LINT_ERRORS[*]}"
fi

if [ $OVERALL_EXIT_CODE -eq 0 ]; then
    echo "All linting checks passed successfully"
fi

tc_close_block "Lint Summary"

exit $OVERALL_EXIT_CODE
