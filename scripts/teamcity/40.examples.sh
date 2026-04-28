#!/usr/bin/env bash

set -eu

SCRIPT_DIR="$(cd "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
source "$SCRIPT_DIR/common.sh"

echo "Running qdb-nats-connector example tests..."
echo "==========================================="
echo ""
echo "Prerequisites:"
echo "- NATS server at localhost:4222"
echo "- QuasarDB cluster at localhost:2836"
echo ""

# Allow environment overrides
export NATS_ENDPOINT=${NATS_ENDPOINT:-"nats://localhost:4222"}
export QDB_CLUSTER_URI=${QDB_CLUSTER_URI:-"qdb://127.0.0.1:2836"}
BUILD_TYPE=${BUILD_TYPE:-commit}

echo "Configuration:"
echo "- NATS_ENDPOINT: $NATS_ENDPOINT"
echo "- QDB_CLUSTER_URI: $QDB_CLUSTER_URI"
echo "- BUILD_TYPE: $BUILD_TYPE"
echo ""

# Determine test target based on BUILD_TYPE
case "$BUILD_TYPE" in
    commit)
        TEST_TARGET="test-quick"
        echo "Running quick tests (10k records) for commit builds..."
        ;;
    nightly|release)
        TEST_TARGET="test-full"
        echo "Running full tests (1M records) for $BUILD_TYPE builds..."
        ;;
    *)
        echo "Warning: Unknown BUILD_TYPE '$BUILD_TYPE', defaulting to quick tests"
        TEST_TARGET="test-quick"
        ;;
esac

# Run example tests
(
    cd "$SCRIPT_DIR/../../examples"
    
    # TeamCity progress reporting
    if [[ -n "${TEAMCITY_VERSION:-}" ]]; then
        echo "##teamcity[testSuiteStarted name='examples.$TEST_TARGET']"
    fi
    
    # Run tests with proper error handling
    if make "$TEST_TARGET"; then
        echo "All example tests passed successfully!"
        EXIT_CODE=0
    else
        echo "Example tests failed!"
        EXIT_CODE=1
        
        # Collect artifacts on failure
        if [[ -d exports ]]; then
            echo "Collecting export artifacts..."
            tar -czf exports-failure.tar.gz exports/
        fi
        
        # Collect log files
        if ls *.log 1> /dev/null 2>&1; then
            echo "Collecting log artifacts..."
            tar -czf logs-failure.tar.gz *.log
        fi
    fi
    
    # TeamCity progress reporting
    if [[ -n "${TEAMCITY_VERSION:-}" ]]; then
        echo "##teamcity[testSuiteFinished name='examples.$TEST_TARGET']"
    fi
    
    exit $EXIT_CODE
)