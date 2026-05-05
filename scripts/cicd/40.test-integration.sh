#!/usr/bin/env bash
# Integration test stub for qdb-nats-connector.
# Invoked by .buildkite/steps/_test_integration.yml.
# Service provisioning (NATS, QuasarDB) is deferred.
# This step exists to establish the dependency graph for future fill-in.

set -eu

echo "==[ test-integration STUB ]=="
echo "TODO: provision NATS at localhost:4222"
echo "TODO: provision QuasarDB at localhost:2836"
echo "TODO: run: go test -v -tags=integration -json ./..."
exit 0
