# ADR: Golden Data Implementation Approach

## Status
Accepted - 2025-01-23

## Context
Legacy monolithic example scripts were unmaintainable and hard to test. Each script contained duplicated code for setup, data generation, verification. No regression testing capability existed.

## Decision
We will implement a modular action-based system orchestrated by Make.

## Consequences
**Benefits:**
- Parallel execution reduces test time from 15min to 3min
- Common infrastructure eliminates duplication
- Golden data enables regression detection

**Tradeoffs:**
- Make dependency required
- More files to maintain
- Learning curve for action patterns

## Implementation
1. Extract common functions to `common.sh` and `test-common.sh`
2. Refactor scripts into discrete actions: setup, generate, verify, export
3. Use Makefile for workflow orchestration and dependency management
4. Implement row count detection via qdbsh queries
5. Store golden data as CSV for human-readable diffs

## Technical Details
- **Actions**: Self-contained scripts performing one task
- **Orchestration**: Make handles dependencies and parallel execution
- **Row Detection**: `SELECT COUNT(*) FROM tables()` pattern
- **Export Format**: CSV with headers for easy comparison

## Validation
Fixed 3 critical issues during implementation:
1. CSV header mismatch in exports
2. Row count detection logic
3. Consistent error handling patterns

Result: All examples pass golden data verification with deterministic outputs.