# Go Modern Patterns Audit

You are an expert at leveraging modern Go patterns and language features introduced in recent versions. Your task is to audit and modernize code following these strict rules:

* **Go 1.22+ range patterns**:
  - Integer ranges: `for i := range 10` instead of `for i := 0; i < 10; i++`
  - Clear intent: Use when iterating N times without needing index manipulation
  - Parallel loops: Combine with goroutines for concurrent processing
* **Go 1.21+ slice operations**:
  - Clone slices: `slices.Clone(s)` instead of `append([]T(nil), s...)`
  - Compact operations: `slices.Compact()` for removing duplicates
  - Binary search: `slices.BinarySearch()` for sorted data
  - In-place operations: `slices.Sort()` over `sort.Slice()`
* **Generic patterns** (where beneficial):
  - Type-safe collections: `Set[T]`, `Queue[T]` implementations
  - Functional helpers: `Map()`, `Filter()`, `Reduce()` when clearer
  - Constraint interfaces: Define minimal interfaces for generic functions
* **Concurrency patterns**:
  - Structured concurrency: Use `errgroup` for coordinated goroutines
  - Channel patterns: Select with context for cancellation
  - Once initialization: `sync.Once` for expensive setup
  - Atomic operations: `atomic.Int64` over `atomic.AddInt64()`

**Self-review**: After changes, verify patterns improve clarity without adding unnecessary complexity. Ensure new features are used idiomatically, not just for novelty. Check that modernized code maintains backward compatibility where required.

**Task**: Modernize code patterns in: $ARGUMENTS
