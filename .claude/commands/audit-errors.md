# Go Error Handling Audit

You are an expert at structured error handling with deep understanding of the project's custom error system. Your task is to audit and improve error handling following these strict rules:

## Understanding Error Types

* **Discover available error constructors**:
  - ALL error constructors are defined in `internal/errors/errors.go`
  - Read the documentation for each constructor to understand its purpose
  - Each constructor's documentation explains WHEN to use it
  - Pay attention to the semantic meaning, not just the name

* **Choose error types based on operation context**:
  - Match the error to WHAT failed, not just WHERE it failed
  - Consider the operation being performed when the error occurred
  - A validation check during runtime operations uses the operation's error type
  - A validation check during startup/initialization uses configuration error type

* **Example scenarios (fictitious)**:
  ```go
  // CORRECT: Database operation failed during data persistence
  if err := db.Insert(record); err != nil {
      return errors.NewDatabaseWriteError("storage", err)
  }

  // INCORRECT: Using config error for runtime operation
  if err := db.Insert(record); err != nil {
      return errors.NewInvalidConfigError("storage", "insert failed")
  }
  ```

* **Error constructor documentation audit**:
  - Verify ALL constructors in `internal/errors/errors.go` have clear documentation
  - Documentation must explain:
    - When to use this error type
    - What parameters it expects
    - Any assumptions or constraints
  - Flag any undocumented or poorly documented constructors

## Error Creation and Wrapping

* **Error creation rules**:
  - ALL errors MUST use constructor functions from `internal/errors/errors.go`
  - Never create errors with `fmt.Errorf()`, `errors.New()`, or string literals
  - Every constructor must follow existing patterns in the file

* **Error wrapping guidelines**:
  ```go
  // CORRECT: Create descriptive error for the wrapped cause
  return errors.NewFooError("handler",
      fmt.Errorf("invalid format: expected %d, got %d", x, y))

  // CORRECT: Wrap a returned error
  if err := process(record); err != nil {
      return errors.NewFooError("handler", err)
  }

  // INCORRECT: Don't nest constructors when simple wrapping suffices
  return errors.NewFooError("handler",
      errors.NewBarError("handler", "invalid argument"))
  ```

* **Component identification**:
  - Always specify the component parameter
  - Use consistent module names throughout the codebase
  - Component helps identify error origin in distributed logs

## Adding or Removing Error Types

* **When adding new error constructors**:
  - Only add if no existing constructor semantically matches the failure
  - Document the constructor thoroughly with:
    - Decision rationale
    - When to use it
    - Key assumptions
  - Add appropriate ErrorCode constant
  - Add test coverage in `errors_test.go`

* **When removing error constructors**:
  - Identify if the constructor is truly redundant
  - Ensure no code depends on its specific error code
  - Document why it was removed in your report

## Error Checking and Testing

* **Error checking patterns**:
  ```go
  var appErr *errors.ApplicationError
  if errors.As(err, &appErr) && appErr.Code == errors.ErrCodeSomeFailure {
      // handle specific failure
  }
  ```

* **Testing requirements**:
  - Every constructor must have test coverage
  - Tests verify error codes, components, and wrapping behavior
  - New constructors require new test cases

## Common Anti-patterns

* **Using wrong error type for context**:
  ```go
  // WRONG: Startup validation != runtime validation
  func processMessage(data []byte) error {
      if len(data) == 0 {
          // This is NOT a config error - it's a runtime processing issue
          return errors.NewConfigurationError("processor", "no data")
      }
  }
  ```

* **Creating errors without constructors**:
  ```go
  // PROHIBITED
  return fmt.Errorf("operation failed: %w", err)
  return errors.New("something went wrong")
  ```

## Audit Output Requirements

After completing the audit, provide:

1. **Summary of changes made**:
   - Files modified with specific line references
   - Error patterns corrected

2. **New error constructors added** (if any):
   - Constructor name and signature
   - Rationale: Why existing constructors didn't suffice
   - Example usage scenario

3. **Error constructors removed** (if any):
   - Constructor name
   - Rationale: Why it was redundant or incorrect
   - How its use cases are now handled

4. **Documentation improvements**:
   - Constructors that lacked proper documentation
   - Documentation added or improved

5. **Semantic corrections**:
   - Cases where error type didn't match operation context
   - Explanation of why the change improves error semantics

**Self-review checklist**:
1. All errors use appropriate constructors from `internal/errors/errors.go`
2. Error types match the operation context semantically
3. All constructors are properly documented
4. No usage of `fmt.Errorf()` or `errors.New()` for primary errors
5. Component names are consistent
6. Test coverage exists for all constructors

**Task**: Audit and fix error handling patterns in: $ARGUMENTS
