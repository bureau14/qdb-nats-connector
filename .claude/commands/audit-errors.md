# Go Error Handling Audit

You are an expert at structured error handling with deep understanding of the project's custom error system. Your task is to audit and improve error handling following these strict rules:

* **Error creation**: ALL errors MUST use constructor functions from `internal/errors/errors.go`:
  - Use existing constructors: `NewNoTopicProvidedError()`, `NewInvalidConfigError()`, etc.
  - Never create errors with `fmt.Errorf()`, `errors.New()`, or string literals
  - If no suitable constructor exists, add one to `internal/errors/errors.go` first
  - Every constructor must follow the pattern: `NewXxxError(component string, ...params)`

* **Error wrapping**: Use the package's wrapping mechanism:
  - For errors with causes: Pass the underlying error to constructors that accept it
  - Example: `NewConnectionFailedError("sink", endpoint, err)`
  - Never use `fmt.Errorf("%w", err)` for wrapping
  - Preserve error chains for debugging with `errors.As()` compatibility

* **Component identification**: Always specify the component parameter:
  - Use module names: "source", "parser", "sink", "connector"
  - Be consistent with component naming across the module
  - Component helps identify error origin in distributed logs

* **Error codes**: Use appropriate ErrorCode constants from `internal/errors/errors.go`:
  - Review existing constants (e.g., `ErrCodeConnectionFailed`, `ErrCodeParsingFailed`)
  - Choose the most specific error code that matches the failure condition
  - If no suitable code exists, add a new ErrorCode constant following the pattern:
    ```go
    // ErrCodeDescriptiveName indicates specific failure condition
    ErrCodeDescriptiveName ErrorCode = iota + 1000
    ```
  - Use `ErrCodeUnexpectedError` only for truly unhandled conditions

* **Metadata usage**: Add debugging context to error metadata:
  - Include relevant IDs, endpoints, topics in metadata map
  - Example: `Metadata: map[string]interface{}{"endpoint": addr}`
  - Keep metadata focused on debugging, not business logic

* **Error checking patterns**:
  - Use `errors.As()` to check error types:
    ```go
    var connErr *errors.ConnectorError
    if errors.As(err, &connErr) && connErr.Code == errors.ErrCodeConnectionFailed {
        // handle connection failure
    }
    ```
  - Never use type switches or string comparison on errors
  - Check most specific error types first in error handling chains

* **Testing requirements for new error constructors**:
  - Add a test case to `TestErrorConstructors` in `errors_test.go`:
    ```go
    {
        name:         "YourErrorType",
        constructor:  func() *ConnectorError { return NewYourErrorTypeError("test", /* other params */) },
        expectedCode: ErrCodeYourErrorType,
    },
    ```
  - Ensure the test validates:
    - Correct error code assignment
    - Component field is properly set
    - Constructor accepts standard parameters
  - For errors with wrapped causes, also verify in separate tests:
    - Error message format matches `[component] message (code: N)`
    - Unwrap() returns the original error
    - Metadata fields are populated correctly

**Prohibited patterns**:
- `return fmt.Errorf("failed to connect: %w", err)`
- `return errors.New("invalid configuration")`
- `if err.Error() == "connection refused"`
- Creating errors without component identification
- Ignoring errors with `_` without explicit comment

**Required patterns**:
- `return errors.NewConnectionFailedError("source", endpoint, err)`
- `return errors.NewInvalidConfigError("parser", "batch size must be positive")`
- `var connErr *errors.ConnectorError; if errors.As(err, &connErr)`
- Adding test coverage for each new error constructor

**Self-review**: After changes, verify:
1. All errors use constructors from `internal/errors/errors.go`
2. No usage of `fmt.Errorf()` or `errors.New()` remains
3. Component names are consistent across the module
4. Error chains are preserved for debugging
5. New error types have test coverage in `TestErrorConstructors`
6. New error codes are added in sequence after existing ones

**Task**: Audit and fix error handling patterns in: $ARGUMENTS
