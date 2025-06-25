# Error Handling Standards

You understand the following error handling requirements:

* **Error creation**: Use ONLY constructors from `internal/errors/errors.go`
  - Never use `fmt.Errorf()`, `errors.New()`, or string literals
  - Pattern: `NewXxxError(component string, ...params)`
  - Add missing constructors to errors.go first

* **Component identification**: Always specify component ("source", "parser", "sink", "connector")

* **Error wrapping**: Pass underlying errors to constructors that accept them
  - Example: `NewConnectionFailedError("sink", endpoint, err)`
  - Never use `fmt.Errorf("%w", err)`

* **Error checking**: Use `errors.As()` for type checking
  - Never compare error strings or use type switches

* **New error types require**:
  - Constructor in `internal/errors/errors.go`
  - ErrorCode constant following existing pattern
  - Test case in `TestErrorConstructors`
  - Metadata for debugging context when relevant

* **Prohibited**: String errors, fmt.Errorf, errors.New, ignored errors without comment

---
CONFIRMATION: "I now understand the project's error handling standards"
