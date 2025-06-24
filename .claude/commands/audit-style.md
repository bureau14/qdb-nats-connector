# Go Code Style Audit

You are an expert at modern, idiomatic Go code with deep knowledge of performance patterns and standard library usage. Your task is to audit and improve code style following these strict rules:

* **Target version**: Go ≥1.24 - utilize all modern language features without compatibility concerns
* **Backwards compatibility**: NOT required - we are the only users of this application:
  - Freely refactor APIs, types, and function signatures for clarity
  - Rename poorly named exports without deprecation cycles
  - Remove unused code without preserving it
  - Change package structures as needed for better organization
  - Break changes are acceptable in favor of better design
* **Standard library first**:
  - Maximize use of stdlib packages for common operations
  - Leverage new stdlib additions (e.g., `log/slog`, `errors.Join`)
* **Third-party packages**: Only when providing measurable benefits:
  - Must solve problems not addressed by stdlib
  - Document the specific advantage in comments
  - Examples: `testify` for assertions, `rapid` for property based testing
  - Avoid wrappers that only add syntactic sugar
* **Import aliases**: ALWAYS use consistent aliases for imports:
  - `qdb-api-go` MUST be aliased as `qdb`: `import qdb "github.com/bureau14/qdb-api-go/v3"`
  - Never use version suffixes in code: use `qdb.Client`, not `v3.Client`
  - Maintain consistent aliases project-wide for all versioned imports
  - Unaliased versioned imports are forbidden
* **Performance patterns**:
  - Preallocate slices when size is known: `make([]T, 0, cap)`
  - Minimize allocations in hot paths
  - Use value receivers for small structs
* **Code organization**:
  - One concept per file, descriptive names
  - Group related types/functions logically
  - Constants before variables before types before functions
  - Keep line length under 120 chars for readability
* **Naming conventions**:
  - Interfaces: verb + "er" suffix (Reader, Subscriber)
  - Avoid stuttering: `user.User` → `user.Client`
  - Acronyms in caps: `userID` not `userId`, `HTTPClient` not `HttpClient`
  - Package names: singular, lowercase, no underscores

**Self-review**: After changes, verify code reads naturally and follows Go idioms. Check for unnecessary complexity that could be simplified with standard library features. Ensure consistent style across the modified files. Don't hesitate to make breaking changes if they improve the codebase.

**Task**: Audit and improve code style in: $ARGUMENTS
