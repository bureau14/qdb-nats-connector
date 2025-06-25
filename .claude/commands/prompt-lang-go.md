# Go Core Standards
You understand the following Go fundamentals:
* Target Go ≥1.24 with all features available
* Prioritize idiomatic, performant standard library solutions
* Only recommend third-party packages with proven benefits
* Return errors as last value; avoid panic
* Handle all errors explicitly
---
CONFIRMATION: "I now understand the project's core Go standards"

# Go Documentation Standards
You understand the following Go documentation rules:
* ONLY use `//` comments, never `/* */`
* Package comments precede `package` declaration
* Exported identifiers need comments starting with the name
* Structure complex docs with `//` prefixed sections
---
CONFIRMATION: "I now understand the project's Go documentation syntax"

# Go Testing Standards
You understand the following Go testing requirements:
* Use testing package + testify (assert/require)
* Isolate all tests; zero shared state
* ALWAYS randomize test data via helpers
---
CONFIRMATION: "I now understand the project's Go testing standards"

# Go Style Standards

You understand the following Go style requirements:

* **Go ≥1.24**: Use all modern features. No backwards compatibility needed - freely break/refactor for better design
* **Stdlib first**: Maximize standard library usage. Third-party only when solving problems stdlib cannot
* **Imports**: `qdb-api-go` MUST alias as `qdb`. Never reference version in code (`qdb.Client` not `v3.Client`)
* **Performance**: Preallocate slices, minimize allocations, value receivers for small structs
* **Naming**: Interfaces end in "er", no stuttering (`user.Client` not `user.User`), acronyms caps (`userID`, `HTTPClient`)
* **Organization**: One concept per file, constants→variables→types→functions, <120 char lines

---
CONFIRMATION: "I now understand the project's Go style standards"
