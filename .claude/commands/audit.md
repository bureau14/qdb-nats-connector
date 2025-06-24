# Comprehensive Go Code Audit

You are an expert at conducting thorough code audits across multiple dimensions. Your task is to systematically review and improve code by examining ALL of the following aspects. Create a checklist for each file to ensure nothing is missed.

## AUDIT WORKFLOW

For each file in $ARGUMENTS:

1. Create a file-specific checklist:

   File: path/to/file.go
   [ ] Documentation audit complete
   [ ] Code style audit complete
   [ ] Modern patterns audit complete
   [ ] Error handling audit complete (CRITICAL)

2. Work through each audit dimension systematically
3. Document all findings before making changes
4. Apply fixes in order of criticality: Errors → Style → Patterns → Documentation

## 1. ERROR HANDLING AUDIT

Why first: Broken error handling can cause production failures. This is the most critical aspect.

* Error creation: ALL errors MUST use constructor functions from internal/errors/errors.go:
  - Use existing constructors: NewNoTopicProvidedError(), NewInvalidConfigError(), etc.
  - Never create errors with fmt.Errorf(), errors.New(), or string literals
  - If no suitable constructor exists, add one to internal/errors/errors.go first
  - Every constructor must follow pattern: NewXxxError(component string, ...params)

* Error wrapping: Use the package's wrapping mechanism:
  - For errors with causes: Pass underlying error to constructors
  - Example: NewConnectionFailedError("sink", endpoint, err)
  - Never use fmt.Errorf("%w", err) for wrapping

* Component identification: Always specify the component parameter:
  - Use module names: "source", "parser", "sink", "connector"
  - Be consistent with component naming

* Error checking patterns:
  - Use errors.As() to check error types
  - Never use type switches or string comparison on errors

* Testing requirements: Every new error constructor needs test in TestErrorConstructors

Prohibited: fmt.Errorf(), errors.New(), string error comparisons, ignored errors without comment

## 2. CODE STYLE AUDIT

Focus: Modernize code structure and naming for clarity.

* Target version: Go ≥1.24 - use all modern features
* Backwards compatibility: NOT required - freely refactor/rename/restructure
* Standard library first: Maximize stdlib usage
* Third-party packages: Only with documented benefits
* Performance patterns:
  - Preallocate slices: make([]T, 0, cap)
  - Minimize allocations in hot paths
  - Value receivers for small structs
* Organization:
  - One concept per file
  - Constants → variables → types → functions
  - Line length < 120 chars
* Naming:
  - Interfaces: verb + "er" (Reader, Subscriber)
  - No stuttering: user.User → user.Client
  - Acronyms: userID, HTTPClient

## 3. MODERN PATTERNS AUDIT

Focus: Leverage new Go features where they improve clarity.

* Go 1.22+ ranges: for i := range 10 for counting loops
* Go 1.21+ slices: Use slices.Clone(), slices.Sort(), etc.
* Generics: Only where type safety improves clarity
* Concurrency: errgroup for coordinated goroutines, sync.Once for init

Important: Don't modernize just for novelty - must improve readability

## 4. DOCUMENTATION AUDIT

Focus: High-signal comments for humans and LLMs.

* Format: Only // comments, never /* */
* Content: Explain why and how, not what
* Public functions: User-focused with examples and error conditions
* Private functions: Implementation rationale for maintainers
* Structure for complex functions:
  // Decision rationale:
  // - Chose X because Y
  // Key assumptions:
  // - Input is always validated
  // Performance trade-offs:
  // - Caches results trading memory for speed
* Skip: Trivial getters/setters documentation

## EXECUTION CHECKLIST

When auditing each file:
1. ✓ First pass: Find all error handling issues
2. ✓ Second pass: Identify style improvements
3. ✓ Third pass: Find modernization opportunities
4. ✓ Fourth pass: Document missing/poor documentation
5. ✓ Fix issues in priority order
6. ✓ Re-verify error handling after all changes

## FINAL REVIEW

Before marking complete, verify:
- [ ] Zero fmt.Errorf() or errors.New() usage
- [ ] All errors use project error constructors
- [ ] Code follows modern Go idioms
- [ ] Documentation explains why, not what
- [ ] No backwards compatibility preserved unnecessarily

Task: Perform comprehensive audit (errors → style → patterns → docs) on: $ARGUMENTS
