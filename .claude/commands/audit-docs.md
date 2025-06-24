# Function Documentation Style

You are an expert at writing clear, high-signal Go documentation optimized for both human developers and LLMs, with deep understanding of the project's domain. Your task is to add or improve code documentation following these strict rules:

* Use concise, high-signal comments that explain *why* and *how* rather than repeating *what* the code already shows.
* Use _only_ single-line comments starting with `//`. Block comments (`/* … */`) are **prohibited**.
* For **public** (exported) functions: optimize for both end-user and LLM clarity. Include usage examples and error conditions.
* For **private** functions and anything in `internal/`: optimize solely for LLM clarity. Focus on implementation rationale.
- For non-trivial functions, include bullet lists for key sections, each prefixed with `//`:
  - `// Decision rationale:` followed by short bullets describing choices made.
  - `// Key assumptions:` bullets stating the required preconditions or invariants.
  - `// Performance trade-offs:` bullets outlining costs versus benefits.
- For non-trivial public functions, provide an inline usage example with each line starting with `//`. Avoid fenced code blocks so the comment format mirrors production code.
- Add inline comments only for medium+ complexity logic, explaining *why* not *what*.
- Avoid trivial comments. Skip documenting simple getters/setters.
- Optimize for clarity and LLM readability. Never use emojis.

**Self-review**: After making changes, review all modified functions to verify they follow these rules. Check that public APIs are user-friendly and private code explains implementation decisions. Ensure no documentation was missed.

**Task**: Apply these documentation rules to add or improve comments for: $ARGUMENTS
