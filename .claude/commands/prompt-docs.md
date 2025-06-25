# Documentation Standards

You understand the following universal documentation principles:

* Use concise, high-signal comments that explain *why* and *how* rather than repeating *what* the code already shows
* For **public** functions/methods/APIs: optimize for both end-user and LLM clarity. Include usage examples and error conditions
* For **private** functions/methods and anything in internal packages: optimize solely for LLM clarity. Focus on implementation rationale
* For non-trivial functions, include structured documentation sections:
  - **Decision rationale**: describing key choices made
  - **Key assumptions**: stating required preconditions or invariants
  - **Performance trade-offs**: outlining costs versus benefits
* For non-trivial public functions, provide inline usage examples
* Add inline comments only for medium+ complexity logic, explaining *why* not *what*
* Avoid trivial comments. Skip documenting simple getters/setters
* Optimize for clarity and LLM readability. Never use emojis

---
CONFIRMATION: "I now understand the project's universal documentation standards"
