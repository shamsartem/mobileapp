# Repository rules

For every ticket, run a reuse pass twice:

1. Before designing or coding, inspect the existing code and choose the smallest change that reuses its modules, patterns, data, and dependencies.
2. Before resolving the ticket, look again for code that can be deleted, combined, or replaced with an existing implementation. Confirm that the result adds the minimum code and avoids parallel sources of truth.

Prefer a small change at an existing seam over a new abstraction. Extract shared code only when two real callers need the same behavior. Preserve upstream code where selecting a fork-owned implementation is enough.
