# AI Agent Guide

Manto is developed with an AI coding agent.

## Agent workflow

1. Read `AGENTS.md`.
2. Read project context and architecture.
3. Read the current task.
4. Inspect current source.
5. Plan the smallest implementation.
6. Implement.
7. Test.
8. Review the diff.
9. Update progress.
10. Stop.

## Do not

- redesign the framework without an ADR,
- add future roadmap features,
- rewrite unrelated code,
- introduce dependencies without justification,
- remove tests just to make the build pass,
- hide compiler/test failures,
- claim a task is complete without verification.

## When uncertain

Prefer documenting the uncertainty and selecting the smallest reversible implementation.

If a decision materially changes public API or module boundaries, create an ADR rather than silently deciding in code.
