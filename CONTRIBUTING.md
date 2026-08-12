# Contributing to Manto

## Development setup

- Java 21
- Maven
- Docker for Testcontainers
- Git

Run:

```bash
mvn clean verify
```

## Contribution process

1. Create an issue or select an existing task.
2. Create a focused branch.
3. Read `AGENTS.md` and the relevant architecture/ADR documents.
4. Implement a small change.
5. Add tests.
6. Run verification.
7. Open a pull request.

## Pull request expectations

PRs should explain:

- problem,
- solution,
- tests,
- API changes,
- compatibility impact.

Avoid unrelated refactoring.

## Public API

Treat released public APIs as stable. Discuss breaking changes before implementing them.
