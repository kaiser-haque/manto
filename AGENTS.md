# Manto AI Coding Agent Instructions

## Mission

Build Manto v1.0 as a small, clean, production-oriented Java/Spring Boot framework around Spring Kafka.

## Required reading order

Before changing code, read:

1. `docs/PROJECT_CONTEXT.md`
2. `docs/PRODUCT_REQUIREMENTS.md`
3. `docs/ARCHITECTURE.md`
4. `docs/API_DESIGN.md`
5. `development/CODING_STANDARDS.md`
6. `development/DAILY_WORKFLOW.md`
7. The current `tasks/DAY-XX.md`
8. Relevant ADRs in `development/decisions/`

## Rules

- Implement only the current day's scope unless a blocker requires another change.
- Do not add future features because they seem useful.
- Do not perform broad refactors unrelated to the current task.
- Prefer small, composable public APIs.
- Keep Kafka-specific code out of `manto-core`.
- Add or update tests for behavior you change.
- Prefer integration tests with Testcontainers for real Kafka behavior.
- Preserve backward compatibility once a public API is established.
- Do not expose secrets, credentials, or sensitive message contents in logs.
- Run the narrowest relevant tests first, then the full Maven verification when practical.
- Update `development/PROGRESS.md` at the end of the task.
- If an architectural decision changes, create/update an ADR.
- Stop when the acceptance criteria are met.

## Definition of done

A task is done only when:

- implementation is complete,
- tests cover the important behavior,
- existing tests still pass,
- documentation is updated where necessary,
- progress is recorded,
- and the expected commit message is documented.

## Preferred implementation style

Use Java 21, Maven, Spring Boot, Spring Kafka, JUnit 5, Mockito, Testcontainers, Jackson, and Micrometer.

Keep dependencies minimal. Favor constructor injection, immutable value objects where practical, clear interfaces, meaningful names, and explicit error handling.
