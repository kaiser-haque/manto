# Day 18 — Exception classification

## Objective

Allow permanent/non-retryable exceptions to bypass retries.

## Timebox

**1 hour total**

- 10 minutes: read context and inspect code
- 40 minutes: implement
- 10 minutes: test, update progress, review diff

## Read first

- `AGENTS.md`
- `docs/PROJECT_CONTEXT.md`
- `docs/PRODUCT_REQUIREMENTS.md`
- `docs/ARCHITECTURE.md`
- `docs/API_DESIGN.md`
- `development/CODING_STANDARDS.md`
- `development/PROGRESS.md`

## Relevant areas

- manto-kafka

## Implementation guidance

Implement the smallest production-quality solution that satisfies today's objective. Preserve existing architecture and public API decisions.

Do not implement features assigned to future days.

## Acceptance criteria

- [ ] The day's objective is implemented.
- [ ] Relevant tests are added or updated.
- [ ] Existing tests are not broken.
- [ ] No future-day feature is implemented.
- [ ] development/PROGRESS.md is updated.

## Testing

Add focused tests for new behavior. Use Testcontainers when the behavior depends on actual Kafka semantics.

## Documentation

Update relevant documentation if the implementation changes public behavior or configuration.

## Progress

Update `development/PROGRESS.md` with:

- completed work,
- current day,
- tests run,
- known issues,
- next task.

## Commit

```text
feat: classify retryable exceptions
```

## Stop condition

When the acceptance criteria are satisfied and tests are passing, stop. Do not expand scope.
