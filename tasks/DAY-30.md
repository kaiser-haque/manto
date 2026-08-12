# Day 30 — Manto 1.0.0

## Objective

Publish the stable release, tag v1.0.0, and verify Maven Central consumption.

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

- release

## Implementation guidance

Implement the smallest production-quality solution that satisfies today's objective. Preserve existing architecture and public API decisions.

Do not implement features assigned to future days.

## Acceptance criteria

- [ ] All v1.0 scope features are verified.
- [ ] mvn clean verify passes.
- [ ] v1.0.0 artifacts are published successfully.
- [ ] A clean external project resolves the released starter from Maven Central.
- [ ] Git tag v1.0.0 exists.
- [ ] README and CHANGELOG reflect the stable release.

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
release: publish Manto 1.0.0
```

## Stop condition

When the acceptance criteria are satisfied and tests are passing, stop. Do not expand scope.
