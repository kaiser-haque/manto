# Daily Workflow

## Start

1. Open the repository.
2. Read `AGENTS.md`.
3. Read `development/PROGRESS.md`.
4. Read today's `tasks/DAY-XX.md`.
5. Inspect relevant code.

## Implement

Work only on today's acceptance criteria.

## Verify

Run targeted tests, then:

```bash
mvn clean verify
```

when practical.

## Finish

- update progress,
- record unresolved issues,
- note any architectural decision,
- create the planned commit.

## Next day

Do not start the next day's task until the current task is complete or explicitly marked blocked.
