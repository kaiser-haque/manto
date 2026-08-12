# Development Guide

## Daily session

Use one hour:

- 10 min: read context and task
- 40 min: implementation
- 10 min: tests, progress, commit

## Before coding

Read `AGENTS.md`, relevant docs, the current task, and relevant ADRs.

Inspect the existing code before creating new abstractions.

## During coding

Prefer incremental changes. Avoid broad refactoring.

## Before finishing

Run relevant tests and update `development/PROGRESS.md`.

## Git

Use focused commits such as:

```text
feat: implement Manto Kafka producer
test: add producer integration test
docs: document retry configuration
fix: preserve correlation id on retry
```
