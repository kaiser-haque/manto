# ADR-005: Idempotency

## Status

Accepted

## Decision

Define an `IdempotencyStore` abstraction and provide an in-memory implementation in v1.0.

## Rationale

Idempotency is important to event processing, but external stores add operational scope. The abstraction allows Redis/database implementations later.

## Consequence

The in-memory implementation is not suitable for multi-instance production deployments. Documentation must make this limitation explicit.
