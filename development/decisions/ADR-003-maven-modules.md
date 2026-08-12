# ADR-003: Maven Modules

## Status

Accepted

## Decision

Keep Kafka-specific implementation out of `manto-core`.

## Rationale

Core abstractions should remain portable and easier to test.

## Consequence

Kafka behavior belongs in `manto-kafka`, while Spring Boot wiring belongs in auto-configuration.
