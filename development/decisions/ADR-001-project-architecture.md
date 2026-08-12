# ADR-001: Project Architecture

## Status

Accepted

## Decision

Use a Maven multi-module architecture:

- manto-core
- manto-kafka
- manto-spring-boot-autoconfigure
- manto-spring-boot-starter
- manto-test

## Rationale

This separates framework abstractions from Kafka and Spring Boot implementation details and keeps future integrations possible.

## Consequences

More modules require more build configuration, but the separation provides clearer public boundaries.
