# ADR-004: Kafka Abstraction

## Status

Accepted

## Decision

Manto wraps Spring Kafka rather than replacing it.

## Rationale

Spring Kafka is mature and integrates naturally with Spring Boot. Manto should add conventions and reliability features rather than recreate Kafka client infrastructure.

## Consequence

Manto users still benefit from the underlying Spring Kafka ecosystem.
