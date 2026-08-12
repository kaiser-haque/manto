# Manto

Manto is a Java/Spring Boot developer-experience and reliability framework for Kafka-based event streaming.

## v1.0 goal

Provide a production-oriented abstraction over Spring Kafka with:

- Producer API
- `@MantoListener`
- JSON event serialization
- standardized event metadata and correlation IDs
- configurable retry and exponential backoff
- dead-letter topic handling
- basic idempotency abstraction with an in-memory implementation (single-instance only in v1.0; see ADR-005)
- Micrometer metrics
- Spring Boot auto-configuration and starter
- Testcontainers integration tests
- Maven Central publication

## Non-goals for v1.0

Do not implement Avro, Protobuf, Schema Registry, Kafka Streams, an admin UI, CLI, event replay, or Redis idempotency.

## Development model

Manto is being built in 30 one-hour daily sessions. Read `AGENTS.md` first, then the applicable file in `tasks/`.

See `docs/PROJECT_CONTEXT.md` for the complete product context.
