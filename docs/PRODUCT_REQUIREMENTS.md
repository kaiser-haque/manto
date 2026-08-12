# Product Requirements

## Functional requirements

### FR-01 Producer

Manto must provide a simple producer abstraction capable of publishing typed objects to Kafka.

### FR-02 Consumer

Manto must provide an annotation-driven consumer model.

### FR-03 Serialization

JSON serialization/deserialization must be supported through Jackson.

### FR-04 Metadata

Events must support event ID, type, version, source, timestamp, and correlation ID.

### FR-05 Retry

Failed processing must support configurable retry attempts and backoff.

### FR-06 DLT

Messages that exhaust retry attempts must be routed to a dead-letter topic with useful diagnostic metadata.

### FR-07 Idempotency

Manto must expose an idempotency abstraction and provide an in-memory implementation in v1.0.

### FR-08 Observability

Manto must expose Micrometer metrics for major message lifecycle operations.

### FR-09 Spring Boot

Manto must auto-configure through a Spring Boot starter.

### FR-10 Testing

Core Kafka behavior must be verified against real Kafka using Testcontainers.

## Non-functional requirements

- Java 21 compatible.
- Clear public APIs.
- Minimal dependency footprint.
- No credentials in source.
- No sensitive payload logging by default.
- Reasonable unit and integration test coverage.
- Reproducible CI builds.
- Publishable Maven metadata, sources, and Javadocs.
