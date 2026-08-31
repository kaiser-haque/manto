# Manto Examples

| Example | Description | Key features |
|---|---|---|
| [order-payment](order-payment/) | Order Service → `order-events` → Payment Service → `payment-events` | producer, `@MantoListener`, metadata/correlation, retry with backoff, DLT, idempotency |

Run `mvn install -DskipTests` at the repository root first — examples depend on `0.1.0-SNAPSHOT`.

Each example is a standalone Spring Boot application with its own `pom.xml` and `README.md`.
