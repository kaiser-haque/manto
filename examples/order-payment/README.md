# Order-Payment Example

Minimal, runnable Spring Boot 3.5 application demonstrating **Manto** end-to-end.

```
                  order-events                     payment-events
OrderService  ─────────────►  PaymentHandler  ─────────────────►  (downstream)
  (REST)        @MantoListener   |  retry + DLT + idempotency
                                 └─► order-events.DLT (on permanent failure / exhausted retries)
```

## What it shows

| Manto feature | Where in this example |
|---|---|
| **Producer** | `src/main/java/com/example/orderpayment/order/OrderService.java:37` — `producer.publish("order-events", event)` <br> `src/main/java/com/example/orderpayment/payment/PaymentService.java:37` — publish to `payment-events` with correlation propagation via `MantoKafkaProducer.publish(topic, event, correlationId)` |
| **@MantoListener** | `src/main/java/com/example/orderpayment/payment/PaymentHandler.java:40` — `@MantoListener(topic="order-events", groupId="payment-service")` <br> `src/main/java/com/example/orderpayment/payment/PaymentDltHandler.java:28` — DLT observer |
| **Metadata / correlation** | Manto injects `Manto-Event-Id`, `Manto-Event-Type`, `Manto-Event-Version`, `Manto-Correlation-Id`, `Manto-Source` on every `publish` (`MantoKafkaProducer.java:97`). <br> Consumer side: `CorrelationIdContext.get()` in `PaymentHandler.java:44` and `PaymentService.java:32`. <br> Downstream propagation: `PaymentService.java:37` forwards the upstream correlation ID. |
| **Retry** | `src/main/resources/application.yml:9` — `manto.retry.*` with exponential backoff (`initial-delay=1s`, `multiplier=2.0`, `max-delay=10s`). <br> Transient `RuntimeException` in `PaymentHandler.java:67` is retried up to `max-attempts=3` (2 retries). |
| **DLT** | `application.yml:19` — `manto.dlt.enabled=true`, `topic-suffix=.DLT`. <br> Exhausted retries or non-retryable `IllegalArgumentException` (`PaymentHandler.java:59`) route to `order-events.DLT` with headers `Manto-DLT-Original-Topic`, `Manto-DLT-Exception-Class`, `Manto-DLT-Retry-Count`, etc. <br> Observed by `PaymentDltHandler.java:28`. |
| **Idempotency** | `PaymentHandler.java:51` — `IdempotencyStore.isProcessed(correlationId)` / `markProcessed` guards duplicate redelivery (at-least-once → effectively-once for single instance). |
| **Metrics** | `manto.observability.enabled=true` exposes `manto.messages.*` and `manto.processing.duration` via Micrometer (see `docs/OBSERVABILITY.md`). |

Events are kept trivial (`orderId + amount` / `orderId + status + processedAt`) so the focus stays on framework usage, not business logic.

## Prerequisites

- Java 21, Maven 3.9+
- Docker (for Kafka) — or any broker at `localhost:9092`

Build Manto locally once (example depends on `0.1.0-SNAPSHOT` from your local `~/.m2`):

```bash
mvn install -DskipTests
```

Start Kafka (KRaft, no ZooKeeper):

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.1
```

## Run

```bash
cd examples/order-payment
mvn spring-boot:run
```

In another terminal, place orders:

```bash
# happy path — consumed, payment published to payment-events
curl -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"orderId":"order-123","amount":5000}'

# transient failure — retried 3× with backoff, then routed to order-events.DLT
curl -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"orderId":"order-999","amount":999}'

# permanent failure — non-retryable, routed straight to DLT
curl -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"orderId":"order-bad","amount":0}'
```

### Observe logs

```
Handling orderId=order-123 amount=5000 correlationId=550e8400-...
Payment completed orderId=order-123 correlationId=550e8400-...

Handling orderId=order-999 ...  -> RuntimeException -> retry 1, 2, 3 -> DLT
DLT received orderId=order-999 amount=999 correlationId=... - requires manual investigation
```

### Inspect topics

```bash
# payment success (headers include propagated Manto-Correlation-Id)
kafka-console-consumer --bootstrap-server localhost:9092 --topic payment-events --from-beginning \
  --property print.headers=true

# dead letters (diagnostic headers + original Manto headers)
kafka-console-consumer --bootstrap-server localhost:9092 --topic order-events.DLT --from-beginning \
  --property print.headers=true
```

Headers on `payment-events` include the propagated `Manto-Correlation-Id` from the original order.
Headers on `order-events.DLT` include `Manto-DLT-Original-Topic`, `Manto-DLT-Exception-Class`, `Manto-DLT-Retry-Count` (value `2` for `max-attempts=3`), `Manto-DLT-Failure-Timestamp`, `Manto-DLT-Trace-Id`, plus the copied `Manto-Event-Id` / `Manto-Correlation-Id`.

## Configuration

All Manto behaviour is driven by `src/main/resources/application.yml` (full reference: `docs/CONFIGURATION.md`):

```yaml
manto:
  kafka:
    bootstrap-servers: localhost:9092
  retry:
    enabled: true
    max-attempts: 3
    backoff:
      initial-delay: 1000      # parsed as milliseconds; use 1s for clarity
      multiplier: 2.0
      max-delay: 10000
  dlt:
    enabled: true
    topic-suffix: .DLT
  idempotency:
    enabled: true
  observability:
    enabled: true
```

`max-attempts` includes the initial attempt. With `3`, a retryable failure is attempted 1 + 2 retries before DLT.

## Key files

| File | Purpose |
|---|---|
| `src/main/java/com/example/orderpayment/OrderPaymentApplication.java` | `@SpringBootApplication` entry point; flow diagram in Javadoc |
| `src/main/java/com/example/orderpayment/order/OrderCreatedEvent.java` | `record(orderId, amount)` published as JSON |
| `src/main/java/com/example/orderpayment/order/OrderService.java` | Constructor-injected `MantoProducer`, one-line publish |
| `src/main/java/com/example/orderpayment/order/OrderController.java` | `POST /orders` → `OrderService` |
| `src/main/java/com/example/orderpayment/payment/PaymentHandler.java` | `@MantoListener` + `CorrelationIdContext` + idempotency + exception classification demo |
| `src/main/java/com/example/orderpayment/payment/PaymentService.java` | Downstream publish with correlation propagation (casts to `MantoKafkaProducer` when available) |
| `src/main/java/com/example/orderpayment/payment/PaymentDltHandler.java` | Logs poison messages; no retry/idempotency here |
| `src/main/java/com/example/orderpayment/payment/PaymentCompletedEvent.java` | `record(orderId, status, processedAt)` |
| `pom.xml` | Standalone Spring Boot 3.5.16 / Java 21 project depending on `manto-spring-boot-starter:${manto.version}` |

## Notes

- **No business complexity** — events are `orderId + amount`; payment is log + publish; no DB, no over-engineering.
- **In-memory idempotency** is single-instance only. For multi-instance production, provide your own `IdempotencyStore` bean (e.g. Redis) — Manto auto-configures via `@ConditionalOnMissingBean` (`MantoAutoConfiguration.java:166`). See `README.md#idempotency` and `docs/ERROR_HANDLING.md`.
- **Retry / DLT wiring** is via `MantoAutoConfiguration.kafkaListenerContainerFactory` (`MantoAutoConfiguration.java:179`): retry uses `ExponentialBackOff` built from `manto.retry.*`; classification uses `DefaultExceptionClassifier` (`manto-kafka/DefaultExceptionClassifier.java:24` — non-retryable: `IllegalArgumentException`, `IllegalStateException`, `NullPointerException`, `SecurityException`).
- **Sensitive data** — example logs only `orderId`/`correlationId`; do not log full payloads. See `docs/SECURITY_MODEL.md`.
