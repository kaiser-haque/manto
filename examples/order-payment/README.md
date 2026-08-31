# Order-Payment Example

Minimal, runnable Spring Boot application demonstrating **Manto** end-to-end.

```
                  order-events                    payment-events
OrderService  ─────────────►  PaymentHandler  ─────────────────►  (downstream)
  (REST)        @MantoListener   |  retry + DLT + idempotency
                                 └─► order-events.DLT (on permanent failure / exhausted retries)
```

## What it shows

| Manto feature | Where |
|---|---|
| **producer** | `order/OrderService.java:24` — `producer.publish("order-events", event)` <br> `payment/PaymentService.java:26` — publish to `payment-events` with correlation propagation |
| **@MantoListener** | `payment/PaymentHandler.java:31` — `@MantoListener(topic="order-events", groupId="payment-service")` <br> `payment/PaymentDltHandler.java:19` — DLT observer |
| **metadata** | Manto injects `Manto-Event-Id`, `Manto-Event-Type`, `Manto-Event-Version`, `Manto-Correlation-Id`, `Manto-Source` on every `publish`. <br> Consumer side: `CorrelationIdContext.get()` in `PaymentHandler.java:33` + `PaymentService.java:22`. <br> Downstream propagation: `MantoKafkaProducer.publish(topic, event, correlationId)` in `PaymentService.java:26`. |
| **retry** | `src/main/resources/application.yml:9` — `manto.retry.*` with exponential backoff. <br> Transient `RuntimeException` in `PaymentHandler.java:51` is retried 3 times. |
| **DLT** | `application.yml:19` — `manto.dlt.enabled=true`. <br> Exhausted retries or non-retryable `IllegalArgumentException` (`PaymentHandler.java:46`) route to `order-events.DLT` with headers `Manto-DLT-Original-Topic`, `Manto-DLT-Exception-Class`, `Manto-DLT-Retry-Count`, etc. <br> Observed by `PaymentDltHandler`. |
| **idempotency** | `PaymentHandler.java:38` — `IdempotencyStore.isProcessed(correlationId)` / `markProcessed` guards duplicate redelivery (at-least-once → effectively-once for single instance). |

## Prerequisites

- Java 21, Maven 3.9+
- Docker (for Kafka) — or any Kafka at `localhost:9092`

Build Manto locally once (example depends on `0.1.0-SNAPSHOT`):

```bash
mvn install -DskipTests
```

Start Kafka (KRaft, no ZooKeeper):

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.1
# or: docker compose up  # if you add a compose file
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

Observe logs:

```
Handling orderId=order-123 amount=5000 correlationId=...
Payment completed orderId=order-123 correlationId=...

Handling orderId=order-999 ...  -> RuntimeException -> retry 1, 2, 3 -> DLT
DLT received orderId=order-999 ... requires manual investigation
```

Inspect topics (e.g. with `kcat` or `kafka-console-consumer`):

```bash
# payment success
kafka-console-consumer --bootstrap-server localhost:9092 --topic payment-events --from-beginning

# dead letters
kafka-console-consumer --bootstrap-server localhost:9092 --topic order-events.DLT --from-beginning \
  --property print.headers=true
```

Headers on `payment-events` include the propagated `Manto-Correlation-Id` from the original order.
Headers on `order-events.DLT` include `Manto-DLT-Original-Topic`, `Manto-DLT-Exception-Class`, `Manto-DLT-Retry-Count`, `Manto-DLT-Failure-Timestamp`, etc.

## Configuration

All Manto behavior is driven by `application.yml` (see `docs/CONFIGURATION.md`):

```yaml
manto:
  kafka:
    bootstrap-servers: localhost:9092
  retry:
    enabled: true
    max-attempts: 3
    backoff:
      initial-delay: 1000
      multiplier: 2.0
      max-delay: 10000
  dlt:
    enabled: true
    topic-suffix: .DLT
  idempotency:
    enabled: true
```

## Notes

- **No business complexity** — events are just `orderId + amount`; payment is just a log + publish.
- **In-memory idempotency** is single-instance only. For multi-instance production, provide your own `IdempotencyStore` bean (e.g. Redis) — Manto auto-configures via `@ConditionalOnMissingBean`.
- **Metrics** (`manto.observability.enabled=true`) expose `manto.messages.*` and `manto.processing.duration` via Micrometer — scrape with Prometheus/Actuator if desired.
- See `docs/ERROR_HANDLING.md` for retry/DLT classification and `docs/OBSERVABILITY.md` for correlation best practices.
