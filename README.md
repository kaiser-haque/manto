# Manto

Manto is a small, production-oriented framework for Kafka-based event streaming on Java 21 and Spring Boot 3.5. It wraps Spring Kafka with a consistent developer experience for producer, consumer, metadata, correlation IDs, retry, DLT, idempotency, metrics, and auto-configuration.

Manto does not replace Kafka. It removes repetitive boilerplate without hiding Kafka's semantics.

## Installation

### Prerequisites

- Java 21
- Maven 3.9+
- Spring Boot 3.5.x (`spring-boot-dependencies:3.5.16` BOM in `pom.xml:29`)
- Apache Kafka (any broker compatible with `kafka-clients:3.9.2`)

### Dependency

Manto v1.0 is `0.1.0-SNAPSHOT` from local build until Maven Central publication (see `docs/MAVEN_CENTRAL.md`). Build once at the repository root:

```bash
mvn install -DskipTests
```

Then add the starter to your application:

```xml
<dependency>
    <groupId>io.github.manto</groupId>
    <artifactId>manto-spring-boot-starter</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

The starter transitively brings `manto-spring-boot-autoconfigure`, `manto-kafka`, and `manto-core`. No other Manto dependency is needed. See `manto-spring-boot-starter/pom.xml:20` and `docs/ARCHITECTURE.md`.

### Auto-configuration

When `spring-kafka` is on the classpath, `MantoAutoConfiguration` (`manto-spring-boot-autoconfigure:66`) registers:

- `MantoProducer` backed by `MantoKafkaProducer` (`MantoAutoConfiguration.java:100`)
- `ProducerFactory<String, Object>` and `KafkaTemplate<String, Object>` using `JsonSerializer`
- `ConsumerFactory<String, Object>` using `JsonDeserializer` with `TRUSTED_PACKAGES=*` and `USE_TYPE_INFO_HEADERS=true`
- `ConcurrentKafkaListenerContainerFactory<String, Object>` with the retry / DLT wiring below
- Listener infrastructure: `MantoListenerValidator`, `MantoListenerDiscoverer`, `MethodKafkaListenerEndpointFactory`, `MantoListenerRegistrar`
- `RetryPolicy` / `BackoffStrategy` / `ExceptionClassifier` / `DeadLetterHandler` (`MantoAutoConfiguration.java:134`)
- `IdempotencyStore` (`InMemoryIdempotencyStore` by default, overridable)
- `MantoMetrics` and `MantoListenerInterceptor` (observability)

All beans use `@ConditionalOnMissingBean` — provide your own bean of the same type to override.

## Quick Start

### 1. Configure Kafka

`manto-spring-boot-autoconfigure` reads `MantoProperties` with prefix `manto` (`MantoProperties.java:19`):

```yaml
# src/main/resources/application.yml
manto:
  kafka:
    bootstrap-servers: localhost:9092
```

Only `manto.kafka.bootstrap-servers` is required. The value is validated with `@NotBlank` and defaults to `localhost:9092` (`MantoProperties.java:84`). Full property reference: `docs/CONFIGURATION.md`.

### 2. Publish events

Inject `MantoProducer` (`manto-core:9`) and call `publish`:

```java
import io.github.manto.core.MantoProducer;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    private final MantoProducer producer;

    public OrderService(MantoProducer producer) {
        this.producer = producer;
    }

    public void placeOrder(OrderCreatedEvent event) {
        producer.publish("order-events", event);
    }
}
```

Contract (`manto-core/MantoProducer.java:18`):

```java
public interface MantoProducer {
    <T> void publish(String topic, T event);
}
```

`topic` must be non-blank, `event` must not be null (`MantoKafkaProducer.java:64`). The call is synchronous — it blocks on `KafkaTemplate.send(...).get()` and throws `MantoProducerException` on failure, restoring the interrupt flag if interrupted (`MantoKafkaProducer.java:77`).

Each `publish` adds standardized headers (`manto-core/MantoHeaders.java:10`):
`Manto-Event-Id` (UUID), `Manto-Event-Type` (event class simple name), `Manto-Event-Version` (`1.0`), `Manto-Correlation-Id` (equals event ID by default), `Manto-Source` (auto-configuration uses `"manto"`, plain `new MantoKafkaProducer(template)` defaults to `"unknown"`).

JSON encoding uses Spring Kafka's `JsonSerializer` with `JavaTimeModule` and `WRITE_DATES_AS_TIMESTAMPS=false` (`MantoJsonSerializer.java:22`).

#### Propagating correlation IDs downstream

When a service consumes an event and publishes a downstream event, forward the upstream correlation ID:

```java
import io.github.manto.kafka.CorrelationIdContext;
import io.github.manto.kafka.MantoKafkaProducer;

@Service
public class PaymentService {

    private final MantoProducer producer;

    public PaymentService(MantoProducer producer) {
        this.producer = producer;
    }

    public void completePayment(PaymentCompletedEvent event) {
        String correlationId = CorrelationIdContext.get();
        if (producer instanceof MantoKafkaProducer kafkaProducer) {
            kafkaProducer.publish("payment-events", event, correlationId);
        } else {
            producer.publish("payment-events", event);
        }
    }
}
```

The three-argument overload (`MantoKafkaProducer.java:64`) is on the Kafka implementation, not the `MantoProducer` interface. If `correlationId` is `null`, a new UUID (equal to the new event ID) is generated (`MantoKafkaProducer.java:95`).

### 3. Consume events

Annotate a method with `@MantoListener` (`manto-core/MantoListener.java:18`):

```java
import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.manto.core.MantoListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentHandler {

    @MantoListener(topic = "order-events", groupId = "payment-service")
    public void handle(OrderCreatedEvent event) {
        // process event
    }
}

// Example payload — any Jackson-serializable type works.
// The deserializer uses type headers (USE_TYPE_INFO_HEADERS) so no manual
// ObjectMapper setup is needed.
public record OrderCreatedEvent(
    @JsonProperty("orderId") String orderId,
    @JsonProperty("amount") long amount
) {}
```

Constraints enforced by `MantoListenerValidator` (`manto-kafka/MantoListenerValidator.java:16`) — violations throw `MantoListenerConfigurationException` at startup:

- `topic` and `groupId` must be non-blank
- Method must be `public`, non-`static`
- Method must declare exactly one parameter (the event payload)

The discoverer unwraps CGLIB proxies (`MantoListenerDiscoverer`) and the registrar hooks into Spring Kafka's `KafkaListenerConfigurer` so endpoints join the same `KafkaListenerEndpointRegistry` as `@KafkaListener` (`MantoListenerRegistrar`). Each endpoint ID is `topic:groupId:Class.method` (`MethodKafkaListenerEndpointFactory`).

Deserialization uses `JsonDeserializer` with `TRUSTED_PACKAGES=*` and `USE_TYPE_INFO_HEADERS=true` (`MantoAutoConfiguration.java:113`). `MantoJsonDeserializer` (`manto-kafka/MantoJsonDeserializer.java:31`) supports explicit `Class<?>`/`JavaType` constructors and falls back to `Map<String, Object>` when no target type is configured so the container's `GenericMessageConverter` can convert to the handler parameter type.

### 4. Run the example

A standalone runnable application is at `examples/order-payment`:

```
OrderService --publish--> order-events --@MantoListener--> PaymentHandler --publish--> payment-events
                                              | retry + backoff + DLT + idempotency
                                              └─► order-events.DLT
```

```bash
mvn install -DskipTests
docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.1
# If the container exits, check logs: docker logs kafka
# Alternative: docker compose -f examples/order-payment/docker-compose.yml up -d
cd examples/order-payment && mvn spring-boot:run
curl -X POST http://localhost:8080/orders -H 'Content-Type: application/json' -d '{"orderId":"order-123","amount":5000}'
```

The `docker run` form works on most hosts without extra env; if your Docker setup requires explicit listeners, use the compose file at `examples/order-payment/docker-compose.yml`. First pull is ~1 min and dominates start-up. See `examples/order-payment/README.md` for transient vs. permanent failure demos and header inspection.

## Configuration

All Manto properties live under `manto` (`MantoProperties.java:19`). See `docs/CONFIGURATION.md` for the full reference.

```yaml
manto:
  kafka:
    bootstrap-servers: localhost:9092   # @NotBlank, default localhost:9092
  retry:
    enabled: true                       # default true
    max-attempts: 3                     # @Min(1) @Max(100), default 3 (includes initial attempt)
    backoff:
      initial-delay: 1000               # Duration, default 1s (must be >0)
      multiplier: 2.0                   # @DecimalMin(1.0) @DecimalMax(10.0), default 2.0
      max-delay: 30000                  # Duration, default 30s (must be >0)
  dlt:
    enabled: false                      # default false
    topic-suffix: .DLT                  # default .DLT → DLT topic is <original-topic> + suffix
  idempotency:
    enabled: true                       # default true
  observability:
    enabled: true                       # default true
```

`maxAttempts` is the total number of delivery attempts including the first. Internally it maps to Spring's `ExponentialBackOff.setMaxAttempts(maxAttempts - 1)` (`MantoAutoConfiguration.java:198`).

## Retry

Controlled by `manto.retry.*` (`docs/ERROR_HANDLING.md`). When `enabled=true`, `MantoAutoConfiguration` builds a Spring `ExponentialBackOff` from `MantoProperties.Retry.Backoff` and installs a `MantoErrorHandler` on the container factory.

- Backoff for attempt `n` is `min(initialDelay * multiplier^(n-1), maxDelay)` (`ExponentialBackoffStrategy.java:51`).
- Exception classification determines whether a failure is retryable.

```java
// Throw a retryable exception → retried with backoff
throw new RuntimeException("transient gateway timeout");

// Throw a non-retryable exception → bypasses retry, routed straight to DLT
throw new IllegalArgumentException("invalid amount");
```

Non-retryable types by default (`DefaultExceptionClassifier.java:24`): `IllegalArgumentException`, `IllegalStateException`, `NullPointerException`, `SecurityException`. All other exceptions are retryable (`ExceptionClassifier.java:11`). Customize by providing your **own `DefaultExceptionClassifier` bean** (type must be `DefaultExceptionClassifier`, not just the `ExceptionClassifier` interface — `MantoAutoConfiguration.java:153` wires `DefaultExceptionClassifier` specifically via `@ConditionalOnMissingBean`):

```java
@Bean
public DefaultExceptionClassifier mantoExceptionClassifier() {
    return new DefaultExceptionClassifier(Set.of(IllegalArgumentException.class, ValidationException.class));
}
// Pass Set.of() to make every exception retryable.
```

Providing a bean of type `ExceptionClassifier` alone will not rewire the container factory.

## Dead-Letter Topic (DLT)

Enable with `manto.dlt.enabled=true` (`MantoProperties.java:179`). The recoverer (`MantoDeadLetterPublishingRecoverer.java:22`) publishes exhausted or non-retryable records to `<original-topic><manto.dlt.topic-suffix>` (default `.DLT`). The DLT consumer partition is preserved from the original record.

Each DLT record carries (`manto-core/MantoHeaders.java:16` + `MantoDeadLetterPublishingRecoverer.java:70`):

- `Manto-Event-Id`, `Manto-Event-Type`, `Manto-Event-Version`, `Manto-Correlation-Id`, `Manto-Source` (copied from original if present)
- `Manto-DLT-Original-Topic`, `Manto-DLT-Original-Partition`, `Manto-DLT-Original-Offset`, `Manto-DLT-Original-Timestamp`
- `Manto-DLT-Exception-Class`, `Manto-DLT-Exception-Message`
- `Manto-DLT-Retry-Count` — **not per-record**, equals configured `maxAttempts - 1` (e.g. `2` when `maxAttempts=3` even if the record failed on the first attempt; `MantoDeadLetterPublishingRecoverer.java:92`), `Manto-DLT-Failure-Timestamp` (ISO-8601), `Manto-DLT-Trace-Id` (UUID)

Observe DLT with a plain `@MantoListener`:

```java
@Component
public class PaymentDltHandler {

    @MantoListener(topic = "order-events.DLT", groupId = "payment-service-dlt")
    public void handleDlt(OrderCreatedEvent event) {
        // alert, persist, or trigger manual replay — no retry is applied here
    }
}
```

The `manto-kafka/DefaultDeadLetterHandler` (`DefaultDeadLetterHandler.java:29`) offers the same header preservation when used programmatically. Full lifecycle documented in `docs/ERROR_HANDLING.md`.

## Idempotency

Manto exposes `IdempotencyStore` (`manto-core/IdempotencyStore.java:9`):

```java
public interface IdempotencyStore {
    boolean isProcessed(String eventId);
    void markProcessed(String eventId);
}
```

The auto-configuration provides `InMemoryIdempotencyStore` (`manto-kafka/InMemoryIdempotencyStore.java:23`) when no other bean exists. It is a thread-safe `ConcurrentHashMap` set. Per ADR-005, it is not suitable for multi-instance deployments because state is not shared. Typical usage in a handler:

```java
@Component
public class PaymentHandler {

    private final IdempotencyStore idempotencyStore;
    private final PaymentService paymentService;

    public PaymentHandler(IdempotencyStore idempotencyStore, PaymentService paymentService) {
        this.idempotencyStore = idempotencyStore;
        this.paymentService = paymentService;
    }

    @MantoListener(topic = "order-events", groupId = "payment-service")
    public void handle(OrderCreatedEvent event) {
        String correlationId = CorrelationIdContext.get();
        if (correlationId != null && idempotencyStore.isProcessed(correlationId)) {
            return; // duplicate redelivery — skip
        }
        paymentService.completePayment(event.orderId());
        if (correlationId != null) {
            idempotencyStore.markProcessed(correlationId);
        }
    }
}
```

For Redis/database-backed deduplication, provide your own bean:

```java
@Bean
public IdempotencyStore idempotencyStore(RedisTemplate<String, String> redis) {
    return new RedisIdempotencyStore(redis);
}
```

`manto.idempotency.enabled` (`MantoProperties.java:203`, default `true`) is a gating flag for **your** handler code — Manto does not auto-skip duplicates. The `IdempotencyStore` bean is always available regardless of the flag (`MantoAutoConfiguration.java:166`); when `false` you simply stop consulting it. The handler decides (pattern below).

## Metrics

Manto uses Micrometer (`MantoMetrics.java:16`). Six low-cardinality instruments are registered (no event IDs, offsets, or exception messages as tags):

| Instrument | Tags | When recorded |
|---|---|---|
| `manto.messages.published` | `topic`, `operation=publish`, `outcome=success\|failure` | `MantoKafkaProducer` after `KafkaTemplate.send(...).get()` |
| `manto.messages.consumed` | `topic`, `operation=consume`, `outcome=success` | `MantoListenerInterceptor.intercept` |
| `manto.messages.failed` | `topic`, `operation=consume`, `outcome=failure` | `MantoListenerInterceptor.recordFailed` via `MantoErrorHandler` |
| `manto.messages.retried` | `topic`, `operation=retry`, `outcome=attempt` | `MantoErrorHandler` on each retry |
| `manto.messages.dlt` | `topic`, `operation=dlt`, `outcome=published` | `MantoDeadLetterPublishingRecoverer.accept` |
| `manto.processing.duration` | `topic`, `operation=process` (percentile histogram) | `MantoListenerInterceptor` timer around handler execution |

Disable all Manto metrics with:

```yaml
manto:
  observability:
    enabled: false
```

A `SimpleMeterRegistry` is auto-configured when no `MeterRegistry` bean exists (`MantoAutoConfiguration.java:93`), so counters/timers are queryable via injection and in tests. It is **in-memory only and not scrapeable** — add `spring-boot-starter-actuator` (and `micrometer-registry-prometheus`) to export to Prometheus. With Actuator, any `MeterRegistry` bean (e.g. `PrometheusMeterRegistry`) is reused automatically.

See `docs/OBSERVABILITY.md` for correlation ID logging details.

### Correlation IDs

`MantoListenerInterceptor` (`manto-kafka/MantoListenerInterceptor.java:15`) extracts `Manto-Correlation-Id` from the consumed record (falling back to `Manto-Event-Id`) and exposes it via:

```java
String correlationId = CorrelationIdContext.get(); // ThreadLocal, cleared after processing
```

Use it for SLF4J/MDC enrichment or downstream propagation (see producer example above). `CorrelationIdContext.clear()` is called on both `recordProcessingDuration` and `recordFailed` to prevent leaks on pooled threads.

## Example Application

`examples/order-payment` — minimal Spring Boot app showing the whole flow:

- `src/main/java/com/example/orderpayment/order/OrderService.java:37` — `producer.publish("order-events", event)`
- `src/main/java/com/example/orderpayment/payment/PaymentHandler.java:40` — `@MantoListener` + `CorrelationIdContext` + idempotency + retry/DLT triggering
- `src/main/java/com/example/orderpayment/payment/PaymentService.java:37` — `MantoKafkaProducer.publish("payment-events", event, correlationId)` downstream propagation
- `src/main/java/com/example/orderpayment/payment/PaymentDltHandler.java:28` — DLT observer

Run guide and log expectations: `examples/order-payment/README.md`. Index of examples: `examples/README.md`.

## Testing

Manto is verified against a real broker via Testcontainers (`apache/kafka:3.9.1`). The `manto-spring-boot-autoconfigure` module contains `MantoEndToEndIntegrationTest` covering: successful publish/consume, JSON serialization, metadata, retry, backoff timing, non-retryable classification, DLT routing + metadata, idempotency, and correlation ID propagation. Strategy: `docs/TESTING_STRATEGY.md`.

```bash
mvn test                         # all modules, includes 2+ Testcontainers broker tests
mvn -pl manto-core -am test      # fast unit tests only
```

## Documentation

| Document | Contents |
|---|---|
| `docs/PROJECT_CONTEXT.md` | Product positioning and v1.0 scope |
| `docs/ARCHITECTURE.md` | Module graph and runtime flow |
| `docs/API_DESIGN.md` | Public contracts and header names |
| `docs/CONFIGURATION.md` | Full `manto.*` property reference |
| `docs/ERROR_HANDLING.md` | Retry, backoff, exception classification, DLT |
| `docs/OBSERVABILITY.md` | Metrics and correlation IDs |
| `docs/TESTING_STRATEGY.md` | Test pyramid and Testcontainers usage |
| `development/decisions/` | ADRs 001–006 |

## v1.0 Goal and Non-Goals

Shipped: producer, `@MantoListener`, JSON serialization (Jackson + `JavaTimeModule`, ISO dates), metadata/correlation headers, configurable retry with exponential backoff, DLT, in-memory idempotency (single-instance, ADR-005), Micrometer, Spring Boot starter, Testcontainers integration, and documentation.

Not in v1.0: Avro/Protobuf, Schema Registry, Kafka Streams, admin UI/CLI, event replay, Redis idempotency (implement `IdempotencyStore` yourself).

## Development Model

30 one-hour daily sessions. Read `AGENTS.md` first, then the task file in `tasks/`.

## License

See `LICENSE`.
