# Error Handling

## Goals

Manto makes transient failures recoverable and permanent failures diagnosable, without requiring the application to manage retry loops or DLT publishing.

## Processing Lifecycle

```text
consume
  |
  v
handler (@MantoListener)
  |
  +-- success --> acknowledge (offset committed)
  |
  +-- failure --> classify (ExceptionClassifier)
                   |
                   +-- retryable --> retry with exponential backoff
                   |      |  up to manto.retry.max-attempts total attempts
                   |      +-- succeeds --> acknowledge
                   |      +-- exhausted --> DLT (if manto.dlt.enabled=true) or log
                   |
                   +-- non-retryable --> DLT immediately (no retries)
```

Acknowledgement is handled by Spring Kafka's `ConcurrentKafkaListenerContainerFactory` — the offset is not committed before the handler completes successfully. Do not acknowledge a message as processed before the handler outcome is established.

## Retry

Configuration: `manto.retry.*` (`MantoProperties.java:98`, `docs/CONFIGURATION.md`).

| Property | Default | Effect |
|---|---|---|
| `manto.retry.enabled` | `true` | When `false`, no `DefaultErrorHandler` is installed on the container factory — failures propagate without retry. |
| `manto.retry.max-attempts` | `3` | Total attempts including the initial call. `1` means no retries. Maps to `ExponentialBackOff.setMaxAttempts(maxAttempts - 1)` (`MantoAutoConfiguration.java:198`). Constrained `@Min(1) @Max(100)`. |
| `manto.retry.backoff.initial-delay` | `1000ms` | First retry delay. |
| `manto.retry.backoff.multiplier` | `2.0` | Exponential multiplier (`@DecimalMin(1.0) @DecimalMax(10.0)`). |
| `manto.retry.backoff.max-delay` | `30000ms` | Cap for exponential delay. |

When `retry.enabled=true`, `MantoAutoConfiguration.kafkaListenerContainerFactory` (`MantoAutoConfiguration.java:179`) builds Spring's `ExponentialBackOff`:

```java
ExponentialBackOff backOff = new ExponentialBackOff();
backOff.setInitialInterval(backoffStrategy.getInitialDelay().toMillis());
backOff.setMultiplier(backoffStrategy.getMultiplier());
backOff.setMaxInterval(backoffStrategy.getMaxDelay().toMillis());
backOff.setMaxAttempts(Math.max(0, retryPolicy.maxAttempts() - 1));
CommonErrorHandler errorHandler = new MantoErrorHandler(interceptor, recovererOrNull, backOff);
```

Backoff formula implemented by `ExponentialBackoffStrategy.nextDelay(int attempt)` (`ExponentialBackoffStrategy.java:47`):

```
delay(n) = min(initialDelay * multiplier^(n-1), maxDelay)
attempt is 1-based (1 = first retry after the initial failure)
```

Example with `initial-delay=1000`, `multiplier=2.0`, `max-delay=10000`:

- attempt 1 → 1000 ms
- attempt 2 → 2000 ms
- attempt 3 → 4000 ms (capped at 10000 from attempt 5 onward)

`MantoErrorHandler` extends `DefaultErrorHandler` and delegates to `MantoListenerInterceptor` for metrics; retry counts are also surfaced on the DLT record (`DLT_RETRY_COUNT = maxAttempts - 1`).

### When retries happen

A retryable failure (see classification below) is retried until one of:

- The handler succeeds on a subsequent attempt — offset is committed, no DLT.
- `maxAttempts` is exhausted — the recoverer publishes to DLT if `manto.dlt.enabled=true`; otherwise the exception is logged and the offset advances.

Retries are blocking on the consumer thread for that partition (the thread sleeps for the backoff interval). Consider partition count and `maxAttempts * maxDelay` when sizing timeouts.

## Exception Classification

`ExceptionClassifier` (`manto-core/ExceptionClassifier.java:11`) is the abstraction; `DefaultExceptionClassifier` (`manto-kafka/DefaultExceptionClassifier.java:20`) is the auto-configured implementation.

### Default policy

| Classification | Types (`DefaultExceptionClassifier.java:25`) | Behaviour |
|---|---|---|
| Non-retryable (permanent) | `IllegalArgumentException`, `IllegalStateException`, `NullPointerException`, `SecurityException` | Bypass retry, go straight to DLT. Rationale: invalid data, programming error, auth failure — a retry will not help. |
| Retryable (transient) | Any other `Throwable` (including `RuntimeException`, `IOException`, timeouts) | Retried with backoff up to `maxAttempts`. `throwable == null` is considered retryable (`DefaultExceptionClassifier.java:43`). |
| Subtypes | `isInstance` is used (`DefaultExceptionClassifier.java:47`), so e.g. `NumberFormatException` (subtype of `IllegalArgumentException`) is non-retryable. | Matches parent types. |

Spring's `DefaultErrorHandler` shares the same set via `addNotRetryableExceptions` (`MantoAutoConfiguration.java:218`) so both framework-level and Spring's error handling agree.

### Customizing classification

Provide your own `DefaultExceptionClassifier` bean (`@ConditionalOnMissingBean`, `MantoAutoConfiguration.java:153`):

```java
import io.github.manto.kafka.DefaultExceptionClassifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Set;

@Configuration
public class MantoExceptionConfig {

    @Bean
    public DefaultExceptionClassifier mantoExceptionClassifier() {
        return new DefaultExceptionClassifier(Set.of(
                IllegalArgumentException.class,
                IllegalStateException.class,
                NullPointerException.class,
                SecurityException.class,
                ValidationException.class  // your domain permanent failure
        ));
    }
}
```

To make no exceptions non-retryable (always retry on any failure), pass an empty set: `new DefaultExceptionClassifier(Set.of())`.

For full control, implement `ExceptionClassifier` directly. The interface is in `manto-core` and `manto-kafka`'s wiring uses `DefaultExceptionClassifier` specifically, so a custom `ExceptionClassifier` type alone will not rewire the container factory — provide a `DefaultExceptionClassifier` if you want the container factory to respect it. (A future release may generalize this.)

### Example: choosing the right exception

```java
@Component
public class PaymentHandler {

    @MantoListener(topic = "order-events", groupId = "payment-service")
    public void handle(OrderCreatedEvent event) {
        if (event.amount() <= 0) {
            throw new IllegalArgumentException("Invalid amount: " + event.amount());
            // non-retryable → DLT immediately, no retries
        }
        try {
            gateway.charge(event);
        } catch (GatewayTimeoutException e) {
            throw new RuntimeException("Transient gateway failure", e);
            // retryable → retried with backoff up to maxAttempts
        }
    }
}
```

## Dead-Letter Topic (DLT)

Enable with `manto.dlt.enabled=true` (`MantoProperties.java:179`). The recoverer is `MantoDeadLetterPublishingRecoverer` extending Spring's `DeadLetterPublishingRecoverer` (`manto-kafka/MantoDeadLetterPublishingRecoverer.java:23`).

### Routing

- **Destination topic**: `record.topic() + manto.dlt.topic-suffix` (default suffix `.DLT`, so `order-events` → `order-events.DLT`). Partition is preserved (`new TopicPartition(record.topic() + topicSuffix, record.partition())`).
- **When**: after `maxAttempts` exhausted for retryable failures, or immediately for non-retryable failures. If `manto.dlt.enabled=false`, the handler still obeys classification (non-retryable bypasses retry) but no DLT publish occurs — the `MantoErrorHandler` is constructed with `BackOff` only (`MantoAutoConfiguration.java:204`).
- **Key/value**: original record's key and value are forwarded unchanged.

Programmatic DLT is also available via `DefaultDeadLetterHandler` (`manto-kafka/DefaultDeadLetterHandler.java:29`), which accepts a `MantoRecord<K,V>` and supports the same header contract. It blocks up to 10 seconds on `kafkaTemplate.send(...).get()` and throws `IllegalStateException` on failure (`DefaultDeadLetterHandler.java:60`).

### DLT Headers

Every DLT record carries (`MantoHeaders.java:16`, `MantoDeadLetterPublishingRecoverer.addMantoHeaders` and `DefaultDeadLetterHandler.buildHeaders`):

| Header | Source | Example |
|---|---|---|
| `Manto-Event-Id` | Copied from original if present | `550e8400-e29b-41d4-a716-446655440000` |
| `Manto-Event-Type` | Copied from original | `OrderCreatedEvent` |
| `Manto-Event-Version` | Copied from original | `1.0` |
| `Manto-Correlation-Id` | Copied from original | `550e8400-...` |
| `Manto-Source` | Copied from original | `manto` |
| `Manto-DLT-Original-Topic` | `record.topic()` | `order-events` |
| `Manto-DLT-Original-Partition` | `record.partition()` | `0` |
| `Manto-DLT-Original-Offset` | `record.offset()` | `42` |
| `Manto-DLT-Original-Timestamp` | `record.timestamp()` | `1717060000000` |
| `Manto-DLT-Exception-Class` | `exception.getClass().getName()` (or `"null"`) | `java.lang.RuntimeException` |
| `Manto-DLT-Exception-Message` | `exception.getMessage()` (or `"null"`) | `Transient payment gateway timeout...` |
| `Manto-DLT-Retry-Count` | `retryPolicy.maxAttempts() - 1` | `2` (for `maxAttempts=3`) |
| `Manto-DLT-Failure-Timestamp` | `Instant.now().toString()` at DLT publish time | `2026-05-06T12:34:56.789Z` |
| `Manto-DLT-Trace-Id` | `UUID.randomUUID().toString()` | `...` |

Note: `DLT_RETRY_COUNT` reflects the configured max retries, not the per-record attempt count at publication.

### Observing the DLT

Consume it like any other topic — `@MantoListener` works:

```java
@Component
public class PaymentDltHandler {

    @MantoListener(topic = "order-events.DLT", groupId = "payment-service-dlt")
    public void handleDlt(OrderCreatedEvent event) {
        String correlationId = CorrelationIdContext.get();
        // alert, persist to a store, or trigger manual replay
        log.warn("DLT received orderId={} correlationId={} - requires manual investigation",
                event.orderId(), correlationId);
    }
}
```

Do not apply retry or idempotency semantics here — the message is already considered poison. See `examples/order-payment/src/main/java/com/example/orderpayment/payment/PaymentDltHandler.java:28`.

## Redelivery and Idempotency Interaction

Retries cause at-least-once delivery: the same record may be delivered multiple times before success or DLT. Use `IdempotencyStore` (`manto-core/IdempotencyStore.java:9`) in the handler to deduplicate when handler side-effects are not naturally idempotent. See the example in `README.md#idempotency` and `examples/order-payment/src/main/java/com/example/orderpayment/payment/PaymentHandler.java:51`. `MantoErrorHandler` does not suppress duplicate deliveries itself — idempotency is the handler's responsibility.

## Logging and Sensitive Data

Never log full sensitive payloads by default. DLT headers include only metadata. `MantoDeserializationException` previews at most 200 characters of a failed payload (`MantoJsonDeserializer.java:112`). Log `orderId`/`correlationId` rather than amounts, tokens, or complete events unless explicitly required.

## Configuration Summary

```yaml
manto:
  retry:
    enabled: true
    max-attempts: 3
    backoff:
      initial-delay: 1000
      multiplier: 2.0
      max-delay: 30000
  dlt:
    enabled: true
    topic-suffix: .DLT
```
