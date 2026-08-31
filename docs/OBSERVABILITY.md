# Observability

Manto uses Micrometer, not a custom metrics API. Two concerns are covered: metrics for lifecycle events, and correlation ID propagation for request tracing.

## Metrics

### Instruments

All instruments are defined in `MantoMetrics` (`manto-kafka/src/main/java/io/github/manto/kafka/MantoMetrics.java:17`) and are low-cardinality — no event IDs, offsets, or exception messages are used as tag values.

| Metric name | Type | Tags | Recorded by |
|---|---|---|---|
| `manto.messages.published` | Counter | `topic`, `operation=publish`, `outcome=success` | `MantoKafkaProducer.publish` after `KafkaTemplate.send(...).get()` succeeds (`MantoKafkaProducer.java:74`) |
| `manto.messages.published` | Counter | `topic`, `operation=publish`, `outcome=failure` | `MantoKafkaProducer` catch blocks for `InterruptedException` / `ExecutionException` (`MantoKafkaProducer.java:79`) |
| `manto.messages.consumed` | Counter | `topic`, `operation=consume`, `outcome=success` | `MantoListenerInterceptor.intercept` on every delivered record (`MantoListenerInterceptor.java:33`) |
| `manto.messages.failed` | Counter | `topic`, `operation=consume`, `outcome=failure` | `MantoListenerInterceptor.recordFailed` called via the error handler path |
| `manto.messages.retried` | Counter | `topic`, `operation=retry`, `outcome=attempt` | `MantoErrorHandler` on each retry attempt |
| `manto.messages.dlt` | Counter | `topic`, `operation=dlt`, `outcome=published` | `MantoDeadLetterPublishingRecoverer.accept` after DLT publish (`MantoDeadLetterPublishingRecoverer.java:66`) |
| `manto.processing.duration` | Timer | `topic`, `operation=process` (percentile histogram) | `MantoListenerInterceptor` around handler execution (`MantoMetrics.java:39`) |

`manto.processing.duration` is a `Timer` with `publishPercentileHistogram()` enabled (`MantoMetrics.java:76`). Two recording paths exist:

```java
Timer.Sample sample = metrics.startProcessingTimer();
metrics.recordProcessingDuration(sample, topic);     // sample-based (interceptor path)
metrics.recordProcessingDuration(Duration.ofMillis(42), topic); // duration-based
```

Tags are deliberately fixed (`MantoMetrics.java:25`): `topic`, `operation`, `outcome`. Do not add high-cardinality tags.

### Enabling / Disabling

Controlled by `manto.observability.enabled` (`MantoProperties.java:219`, default `true`):

```yaml
manto:
  observability:
    enabled: true   # set false to make all MantoMetrics methods no-ops
```

When disabled, every `record*` method returns without touching the registry (`MantoMetrics.java:38`):

```java
public void recordPublished(String topic) {
    if (!enabled) return;
    registry.counter(...).increment();
}
```

### Registry Wiring

`MantoAutoConfiguration` (`MantoAutoConfiguration.java:88`) provides:

```java
@Bean
@ConditionalOnMissingBean
public MantoMetrics mantoMetrics(MeterRegistry meterRegistry, MantoProperties properties) {
    return new MantoMetrics(meterRegistry, properties.getObservability().isEnabled());
}

@Bean
@ConditionalOnMissingBean(MeterRegistry.class)
public MeterRegistry mantoMeterRegistry() {
    return new SimpleMeterRegistry();
}
```

- With Spring Boot Actuator on the classpath, your `PrometheusMeterRegistry` (or any other) is reused — metrics are exported automatically.
- Without Actuator, the fallback `SimpleMeterRegistry` keeps Manto metrics in-memory and queryable via `MeterRegistry` bean injection.
- To export elsewhere, declare your own `MeterRegistry` bean — the fallback is not created.

### Querying Metrics

With Actuator + Prometheus:

```yaml
management:
  endpoints:
    web:
      exposure:
        include: metrics,prometheus
```

Visit `/actuator/metrics/manto.messages.published` or scrape `/actuator/prometheus`. Example PromQL:

```
rate(manto_messages_published_total{outcome="success"}[5m])
rate(manto_messages_failed_total[5m])
histogram_quantile(0.99, rate(manto_processing_duration_seconds_bucket[5m]))
```

In code (e.g. in a test or health check):

```java
@Autowired MeterRegistry registry;

Counter published = registry.find("manto.messages.published")
    .tag("topic", "order-events").tag("outcome", "success").counter();
```

`InMemoryIdempotencyStore` and `MantoDeadLetterHandler` do not emit dedicated metrics — idempotency is application-level, and DLT success is counted via `manto.messages.dlt`.

## Correlation IDs

Correlation IDs let you trace an event across services. Manto carries them in the `Manto-Correlation-Id` header.

### Producer side

`MantoKafkaProducer.buildMessage` (`MantoKafkaProducer.java:91`) generates:

- `eventId = UUID.randomUUID().toString()`
- `eventType = event.getClass().getSimpleName()`
- `eventVersion = "1.0"`
- `correlationId = (explicitOrNull ? generated : explicit)` where explicit comes from the overload `publish(topic, event, correlationId)`; when `null`, `correlationId = eventId`
- `source = "manto"` (auto-config) or `"unknown"` (plain constructor)

Headers are set via `MessageBuilder` with `KafkaHeaders.TOPIC` + the five `MantoHeaders` constants.

To propagate a correlation ID from an upstream handler:

```java
import io.github.manto.kafka.CorrelationIdContext;
import io.github.manto.kafka.MantoKafkaProducer;

@Service
public class PaymentService {

    private final MantoProducer producer;

    public PaymentService(MantoProducer producer) {
        this.producer = producer;
    }

    public void completePayment(String orderId) {
        PaymentCompletedEvent event = new PaymentCompletedEvent(orderId, "COMPLETED", Instant.now());
        String correlationId = CorrelationIdContext.get();
        if (producer instanceof MantoKafkaProducer kafkaProducer) {
            kafkaProducer.publish("payment-events", event, correlationId);
        } else {
            producer.publish("payment-events", event);
        }
    }
}
```

### Consumer side

`MantoListenerInterceptor.intercept` (`MantoListenerInterceptor.java:29`) runs on the listener thread before the handler:

```java
public ConsumerRecord<String, Object> intercept(ConsumerRecord<String, Object> record, ...) {
    String correlationId = extractCorrelationId(record);
    CorrelationIdContext.set(correlationId);
    metrics.recordConsumed(record.topic());
    processingTimer.set(metrics.startProcessingTimer());
    return record;
}
```

`extractCorrelationId` reads `Manto-Correlation-Id`; if absent, falls back to `Manto-Event-Id`; if both absent, `null` is stored. The same header can be extracted without the interceptor via `MantoHeaderExtractor` (`manto-kafka/MantoHeaderExtractor.java:29`), which replaces missing headers with defaults (`eventId=UUID`, `eventType="UnknownEvent"`, etc.).

Application code accesses it via `CorrelationIdContext` (`manto-kafka/CorrelationIdContext.java:13`):

```java
import io.github.manto.kafka.CorrelationIdContext;

@MantoListener(topic = "order-events", groupId = "payment-service")
public void handle(OrderCreatedEvent event) {
    String correlationId = CorrelationIdContext.get();
    log.info("Handling orderId={} correlationId={}", event.orderId(), correlationId);
}
```

`CorrelationIdContext` is a `ThreadLocal<String>` — each consumer thread maintains its own value. It is cleared after processing on both paths to prevent leaks on pooled threads:

- `MantoListenerInterceptor.recordProcessingDuration(String topic)` clears after success (`MantoListenerInterceptor.java:45`)
- `MantoListenerInterceptor.recordFailed(String topic)` clears after failure (`MantoListenerInterceptor.java:49`)

Do not store the value beyond the handler invocation — future invocations on the same thread will overwrite it.

### Enriching logs with MDC

Typical pattern with SLF4J:

```java
@MantoListener(topic = "order-events", groupId = "payment-service")
public void handle(OrderCreatedEvent event) {
    String correlationId = CorrelationIdContext.get();
    MDC.put("correlationId", correlationId);
    try {
        paymentService.completePayment(event.orderId());
    } finally {
        MDC.remove("correlationId");
    }
}
```

Or use an interceptor/filter that reads `CorrelationIdContext.get()` and writes `MDC`. The framework clears the ThreadLocal itself — no manual `CorrelationIdContext.clear()` call is needed inside the handler, but `MDC` is the handler's responsibility.

## Logging

- Manto never logs credentials, tokens, or full sensitive payloads by default (`docs/SECURITY_MODEL.md`).
- `MantoDeserializationException` includes at most 200 characters of a failed payload (`MantoJsonDeserializer.java:112`).
- DLT headers include only metadata (`MantoDeadLetterPublishingRecoverer.java:70`).
- In DLT and handler logs, prefer `orderId`/`correlationId` over event amounts or raw JSON. The example DLT handler (`examples/order-payment/src/main/java/com/example/orderpayment/payment/PaymentDltHandler.java:32`) logs only `orderId`, `amount`, and `correlationId`.

## Configuration

```yaml
manto:
  observability:
    enabled: true
logging:
  level:
    io.github.manto: INFO
```

See `docs/CONFIGURATION.md` and `docs/ERROR_HANDLING.md#dlt-headers`.
