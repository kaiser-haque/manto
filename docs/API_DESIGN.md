# API Design

## Design goals

- Small surface area.
- Strong typing.
- Stable public contracts.
- Clear separation between framework abstractions and Kafka implementation.

## Initial public concepts

### MantoProducer

```java
// manto-core: framework abstraction (keep small)
public interface MantoProducer {
    <T> void publish(String topic, T event);
}
```

The Kafka implementation `MantoKafkaProducer` (`manto-kafka/MantoKafkaProducer.java:64`) adds one intentional overload for correlation propagation:

```java
// manto-kafka: Kafka-backed implementation only
public <T> void publish(String topic, T event, String correlationId) { ... }
```

The interface stays minimal so downstream code can stay framework-agnostic. When a consumer publishes a downstream event, cast to `MantoKafkaProducer` if present and forward `CorrelationIdContext.get()` (see `README.md#propagating-correlation-ids-downstream`, `docs/OBSERVABILITY.md#producer-side`, and `examples/order-payment/src/main/java/com/example/orderpayment/payment/PaymentService.java:37`). If `correlationId` is `null`, the producer generates a new UUID equal to the new `eventId` (`MantoKafkaProducer.java:95`). Additional overloads may be added only when justified.

### MantoEventMetadata

Fields:

- eventId
- eventType
- eventVersion
- correlationId
- source
- timestamp

### MantoListener

Annotation properties:

- topic
- groupId

Keep annotation semantics predictable.

### IdempotencyStore

```java
public interface IdempotencyStore {
    boolean isProcessed(String eventId);
    void markProcessed(String eventId);
}
```

## API compatibility

Once v1.0 is released:

- avoid breaking method signatures,
- prefer additive changes,
- deprecate before removal,
- document behavioral changes.

## Headers

Use standardized Manto-prefixed Kafka headers:

- `Manto-Event-Id`
- `Manto-Event-Type`
- `Manto-Event-Version`
- `Manto-Correlation-Id`
- `Manto-Source`

Do not serialize sensitive information into headers.
