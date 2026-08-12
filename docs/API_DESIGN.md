# API Design

## Design goals

- Small surface area.
- Strong typing.
- Stable public contracts.
- Clear separation between framework abstractions and Kafka implementation.

## Initial public concepts

### MantoProducer

```java
public interface MantoProducer {
    <T> void publish(String topic, T event);
}
```

Additional overloads may be added only when justified.

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
