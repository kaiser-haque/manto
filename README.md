# Manto

Manto is a Java/Spring Boot developer-experience and reliability framework for Kafka-based event streaming.

## Getting Started

### 1. Add the starter dependency

```xml
<dependency>
    <groupId>io.github.manto</groupId>
    <artifactId>manto-spring-boot-starter</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

### 2. Configure Kafka bootstrap servers

```yaml
# application.yml
manto:
  kafka:
    bootstrap-servers: localhost:9092
```

### 3. Publish events with `MantoProducer`

```java
@Service
public class OrderService {
    private final MantoProducer producer;

    public OrderService(MantoProducer producer) {
        this.producer = producer;
    }

    public void placeOrder(Order order) {
        producer.publish("order-events", new OrderCreatedEvent(order.getId(), order.getAmount()));
    }
}
```

### 4. Consume events with `@MantoListener`

```java
@Component
public class PaymentHandler {
    @MantoListener(topic = "order-events", groupId = "payment-service")
    public void handleOrder(OrderCreatedEvent event) {
        // process event
    }
}
```

The starter provides auto-configuration for:
- `MantoProducer` (Kafka-backed, synchronous, adds standard headers)
- `KafkaTemplate` for direct Spring Kafka access
- `ConcurrentKafkaListenerContainerFactory` with JSON deserialization
- Automatic discovery and registration of `@MantoListener` methods

### 5. Run the example

A minimal order → payment flow is available under [`examples/order-payment`](examples/order-payment/):

```
OrderService --publish--> order-events --@MantoListener--> PaymentHandler --publish--> payment-events
                                              | retry + DLT + idempotency
                                              └─► order-events.DLT
```

It demonstrates producer, `@MantoListener`, metadata/correlation, retry with exponential backoff, DLT, and idempotency:

```bash
mvn install -DskipTests
docker run -d --name kafka -p 9092:9092 apache/kafka:3.9.1
cd examples/order-payment && mvn spring-boot:run
curl -X POST http://localhost:8080/orders -H 'Content-Type: application/json' -d '{"orderId":"order-123","amount":5000}'
```

See [`examples/order-payment/README.md`](examples/order-payment/README.md) for details.

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
