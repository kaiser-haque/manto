# Project Context

## Product

**Name:** Manto

**Positioning:** Enterprise-oriented event streaming developer-experience and reliability framework for Java and Spring Boot.

Manto is not a replacement for Kafka. It is a convention and abstraction layer over Spring Kafka.

## Problem

Teams repeatedly implement similar Kafka concerns:

- producer boilerplate,
- consumer registration,
- event metadata,
- correlation IDs,
- retries,
- dead-letter handling,
- idempotency,
- metrics,
- configuration.

Manto provides a consistent developer experience for those concerns.

## Target users

Java/Spring Boot teams using Apache Kafka who want standardized application-level messaging patterns without hiding Kafka completely.

## Core user experience

Dependency:

```xml
<dependency>
  <groupId>io.github.<namespace></groupId>
  <artifactId>manto-spring-boot-starter</artifactId>
  <version>1.0.0</version>
</dependency>
```

Consumer:

```java
@MantoListener(topic = "order-events", groupId = "payment-service")
public void handle(OrderCreatedEvent event) {
    paymentService.process(event);
}
```

Producer:

```java
mantoProducer.publish("order-events", event);
```

## Technology baseline

- Java 21
- Maven
- Spring Boot
- Spring Kafka
- Jackson
- JUnit 5
- Mockito
- Testcontainers
- Micrometer

## v1.0 scope

Producer, consumer, event metadata, correlation ID, retry, exponential backoff, DLT, in-memory idempotency, metrics, Spring Boot starter, integration testing, documentation, CI, Maven Central release.

## Deferred

Avro, Protobuf, Schema Registry, Kafka Streams, CLI, dashboard, replay, Redis idempotency, multi-broker management.

## Success criteria

A developer with no Manto source checkout can consume the released starter from Maven Central and implement a producer/consumer flow with retry and DLT using documented configuration.
