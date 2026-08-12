# Architecture

## High-level

```text
Application
    |
    v
Manto Spring Boot Starter
    |
    +--> manto-spring-boot-autoconfigure
    |
    +--> manto-kafka
    |
    +--> manto-core
    |
    v
Spring Kafka
    |
    v
Apache Kafka
```

## Modules

### manto-core

Framework abstractions and domain models. Must not depend on Spring Kafka.

### manto-kafka

Kafka-specific implementations using Spring Kafka.

### manto-spring-boot-autoconfigure

Spring Boot configuration properties, auto-configuration, bean creation, and listener integration.

### manto-spring-boot-starter

Convenience dependency that brings the runtime modules together.

### manto-test

Reserved for testing utilities; keep v1.0 scope small.

## Dependency direction

`manto-core` <- `manto-kafka` <- `manto-spring-boot-autoconfigure` <- starter.

Core must remain technology-light.

## Runtime flow

Producer:

```text
Application
 -> MantoProducer
 -> MantoKafkaProducer
 -> KafkaTemplate
 -> Kafka
```

Consumer:

```text
Kafka
 -> Spring Kafka listener
 -> Manto listener infrastructure
 -> deserialization
 -> application handler
```

Failure:

```text
handler failure
 -> retry policy
 -> backoff
 -> retry
 -> DLT after exhaustion
```

## Design principle

Manto should simplify repetitive infrastructure concerns without preventing developers from understanding or controlling Kafka behavior.
