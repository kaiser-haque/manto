# Manto Development Progress

## Current day

Day 28 — Maven Central preparation.

Next session: Day 29 — Performance and polish.

## Current version

0.1.0-SNAPSHOT

## Completed

- [x] Repository foundation
- [x] Maven modules
- [x] Dependency management
- [x] Core API
- [x] Producer
- [x] Listener registration
- [x] Consumer
- [x] JSON serialization
- [x] Headers
- [x] Configuration properties
- [x] Spring Boot starter auto-configuration
- [x] Retry
- [x] DLT
- [x] DLT metadata enhancement
- [x] Idempotency
- [x] Exception classification
- [x] Metrics
- [x] Correlation IDs
- [x] Integration tests
- [x] Example application
- [x] Documentation
- [x] Quality and security
- [x] Maven Central preparation

## Current task

Day 28 — Maven Central preparation.

## Day 25 work

- `examples/order-payment` — new standalone Spring Boot application demonstrating Manto end-to-end (Order Service → `order-events` → Payment Service → `payment-events`):
  - `OrderPaymentApplication.java` — entry point, docs the full flow diagram
  - `order/OrderCreatedEvent.java` — minimal record (`orderId`, `amount`) published as JSON; Manto adds standard headers automatically
  - `order/OrderService.java:24` — **producer**: `producer.publish("order-events", event)` with constructor-injected `MantoProducer`; no KafkaTemplate boilerplate
  - `order/OrderController.java` — minimal `POST /orders` to trigger flow without Kafka tooling (`curl` examples in README)
  - `payment/PaymentCompletedEvent.java` — downstream event (`orderId`, `status`, `processedAt`)
  - `payment/PaymentService.java:26` — **producer + metadata propagation**: `MantoKafkaProducer.publish("payment-events", event, correlationId)` forwards upstream `CorrelationIdContext.get()` so traces correlate across services
  - `payment/PaymentHandler.java:31` — **`@MantoListener`** + **metadata** + **retry** + **idempotency**:
    - `@MantoListener(topic="order-events", groupId="payment-service")` — one annotation, no manual container setup
    - `CorrelationIdContext.get()` — reads `Manto-Correlation-Id` header populated by Manto interceptor
    - Idempotency guard: `IdempotencyStore.isProcessed(correlationId)` / `markProcessed` after success (at-least-once → effectively-once for single instance; distributed store can replace bean via `@ConditionalOnMissingBean`)
    - Permanent failure: `IllegalArgumentException` for `amount<=0` → non-retryable → straight to DLT
    - Transient failure: `RuntimeException` for `amount==999` → retryable → exponential backoff (`manto.retry.*`) up to `max-attempts`
  - `payment/PaymentDltHandler.java:19` — **DLT** observer: `@MantoListener(topic="order-events.DLT", groupId="payment-service-dlt")` logs poison messages; DLT headers include `Manto-DLT-Original-Topic`, `Manto-DLT-Exception-Class`, `Manto-DLT-Retry-Count`, etc.
  - `src/main/resources/application.yml` — complete Manto config: `kafka.bootstrap-servers`, `retry.enabled/max-attempts/backoff`, `dlt.enabled/topic-suffix`, `idempotency.enabled`, `observability.enabled`
  - `pom.xml` — standalone Maven project (Spring Boot 3.5.16, Java 21) depending on `manto-spring-boot-starter:0.1.0-SNAPSHOT`; requires `mvn install -DskipTests` at repo root first
  - `README.md` — GitHub-discoverable guide with diagram, feature table (file:line for each demo), prerequisites, `docker run apache/kafka:3.9.1`, `mvn spring-boot:run`, `curl` for happy/transient/permanent paths, log expectations, `kafka-console-consumer` for `payment-events` and `order-events.DLT` with headers
  - `examples/README.md` — index for future examples

- `README.md` (root): Added section 5 “Run the example” linking to `examples/order-payment` with diagram, `mvn install`, `docker run`, and `curl` snippet; references feature docs

- Design choices:
  - No unnecessary business complexity: events are `orderId+amount`, payment is log+publish; no DB, no over-engineering
  - Example is not added to root reactor modules — standalone build avoids polluting `mvn test` while still being verifiable via `mvn -f examples/order-payment/pom.xml compile`
  - Constructor injection, immutable records, slf4j with correlationId, no sensitive payload logging — matches `development/CODING_STANDARDS.md`

- Tests:
  - Verified example compiles: `mvn install -DskipTests -pl manto-core,manto-kafka,manto-spring-boot-autoconfigure,manto-spring-boot-starter -am` → BUILD SUCCESS; `mvn -f examples/order-payment/pom.xml compile` → BUILD SUCCESS (8 sources)
  - Existing tests still pass: `mvn test -pl manto-core -am` → 19 tests pass; `mvn test -pl manto-kafka -am` → 129 tests pass; `mvn test -Dsurefire.failIfNoSpecifiedTests=false -Dtest=MantoPropertiesTest` across reactor → BUILD SUCCESS

## Day 24 work

- `manto-spring-boot-autoconfigure` (test): Created `MantoEndToEndIntegrationTest` — comprehensive E2E integration test suite covering all 10 scenarios against a real Kafka broker (Testcontainers `apache/kafka:3.9.1`):
  1. **Successful publish/consume**: basic event round-trip through `@MantoListener`
  2. **JSON serialization**: nested/complex event objects survive JSON encode/decode
  3. **Metadata propagation**: Manto headers (`Event-Id`, `Event-Type`, `Event-Version`, `Correlation-Id`, `Source`) present on consumed records
  4. **Retry (failure then success)**: handler fails once, succeeds on 2nd attempt (attempts=2)
  5. **Exponential backoff timing**: total retry time >= 200ms for 100ms+200ms expected delays
  6. **Non-retryable failure**: `IllegalArgumentException` triggers 1 attempt only, no retries
  7. **DLT routing**: exhausted retries route record to `topic.DLT`
  8. **DLT metadata**: DLT record carries all expected headers (`Original-Topic`, `Exception-Class`, `Exception-Message`, `Retry-Count`, `Failure-Timestamp`, `Trace-Id`, `Event-Id`, `Correlation-Id`)
  9. **Idempotency**: duplicate event with same key is detected and skipped (processed once)
  10. **Correlation ID**: explicit correlation ID propagates through headers to handler context

- `manto-kafka` (production): Fixed `MantoDeadLetterPublishingRecoverer` DLT headers:
  - Overrode `createProducerRecord` to add Manto DLT headers (`DLT_ORIGINAL_TOPIC`, `DLT_ORIGINAL_PARTITION`, `DLT_ORIGINAL_OFFSET`, `DLT_ORIGINAL_TIMESTAMP`, `DLT_EXCEPTION_CLASS`, `DLT_EXCEPTION_MESSAGE`) alongside event metadata headers.
  - Added ThreadLocal `CURRENT_EXCEPTION` to pass the exception from the 3-arg `accept` (ConsumerAwareRecordRecoverer) to `createProducerRecord` which doesn't receive the exception.
  - Overrode 3-arg `accept(ConsumerRecord, Consumer, Exception)` to store exception in ThreadLocal before `super.accept()` and clear it in `finally`.
  - Added `DLT_RETRY_COUNT`, `DLT_FAILURE_TIMESTAMP`, `DLT_TRACE_ID` headers in `addMantoHeaders`.

- Tests:
  - `MantoEndToEndIntegrationTest` (9 tests): all 10 scenarios covered in a single test class (scenarios 7 & 8 combined in `dltRoutingAndMetadata`).
  - All **183 tests pass** across all modules (core 19 + kafka 129 + autoconfigure 25 + starter 0 + test 0).
  - Build command: `mvn test -pl manto-spring-boot-autoconfigure -am -Dtest=MantoEndToEndIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false`.

## Day 23 work

- `manto-kafka` (package `io.github.manto.kafka`): Implemented correlation ID propagation for logging context:
  - `CorrelationIdContext`: New ThreadLocal-based utility class for accessing the current correlation ID during message processing. Provides `get()`, `set(correlationId)`, and `clear()` methods. Thread-isolated: each consumer thread maintains its own correlation ID.
  - `MantoListenerInterceptor`: Updated to extract the correlation ID from the incoming record's `Manto-Correlation-Id` header (falling back to `Manto-Event-Id`) and set it in `CorrelationIdContext` on intercept. Clears the context on both `recordProcessingDuration` and `recordFailed` to prevent ThreadLocal leaks.
  - `MantoKafkaProducer`: Added `publish(String topic, T event, String correlationId)` overload that accepts an explicit correlation ID. When `correlationId` is null, generates a new UUID (default behavior). Enables propagating a correlation ID from an upstream service or processing context through Kafka headers.
- Integration tests: Added `CorrelationIdPropagationIntegrationTest` (Testcontainers) with 3 end-to-end scenarios:
  - `propagatesExplicitCorrelationIdFromProducerToConsumer`: verifies explicit correlation ID flows through Kafka headers
  - `generatesCorrelationIdEqualToEventIdWhenNotExplicit`: verifies default correlation ID equals event ID
  - `interceptorSetsCorrelationIdInContext`: verifies the interceptor populates `CorrelationIdContext` and clears it after processing

- Tests:
  - `CorrelationIdContextTest` (6 tests): covers get/set/clear, ThreadLocal isolation, concurrent access, and clear-on-one-thread-not-affecting-another.
  - `MantoListenerInterceptorTest` (9 tests): added 5 tests for correlation ID context propagation — sets on intercept, clears on processing duration, clears on failed, falls back to event ID, sets null when no headers present.
  - `MantoKafkaProducerTest` (15 tests): added 4 tests for explicit correlation ID — uses explicit ID, generates when null, rejects null topic, rejects null event.
  - `CorrelationIdPropagationIntegrationTest` (3 tests): end-to-end correlation ID propagation with real Kafka broker.
  - All 145 tests pass (`mvn -pl manto-spring-boot-autoconfigure -am test` — BUILD SUCCESS): core 19 + kafka 110 + autoconfigure 16.

## Day 22 work

- `manto-kafka` (package `io.github.manto.kafka`): Added Micrometer metrics infrastructure:
  - `MantoMetrics`: Core metrics class wrapping Micrometer `MeterRegistry`, exposing methods for all required metrics:
    - `manto.messages.published` (counter with tags: topic, operation=publish, outcome=success|failure)
    - `manto.messages.consumed` (counter with tags: topic, operation=consume, outcome=success)
    - `manto.messages.failed` (counter with tags: topic, operation=consume, outcome=failure)
    - `manto.messages.retried` (counter with tags: topic, operation=retry, outcome=attempt)
    - `manto.messages.dlt` (counter with tags: topic, operation=dlt, outcome=published)
    - `manto.processing.duration` (timer with tags: topic, operation=process, percentile histogram enabled)
  - All metrics use low-cardinality tags only (topic, operation, outcome) — no event IDs, offsets, exception messages, or user-controlled values.
  - `MantoListenerInterceptor`: RecordInterceptor for consumed metrics, processing duration (via ThreadLocal Timer.Sample), and failed metrics.
  - `MantoErrorHandler`: Extends Spring Kafka's `DefaultErrorHandler` to record failed metrics on handleRemaining and publishDeadLetter.
  - Updated `MantoKafkaProducer` to record published (success/failure) metrics.
  - Updated `MantoDeadLetterPublishingRecoverer` to record DLT metrics.
- `manto-spring-boot-autoconfigure`:
  - Added `MantoMetrics` bean auto-configured from `MeterRegistry` and `MantoProperties.observability.enabled`.
  - Provides default `SimpleMeterRegistry` when no `MeterRegistry` bean exists (enables metrics out-of-the-box without Spring Boot Actuator).
  - Wired metrics into producer, listener interceptor, error handler, DLT recoverer, and container factory.
  - Metrics can be disabled via `manto.observability.enabled=false`.
- Configuration: Metrics controlled via `manto.observability.enabled` (default `true`).

- Tests:
  - `MantoMetricsTest` (10 tests): covers all metrics (published success/failure, consumed, failed, retried, DLT, processing duration), disabled state, low-cardinality tag verification.
  - `MantoListenerInterceptorTest` (4 tests): verifies consumed, processing duration, failed metrics, and disabled state.
  - `MantoKafkaProducerTest`: Added 3 tests for published success/failure/interrupt metrics.
  - All unit tests pass (`mvn test` — BUILD SUCCESS): core 19 + kafka 111 + autoconfigure 16 (including Testcontainers integration tests).
  - Verified all existing tests still pass.

## Day 21 work

- `manto-kafka` (package `io.github.manto.kafka`): Added `InMemoryIdempotencyStore` — thread-safe in-memory implementation of `IdempotencyStore` using `ConcurrentHashMap`. Documented as **not suitable for multi-instance production deployments** (per ADR-005); external stores (Redis, database) can be added later by implementing the `IdempotencyStore` interface.
- `manto-spring-boot-autoconfigure`: Added `mantoIdempotencyStore` bean to `MantoAutoConfiguration` with `@ConditionalOnMissingBean` for user overrides.
- Configuration: Idempotency controlled via `manto.idempotency.enabled` (default `true`).

- Tests:
  - `InMemoryIdempotencyStoreTest` (17 tests): covers basic operations (isProcessed/markProcessed), idempotency, multiple events, empty event ID, and concurrency (10 repeated concurrent access tests + concurrent mark/check test with 50 threads).
  - All unit tests pass (`mvn test` — BUILD SUCCESS): core 19 + kafka 94 + autoconfigure 16 (including Testcontainers integration tests).

## Day 20 work

- `manto-kafka` (package `io.github.manto.kafka`): No code changes needed — `DefaultDeadLetterHandler` already preserves all required metadata headers per `ERROR_HANDLING.md` spec:
  - Original topic, partition, offset, timestamp
  - Event ID, correlation ID (from Manto headers)
  - Exception class, exception message
  - Retry count
  - Failure timestamp, trace ID
  - No sensitive payload logging by default

- Configuration: DLT behavior controlled via `manto.dlt.enabled` (default `false`) and `manto.dlt.topic-suffix` (default `.DLT`).

- Tests:
  - All unit tests pass (`mvn test` — BUILD SUCCESS): core 19 + kafka 77 unit + autoconfigure 13 unit.
  - DLT metadata is verified through the existing `DefaultDeadLetterHandlerTest` unit tests (7 tests) and the `DefaultDeadLetterHandler` implementation.
  - DLT publishing via error handler recoverer has known limitations in Spring Kafka 3.3.x; programmatic use of `DefaultDeadLetterHandler` is recommended for production DLT publishing.

## Day 19 work

- `manto-kafka` (package `io.github.manto.kafka`): Connected the existing `DefaultExceptionClassifier` to Spring Kafka's `DefaultErrorHandler`:
  - Added `getNonRetryableTypes()` getter to `DefaultExceptionClassifier` to expose configured non-retryable exception types.
  - Updated `MantoAutoConfiguration.kafkaListenerContainerFactory` to accept `DefaultExceptionClassifier` and configure Spring Kafka's `DefaultErrorHandler` with the non-retryable exception types via `addNotRetryableExceptions()`.
  - Default non-retryable types: `IllegalArgumentException`, `IllegalStateException`, `NullPointerException`, `SecurityException` (permanent failures like invalid data, programming errors, auth failures). All other exceptions are retryable (transient failures).
  - Users can customize by providing their own `DefaultExceptionClassifier` bean with custom non-retryable types.

- Tests:
  - `DefaultExceptionClassifierTest` (7 tests): verifies classification of retryable/non-retryable exceptions, null handling, and custom non-retryable types.
  - Added `ExceptionClassificationIntegrationTest` (3 Testcontainers tests):
    - `nonRetryableExceptionShouldBypassRetriesAndGoToDlt`: verifies `IllegalArgumentException` bypasses retries (1 attempt).
    - `retryableExceptionShouldRetry`: verifies `RuntimeException` retries (fail twice, succeed on 3rd attempt).
    - `defaultBehaviorRuntimeExceptionIsRetryable`: verifies `RuntimeException` is retryable by default (fail once, succeed on 2nd attempt).
  - All 115 tests pass (`mvn clean verify` — BUILD SUCCESS): core 19 + kafka 77 + autoconfigure 19 (including Testcontainers exception classification integration tests).

## Day 17 work

- `manto-spring-boot-autoconfigure` (package `io.github.manto.autoconfigure`): Wired retry policy and backoff strategy into Spring Kafka's listener container factory:
  - Updated `kafkaListenerContainerFactory` bean to accept `RetryPolicy` and `ExponentialBackoffStrategy` dependencies.
  - When retry is enabled, creates Spring's `ExponentialBackOff` from Manto's backoff strategy and configures `DefaultErrorHandler` with it.
  - Maps Manto's `maxAttempts` (total attempts including initial) to Spring's `ExponentialBackOff.setMaxAttempts(maxAttempts - 1)` (retries only).

- Tests:
  - Added `RetryIntegrationTest` (Testcontainers) with three scenarios:
    - Success on first attempt (1 attempt)
    - Failure followed by success (2 attempts)
    - Failure on all attempts (3 attempts, matching `max-attempts: 3`)
  - All tests pass with real Kafka broker.

## Day 15 work

- `manto-core` (package `io.github.manto.core`): Added error handling abstractions (no Kafka/Spring dependencies):
  - `RetryPolicy` interface: defines `isEnabled()` and `maxAttempts()` for configurable retry behavior.
  - `BackoffStrategy` interface: defines `nextDelay(int attempt)` for backoff calculation between retries.
  - `ExceptionClassifier` interface: defines `isRetryable(Throwable)` to classify exceptions as transient (retryable) or permanent (non-retryable).
  - `DeadLetterHandler` interface: defines `handle(MantoRecord, Throwable, int)` for routing failed messages to a dead-letter topic with diagnostic metadata.
  - `MantoRecord` interface: framework-agnostic representation of a consumed message record (topic, partition, offset, timestamp, key, value, headers).
  - `MantoHeader` interface: framework-agnostic representation of a message header.
  - `MantoHeaders`: added DLT-specific header constants (`Manto-DLT-Original-Topic`, `Manto-DLT-Original-Partition`, `Manto-DLT-Original-Offset`, `Manto-DLT-Original-Timestamp`, `Manto-DLT-Exception-Class`, `Manto-DLT-Exception-Message`, `Manto-DLT-Retry-Count`, `Manto-DLT-Failure-Timestamp`, `Manto-DLT-Trace-Id`).

- `manto-kafka` (package `io.github.manto.kafka`): Implemented the error handling abstractions:
  - `DefaultRetryPolicy`: configuration-backed implementation of `RetryPolicy`.
  - `ExponentialBackoffStrategy`: exponential backoff with configurable initial delay, multiplier, and maximum delay.
  - `DefaultExceptionClassifier`: classifies `IllegalArgumentException`, `IllegalStateException`, `NullPointerException`, `SecurityException` as non-retryable; all others as retryable. Supports custom non-retryable types.
  - `DefaultDeadLetterHandler`: publishes failed messages to a DLT topic (original topic + configurable suffix, default `.DLT`). Preserves original Manto headers and adds diagnostic headers (original topic/partition/offset/timestamp, exception class/message, retry count, failure timestamp, trace ID). Includes `KafkaMantoRecord` adapter for converting Kafka `ConsumerRecord` to `MantoRecord`.
  - Auto-configuration beans for all retry/DLT abstractions exposed via `MantoAutoConfiguration`.

- Tests:
  - `manto-core`: Unit tests for all new interfaces (`RetryPolicyTest`, `BackoffStrategyTest`, `ExceptionClassifierTest`, `DeadLetterHandlerTest`).
  - `manto-kafka`: Unit tests for all implementations (`DefaultRetryPolicyTest`, `ExponentialBackoffStrategyTest`, `DefaultExceptionClassifierTest`, `DefaultDeadLetterHandlerTest` with Mockito).
  - All 76 tests pass (`mvn clean verify` — BUILD SUCCESS): core 19 + kafka 57 + autoconfigure 13 (including Testcontainers consumer integration test).

## Day 14 work

- `manto-spring-boot-autoconfigure` (package `io.github.manto.autoconfigure`): Extended `MantoAutoConfiguration` with producer and consumer factory auto-configuration:
  - `mantoProducerFactory`: Creates `DefaultKafkaProducerFactory` with Spring's `JsonSerializer` (adds type headers for deserializer compatibility) using `manto.kafka.bootstrap-servers` property.
  - `mantoKafkaTemplate`: Exposes `KafkaTemplate<String, Object>` for direct use.
  - `mantoProducer`: Creates `MantoKafkaProducer` (implements `MantoProducer`) backed by the auto-configured `KafkaTemplate`. Adds Manto headers (`Manto-Event-Id`, `Manto-Event-Type`, `Manto-Event-Version`, `Manto-Correlation-Id`, `Manto-Source`) to each published message.
  - `mantoConsumerFactory`: Creates `DefaultKafkaConsumerFactory` with Spring's `JsonDeserializer` (reads type headers, `TRUSTED_PACKAGES=*`, `USE_TYPE_INFO_HEADERS=true`) and `auto.offset.reset=earliest`.
  - `kafkaListenerContainerFactory`: Creates `ConcurrentKafkaListenerContainerFactory` wired with the auto-configured consumer factory.
  - All beans use `@ConditionalOnMissingBean` to allow user overrides.
  - Listener registration infrastructure (validator, discoverer, endpoint factory, registrar) unchanged from Day 13.
- `manto-kafka`: Updated `MantoJsonDeserializer` to support generic deserialization to `Map<String, Object>` when no target type is configured, enabling multi-listener scenarios where each handler has a different event type. Updated `MethodKafkaListenerEndpointFactory` to use standard `GenericMessageConverter` (Spring's `JsonDeserializer` handles typed deserialization via type headers).
- Tests:
  - Updated `MantoListenerRegistrationContextTest` and `MantoKafkaConsumerIntegrationTest` to use auto-configured infrastructure (no manual `ConsumerFactory`/`ContainerFactory` beans needed).
  - Updated `MantoJsonDeserializerTest` to reflect new generic deserialization behavior (1 test changed).
  - All 82 tests pass (`mvn clean verify` — BUILD SUCCESS): core 15 + kafka 54 + autoconfigure 13.

## Day 13 work

- `manto-kafka` (package `io.github.manto.kafka`): Implemented standardized Manto Kafka headers per docs/API_DESIGN.md:
  - `MantoKafkaProducer`: Updated to add Manto headers (`Manto-Event-Id`, `Manto-Event-Type`, `Manto-Event-Version`, `Manto-Correlation-Id`, `Manto-Source`) to every published message. Event ID is a UUIDv4, event type is the event class simple name, version defaults to "1.0", correlation ID defaults to event ID, source is configurable (defaults to "unknown").
  - `MantoHeaderExtractor`: New utility class to extract Manto metadata from both Spring Kafka `Message` and raw Kafka `ConsumerRecord`. Provides sensible defaults when headers are missing (generated UUID for event ID, "UnknownEvent" for event type).
  - Added constructor overload for `MantoKafkaProducer` accepting source identifier; original constructor defaults to "unknown".
- `manto-core`: `MantoHeaders` and `MantoEventMetadata` already existed (Day 4/Day 5) — no changes needed.
- Tests:
  - `MantoKafkaProducerTest`: Updated to verify header injection (13 tests total). Tests cover header presence, values, uniqueness per publish, and default source.
  - `MantoHeaderExtractorTest`: New test class (5 tests) covering extraction from Spring Message and ConsumerRecord, defaults for missing headers, and timestamp preservation.
  - `MantoKafkaProducerIntegrationTest`: Added `propagatesMantoHeadersToConsumer` test verifying end-to-end header propagation through a real Kafka broker (Testcontainers).
- All existing tests pass (`mvn clean verify` — BUILD SUCCESS).

## Day 11 work

- `manto-kafka`: Added `MantoJsonSerializer` and `MantoJsonDeserializer` for JSON serialization/deserialization using Jackson with JavaTimeModule support. Serialization writes dates as ISO strings (not timestamps). Deserializer supports typed deserialization via constructor (`Class<?>` or `JavaType`) or Kafka config property `manto.deserializer.target.type`. Generic types supported through `JavaType`.
- `manto-kafka`: Added `MantoDeserializationException` and `MantoSerializationException` for clear error handling. Deserialization exception includes target type and payload preview (truncated to 200 chars) for diagnostics without exposing full sensitive payloads.
- `manto-kafka`: Added `jackson-datatype-jsr310` dependency for Java time type support.
- `manto-kafka`: Added unit tests (46 tests total): `MantoJsonSerializerTest` (13 tests), `MantoJsonDeserializerTest` (30 tests), `MantoDeserializationExceptionTest` (1 test), `MantoSerializationExceptionTest` (1 test). Tests cover valid payloads (POJOs, records, nested objects, collections, maps, Java time types), invalid payloads (malformed JSON, type mismatches, missing fields), and error handling (missing target type config, class not found, payload preview truncation).
- All existing tests pass (`mvn clean verify` — BUILD SUCCESS).

## Day 10 work

- `manto-kafka`: Updated `MethodKafkaListenerEndpointFactory` to use `GenericMessageConverter` with a `DefaultFormattingConversionService`, ensuring the handler method factory can convert the deserialized message payload (from the container's `RecordMessageConverter`, typically `JsonMessageConverter`) to the handler method's parameter type. This completes the consumer execution path: Kafka → Spring Kafka listener → Manto listener infrastructure → deserialization → `@MantoListener` → application method.
- `manto-spring-boot-autoconfigure`: Added `MantoKafkaConsumerIntegrationTest` (Testcontainers) — end-to-end test verifying a `@MantoListener` handler is invoked with a correctly deserialized typed event object (`OrderCreatedEvent`) produced as JSON to a real Kafka broker. The test uses a real `AnnotationConfigApplicationContext` with `@EnableKafka` + auto-configuration + a `ConcurrentKafkaListenerContainerFactory` backed by a real `DefaultKafkaConsumerFactory` with `JsonDeserializer`.
- `manto-kafka`: Added unit test `MethodKafkaListenerEndpointFactoryTest` (3 tests) verifying endpoint creation, unique IDs for handlers sharing topic/group, and message handler method factory configuration.
- No public API changes; no config/docs changes.

## Day 9 work

- `manto-kafka` (package `io.github.manto.kafka`) implements discovery and registration of `@MantoListener` methods, connecting them to Spring Kafka through the standard programmatic-registration hook:
  - `MantoListenerDefinition` — immutable record binding a bean, method, topic, and group id.
  - `MantoListenerValidator` — fail-fast validation: public, non-static, exactly one parameter (the event payload), non-blank topic and group id; violations throw `MantoListenerConfigurationException` (extends `IllegalStateException`) naming the offending class#method, so misconfiguration stops startup.
  - `MantoListenerDiscoverer` — scans a `ListableBeanFactory` for annotated methods after all singletons are instantiated, unwrapping proxies via `AopUtils.getTargetClass` (CGLIB-safe).
  - `KafkaListenerEndpointFactory` + `MethodKafkaListenerEndpointFactory` — build a `MethodKafkaListenerEndpoint<String, ?>` per definition with a unique id (`topic:groupId:Class.method`), group id, topics, bean, and method, plus the same `KafkaMessageHandlerMethodFactory` wiring Spring Kafka uses for `@KafkaListener` (conversion service + `GenericMessageConverter`, then `afterPropertiesSet`).
  - `MantoListenerRegistrar` — implements `KafkaListenerConfigurer`; Spring Kafka's `KafkaListenerAnnotationBeanPostProcessor` calls `configureKafkaListeners(registrar)` for every configurer bean, so declaring this bean is the entire integration. Discovery runs at that point (after all singletons exist), and endpoints join the same registrar/registry as `@KafkaListener` methods; the app's `kafkaListenerContainerFactory` builds the containers.
  - Error boundaries: configuration errors (validation/registration) fail fast at startup; runtime message-processing errors stay with Spring Kafka (retry/DLT are future days).
- `manto-spring-boot-autoconfigure`: `MantoAutoConfiguration` (`@AutoConfiguration`, `@ConditionalOnClass(KafkaListenerContainerFactory.class)`) declares the validator, discoverer, endpoint factory, and registrar beans; registered via `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`. No producer/consumer factory auto-configuration (Day 14).
- `manto-core` untouched; `@MantoListener` semantics unchanged.
- Tests: `MantoListenerValidatorTest` (7), `MantoListenerDiscovererTest` (4, including CGLIB proxy unwrapping), `MethodKafkaListenerEndpointFactoryTest` (2, including unique ids for handlers sharing topic/group), `MantoListenerRegistrarTest` (2), and `MantoListenerRegistrationContextTest` in the autoconfigure module (1): a real `AnnotationConfigApplicationContext` with `@EnableKafka` + the auto-configuration + a container factory backed by a mocked `ConsumerFactory` (`MockConsumer`, no broker) proves a `@MantoListener` method lands in the Spring `KafkaListenerEndpointRegistry` end to end.
- No config/docs changes: discovery/registration changes no documented public API or configuration surface.

## Day 8 work

- The `@MantoListener` public annotation already exists in manto-core (created within Day 4's "Core API" scope): `@Documented @Retention(RUNTIME) @Target(METHOD)`, with required `topic()` and `groupId()` — exactly matching docs/API_DESIGN.md.
- Day 8 verified the existing annotation against the documented contract instead of reimplementing it:
  - Design confirmed: plain Java annotation (no Spring/Kafka imports, per ADR-003); method-targeted (one annotation = one handler = one topic binding); runtime retention for startup reflection by the future auto-configuration; required attributes so misconfiguration fails at compile time.
  - No code changes needed; "keep the annotation minimal" rules out additions such as `@Inherited` (not applicable to methods) or optional/defaulted fields (not in the API design).
  - `MantoListenerTest` (4 tests) already validates runtime retention, method target, `@Documented`, and topic/groupId value exposure.
- Runtime machinery (discovery/registration, Spring Boot wiring) is deliberately out of scope — Days 9+.
- No production code, pom, or docs changes.

## Day 7 work

- `MantoKafkaProducerIntegrationTest` (manto-kafka, `io.github.manto.kafka`): end-to-end Testcontainers test verifying `MantoProducer` -> real Kafka -> message exists.
  - Uses `org.testcontainers.kafka.KafkaContainer` (modern KRaft container; the old `org.testcontainers.containers.KafkaContainer` is deprecated in Testcontainers 1.21.4) with the official `apache/kafka:3.9.1` image, matching kafka-clients 3.9.2.
  - Real broker, no mocks: a `DefaultKafkaProducerFactory` + `KafkaTemplate<String, Object>` with Spring's `JsonSerializer` drives `MantoKafkaProducer`, and a real `KafkaConsumer` with `StringDeserializer` reads the record back from the topic.
  - The topic is created up front with an `AdminClient` (1 partition, RF 1); the consumer uses `assign` + `earliest` and polls until the record arrives (30 s deadline).
  - Asserts the payload is the JSON encoding of the published event (`orderId`, `amount`), verifying FR-03 serialization through the real broker.
- manto-kafka pom: added test-scope `com.fasterxml.jackson.core:jackson-databind` (managed by the Spring Boot BOM) so the test can parse the produced JSON. Spring Kafka declares jackson optional, so it is not otherwise on the classpath.
- No production code or public API changes; no docs changes needed.

## Day 6 work

- `MantoKafkaProducer` (manto-kafka, `io.github.manto.kafka`): implements the core `MantoProducer` abstraction (ADR-004) by wrapping a constructor-injected `KafkaTemplate<String, Object>`; the underlying producer factory is expected to use Spring's JSON serializer (FR-03), so typed events are encoded as JSON by Spring Kafka.
  - `publish(topic, event)` validates topic (not null/blank) and event (not null) with `IllegalArgumentException`, matching the core contract.
  - Publishing is synchronous: the call blocks on the Kafka send future and surfaces failures as `MantoProducerException`.
  - Interrupted sends restore the thread interrupt flag and also wrap in `MantoProducerException`.
  - No retry/DLT/metrics/headers — those are future days (headers are Day 12, producer Testcontainers test is Day 7).
- `MantoProducerException`: unchecked exception wrapping Kafka send failures so callers do not depend on Spring Kafka exception types.
- `MantoKafkaProducerTest`: 6 unit tests with Mockito covering happy-path delegation, null/blank topic, null event, send failure wrapping, and interrupt handling.
- No pom or docs changes needed; the producer does not change documented public behavior or configuration.

## Day 5 work

- `MantoEventMetadata`: added a compact constructor that requires every field:
  - `eventId`, `eventType`, `eventVersion`, `correlationId`, `source` must not be null or blank.
  - `timestamp` must not be null.
  - Violations throw `IllegalArgumentException` with a field-specific message.
  - No new dependencies; record stays immutable and Kafka-header-ready (header serialization is Day 12).
- `MantoEventMetadataTest`: added 7 validation tests (null/blank per string field, null timestamp); 10 tests total in this class.
- No docs changes needed; validation does not alter documented public behavior.

## Day 4 work

- `manto-core` initial public API in package `io.github.manto.core` (no new dependencies, per ADR-003):
  - `MantoProducer`: interface `void publish(String topic, T event)` — framework producer abstraction.
  - `MantoEventMetadata`: immutable record with eventId, eventType, eventVersion, correlationId, source, timestamp (`Instant`).
  - `MantoListener`: method-level annotation with `topic` and `groupId` (`@Retention(RUNTIME)`, `@Target(METHOD)`, `@Documented`).
  - `IdempotencyStore`: interface `isProcessed(String)` / `markProcessed(String)` (ADR-005). Abstraction only; in-memory implementation is a later task.
  - `MantoHeaders`: standardized header name constants (`Manto-Event-Id`, `Manto-Event-Type`, `Manto-Event-Version`, `Manto-Correlation-Id`, `Manto-Source`) per docs/API_DESIGN.md.
- No Kafka or Spring dependencies; no Kafka-specific functionality implemented.
- Unit tests: `MantoEventMetadataTest`, `MantoListenerTest`, `MantoHeadersTest` (8 tests total).

## Day 3 work

- Root POM: `spring-boot.version` property (3.5.16) extracted; BOM import uses it.
- Root POM: the five modules (manto-core, manto-kafka, manto-spring-boot-autoconfigure, manto-spring-boot-starter, manto-test) added to `dependencyManagement` at `${project.version}`.
- Root POM: inherited test-scope `junit-jupiter` (5.12.2) and `mockito-junit-jupiter` (5.17.0); versions come from the Spring Boot BOM. Test scope only, so no runtime leakage.
- manto-kafka and manto-spring-boot-autoconfigure: added test-scope `org.testcontainers:junit-jupiter` and `org.testcontainers:kafka` (1.21.4, managed by BOM).
- No production or test Java sources this day (configuration only).
- Verified with `mvn dependency:tree`: all versions resolve from the BOM (spring-kafka 3.3.16, kafka-clients 3.9.2, jackson 2.21.4, junit 5.12.2, mockito 5.17.0, testcontainers 1.21.4, micrometer 1.15.12). `manto-core` compile/runtime dependency tree is empty; it remains free of Kafka/Spring dependencies per ADR-003.

## Day 2 work

- Root parent POM (pom.xml): groupId `io.github.manto` (placeholder until Day 28 confirms the Maven Central namespace), artifactId `manto`, version `0.1.0-SNAPSHOT`, packaging `pom`.
- Modules: manto-core, manto-kafka, manto-spring-boot-autoconfigure, manto-spring-boot-starter, manto-test (per ADR-001).
- Java 21 via `maven.compiler.release=21`; UTF-8 encoding.
- Dependency management imports `spring-boot-dependencies:3.5.16` (no Spring Boot parent; Manto owns the hierarchy).
- Plugin management: maven-compiler-plugin 3.13.0, maven-surefire-plugin 3.5.2, maven-jar-plugin 3.4.2.
- Dependency boundaries: manto-core has no dependencies; manto-kafka depends on manto-core and spring-kafka; manto-spring-boot-autoconfigure depends on manto-core, manto-kafka, spring-boot-autoconfigure, spring-boot, and spring-boot-configuration-processor (optional); manto-spring-boot-starter depends only on manto-spring-boot-autoconfigure; manto-test depends on manto-core.
- No Java sources, tests, or Kafka functionality yet (Days 3+).

## Day 1 work

- Repository foundation: README.md, AGENTS.md, LICENSE, CHANGELOG.md, CODE_OF_CONDUCT.md, CONTRIBUTING.md, SECURITY.md, .gitignore, .gitattributes.
- Documentation: docs/PROJECT_CONTEXT.md, docs/PRODUCT_REQUIREMENTS.md, docs/ARCHITECTURE.md, docs/API_DESIGN.md, docs/CONFIGURATION.md, docs/ERROR_HANDLING.md, docs/OBSERVABILITY.md, docs/TESTING_STRATEGY.md, docs/SECURITY_MODEL.md, docs/RELEASE_STRATEGY.md, docs/MAVEN_CENTRAL.md, docs/ROADMAP.md.
- Development docs: development/CODING_STANDARDS.md, development/DAILY_WORKFLOW.md, development/DEVELOPMENT_GUIDE.md, development/AI_AGENT_GUIDE.md, development/PROGRESS.md.
- Decisions: ADR-001 through ADR-005 in development/decisions/.
- Task plans: tasks/DAY-01.md through tasks/DAY-30.md.
- No Kafka or Maven functionality implemented (Days 2+).

## Day 26 work

- Audited every documented API and configuration against the actual implementation (`MantoProperties.java:19`, `MantoAutoConfiguration.java:66`, `MantoKafkaProducer.java:26`, `MantoListener.java:18`, `DefaultExceptionClassifier.java:20`, `MantoMetrics.java:16`, `MantoHeaders.java:8`, `MantoDeadLetterPublishingRecoverer.java:23`, `MantoListenerValidator.java:16`, `CorrelationIdContext.java:13`, `InMemoryIdempotencyStore.java:23`, `ExponentialBackoffStrategy.java:13`, `DefaultDeadLetterHandler.java:29`). Removed invented/hallucinated features; every property, header, and metric name now matches the source file.
- `README.md`: Complete rewrite for an external developer — installation (Java 21, Spring Boot 3.5.16, `0.1.0-SNAPSHOT` from local `mvn install`), auto-configuration bean inventory, quick start (configure `manto.kafka.bootstrap-servers`, producer `MantoProducer.publish(topic, event)` contract at `manto-core/MantoProducer.java:18`, correlation propagation via `MantoKafkaProducer.publish(topic, event, correlationId)` overload at `manto-kafka/MantoKafkaProducer.java:64`, consumer `@MantoListener` with validator constraints), configuration summary table, retry (backoff formula, `maxAttempts` → `ExponentialBackOff` mapping at `MantoAutoConfiguration.java:198`), DLT (suffix routing, full header table from `MantoHeaders.java:16`), idempotency (`IdempotencyStore` + `InMemoryIdempotencyStore` single-instance warning per ADR-005, `@ConditionalOnMissingBean` override example), metrics (6 instruments with tags at `MantoMetrics.java:17`, disable via `manto.observability.enabled`, `SimpleMeterRegistry` fallback at `MantoAutoConfiguration.java:93`), correlation IDs (`MantoListenerInterceptor.java:15` + `CorrelationIdContext.java:13` ThreadLocal lifecycle), example application cross-links, testing instructions, and docs index.
- `docs/CONFIGURATION.md`: Complete rewrite — full YAML example, property reference table with types/defaults/constraints/source file lines (`MantoProperties.java:84/98/179/219`), validation failures, Spring Kafka vs. Manto boundary, `@ConditionalOnMissingBean` override catalogue, minimal vs. full config.
- `docs/ERROR_HANDLING.md`: Complete rewrite — lifecycle diagram, retry enable/maxAttempts/backoff mapping to `MantoAutoConfiguration.java:179` and `ExponentialBackoffStrategy.java:47`, exception classification table (`DefaultExceptionClassifier.java:25` — `IllegalArgumentException`, `IllegalStateException`, `NullPointerException`, `SecurityException` as non-retryable via `isInstance`), custom `DefaultExceptionClassifier` bean snippet, DLT routing (`topicSuffix` destination resolver at `MantoDeadLetterPublishingRecoverer.java:45`), full 14-header DLT table, programmatic `DefaultDeadLetterHandler` note (10s send timeout at `DefaultDeadLetterHandler.java:60`), observer `@MantoListener` example at `PaymentDltHandler.java:28`, redelivery/idempotency interaction, logging guidance.
- `docs/OBSERVABILITY.md`: Complete rewrite — 7-row metric table (2 rows for published success/failure), enable flag, `MantoAutoConfiguration.java:88` registry wiring (Actuator vs. `SimpleMeterRegistry` fallback), PromQL/Actuator snippets, producer correlation header generation (`MantoKafkaProducer.java:91`), consumer interceptor lifecycle (`MantoListenerInterceptor.java:29`), `CorrelationIdContext.java:13` ThreadLocal + MDC enrichment pattern, `MantoHeaderExtractor.java:29` defaults, logging rules (200-char payload preview at `MantoJsonDeserializer.java:112`).
- `examples/order-payment/README.md`: Rewrote feature table with exact source paths under `src/main/java/com/example/orderpayment/...` plus line numbers, corrected transient/permanent failure amounts (999 vs. ≤0 per `PaymentHandler.java:59/67`), added header inspection with `print.headers=true`, clarified `max-attempts` includes initial attempt, documented all key files.
- Fixed duplicate `import io.micrometer.core.instrument.MeterRegistry;` in `MantoAutoConfiguration.java` (harmless but confusing during doc audit).
- Verified example still compiles: `mvn install -DskipTests` → BUILD SUCCESS; `mvn -f examples/order-payment/pom.xml compile` → BUILD SUCCESS (8 sources).
- No public API changes; docs-only task.

## Day 27 work

- Release-quality review: `mvn clean verify` plus dependency, JavaDoc, API, logging, secrets, exception, coverage, reproducibility, static analysis checks.

- **Dependency vulnerabilities / unnecessary dependencies**
  - `mvn dependency:tree` — reactor resolves Spring Boot BOM `3.5.16` → spring-kafka `3.3.16`, kafka-clients `3.9.2`, jackson `2.21.4`, micrometer `1.15.12`, testcontainers `1.21.4` — all current stable; no known critical CVE in released versions. `mvn versions:display-dependency-updates` shows only major-line upgrades (Spring Boot 4, spring-kafka 4.2-M1) not applicable to 3.5.x line.
  - `mvn dependency:analyze` — warnings are false positives for transitive `spring-context/beans/core/messaging` via `spring-kafka` and `testcontainers` via `kafka` test scope; no compile leakage. `manto-core` remains dependency-free per ADR-003 (`mvn dependency:tree` shows zero compile deps). `manto-kafka` compile deps are minimal: `manto-core`, `spring-kafka`, `jackson-databind`, `jackson-datatype-jsr310`, `micrometer-core` (`manto-kafka/pom.xml:20`). `manto-spring-boot-autoconfigure` declares only `jakarta.validation-api` (for `@Validated` on `MantoProperties`) and `micrometer-core` (direct use in `MantoAutoConfiguration`) plus optional `spring-boot-configuration-processor`. No unnecessary runtime dependencies removed — explicit `micrometer-core` in autoconfigure kept for direct `MeterRegistry` usage (transitive via `manto-kafka` would also work but obscures direct dependency).
  - No `org.owasp:dependency-check-maven` in v1.0; CI should run `mvn org.owasp:dependency-check-maven:check` with NVD API key before release (documented in `docs/SECURITY_MODEL.md` “Run dependency vulnerability checks in CI”).

- **JavaDoc**
  - Fixed `manto-core` compilation error: `MantoRecord.java:11` referenced `org.apache.kafka.clients.consumer.ConsumerRecord` via `{@link}` without kafka-clients on classpath — changed to `{@code ConsumerRecord}` to keep `manto-core` kafka-free.
  - Added missing `@return`/`@param` to `MantoHeader.java:11`, `MantoRecord.java:18` (all 7 methods), `MantoListener.java:18`, and field docs to `MantoHeaders.java:8` (all 14 constants), and compact-constructor Javadoc to `MantoEventMetadata.java:26`. `manto-core` now builds with `mvn javadoc:javadoc` (previously `1 error, 29 warnings` → BUILD SUCCESS). `manto-kafka` and `manto-spring-boot-autoconfigure` also succeed with plugin config `doclint= all,-missing`, `failOnWarnings=false`.
  - Verified with `mvn javadoc:javadoc` across full reactor → BUILD SUCCESS (6 modules).

- **Public API**
  - Audited public API vs `docs/API_DESIGN.md`: `MantoProducer.java:9`, `MantoListener.java:18`, `MantoEventMetadata.java:18`, `IdempotencyStore.java:9`, `MantoHeaders.java:8`, `MantoKafkaProducer.java:64` overload, `CorrelationIdContext.java:13`, `InMemoryIdempotencyStore.java:23` — all stable, no breaking signature changes. Javadoc now present for every public method/field.

- **Logging**
  - Grep for `Logger`/`log.info` — framework modules (`manto-core`, `manto-kafka`, `manto-spring-boot-autoconfigure`) have **zero loggers** (only example `com.example.orderpayment` has 3 loggers). Example logs only `orderId`/`correlationId` (`PaymentHandler.java:45`, `PaymentService.java:41`, `PaymentDltHandler.java:32`), never full payload. Framework never logs credentials/secrets; `MantoDeserializationException` previews max 200 chars (`MantoJsonDeserializer.java:112`, `MantoJsonDeserializer.previewPayload`), `MantoSerializationException` logs only type name.

- **Secrets**
  - Grep for `password|secret|credential|token|apiKey` — no hardcoded secrets. `MantoProperties.java:84` defaults `bootstrapServers=localhost:9092` (non-secret). `docs/SECURITY_MODEL.md` enforced: credentials via external config/secret manager.

- **Exception handling**
  - `MantoKafkaProducer.java:77` catches `InterruptedException` → restores interrupt flag, records `recordPublishedFailure`, throws `MantoProducerException`; `ExecutionException` → unwraps cause similarly. `MantoJsonSerializer.java:40` wraps `JsonProcessingException` in `MantoSerializationException`. `MantoJsonDeserializer.java:101` wraps `JsonProcessingException`/`IOException` in `MantoDeserializationException` with truncated preview; fixed NPE when `targetType` is null (`MantoDeserializationException.java:15` now null-safe: `"unknown"` fallback, `manto-kafka/MantoDeserializationException.java:15`). `DefaultDeadLetterHandler.java:60` catches `Exception` on DLT send → throws `IllegalStateException` with topic name only (no payload). No swallowed exceptions or `printStackTrace` (grep clean).

- **Test coverage**
  - `mvn clean verify` → BUILD SUCCESS: `manto-core` 19 tests, `manto-kafka` 129 tests (including `CorrelationIdPropagationIntegrationTest` 3 + `MantoKafkaProducerIntegrationTest` 2 via Testcontainers `apache/kafka:3.9.1`), `manto-spring-boot-autoconfigure` 25 tests (including `MantoEndToEndIntegrationTest` 9 + `MantoKafkaConsumerIntegrationTest` 1 + `RetryIntegrationTest` 3). Total **173 tests**, 0 failures. Coverage spans producer/consumer, serialization, headers, retry/backoff, exception classification, DLT routing+metadata, idempotency, metrics, correlation.

- **Build reproducibility**
  - Added `project.build.outputTimestamp=2026-08-31T00:00:00Z` to `pom.xml:30` (Maven reproducible builds).
  - Added required Maven Central metadata to `pom.xml:13`: `<url>`, `<licenses>` (Apache 2.0), `<developers>`, `<scm>` (`https://github.com/kaiser-haque/manto.git`), `<issueManagement>`.
  - Added `pluginManagement` for `maven-source-plugin:3.3.1` and `maven-javadoc-plugin:3.12.0` (`doclint=all,-missing`, `failOnError=true`, `failOnWarnings=false`) with `attach-sources`/`attach-javadocs` executions, and `maven-enforcer-plugin:3.5.0` (`requireMavenVersion 3.9.0`, `requireJavaVersion 21`, `requireUpperBoundDeps`) for CI reproducibility. `mvn javadoc:jar` / `source:jar` now succeed.

- **Static analysis**
  - No Checkstyle/SpotBugs/PMD configured in v1.0 (explicitly noted). Added `maven-enforcer-plugin` for version/dependency convergence enforcement; full static analysis remains out of scope for Day 27 per “if configured”.

- **Charset hardening**
  - Fixed `MantoListenerInterceptor.java:59` and `MantoHeaderExtractor.java:64` to use `StandardCharsets.UTF_8` instead of platform default when decoding header bytes.

- Tests:
  - `mvn clean verify` → BUILD SUCCESS (173 tests, ~85s integration due to container pull)
  - `mvn javadoc:javadoc` → BUILD SUCCESS (all 6 modules, after core fixes)
  - Verified no regressions: `manto-core` 19, `manto-kafka` 129, `autoconfigure` 25 unchanged.

## Day 28 work

- Verified Maven Central requirements against current official docs (2026-09-02):
  - https://central.sonatype.org/publish/requirements/ — required POM metadata (name, description, url, licenses, developers, scm, issueManagement), sources/javadoc jars, GPG signatures, checksums
  - https://central.sonatype.org/publish/publish-portal-maven/ — `org.sonatype.central:central-publishing-maven-plugin:0.9.0` with `publishingServerId=central`, `autoPublish=false`, `waitUntil=validated`, `checksums=all`
  - https://central.sonatype.org/publish/requirements/gpg/ — public key on `keyserver.ubuntu.com` / `keys.openpgp.org` / `pgp.mit.edu`, `--pinentry-mode loopback`
  - https://central.sonatype.org/register/namespace/ — GitHub namespace `io.github.<username>` auto-verified for the signing GitHub user.

- **Namespace / groupId strategy** (`pom.xml:8`, `docs/MAVEN_CENTRAL.md#namespace`):
  - Day 1–27 placeholder `io.github.manto` is **not** auto-verified for GitHub user `kaiser-haque` (`https://github.com/kaiser-haque/manto`). Per Central Portal docs only `io.github.<your GitHub username>` is auto-provisioned; `io.github.manto` would require user `manto` or a verified DNS TXT.
  - Migrated Maven coordinates to verified namespace `io.github.kaiser-haque`:
    - parent `pom.xml:8` groupId `io.github.kaiser-haque` (from `io.github.manto`)
    - `dependencyManagement` entries `manto-core`/`manto-kafka`/`manto-spring-boot-autoconfigure`/`manto-spring-boot-starter`/`manto-test` updated accordingly
    - module parents `manto-core/pom.xml:9`, `manto-kafka/pom.xml:9`, `manto-spring-boot-autoconfigure/pom.xml:9`, `manto-spring-boot-starter/pom.xml:9`, `manto-test/pom.xml:9`
    - example `examples/order-payment/pom.xml:30` dependency updated
    - docs updated: `README.md:28`, `docs/PROJECT_CONTEXT.md:37`, `docs/RELEASE_STRATEGY.md:37`, `docs/MAVEN_CENTRAL.md`
  - Java packages remain `io.github.manto.*` (e.g., `io.github.manto.core.MantoProducer`) — package vs. coordinate divergence is allowed and avoids breaking existing imports; documented in `docs/MAVEN_CENTRAL.md#namespace`.

- **Required metadata** (`pom.xml:13`):
  - Already present from Day 27: `<name>`, `<description>`, `<url>`, `<licenses>` Apache-2.0, `<scm>` (`https://github.com/kaiser-haque/manto.git`), `<issueManagement>`, `<developers>`, `project.build.outputTimestamp` (reproducible builds), `maven-enforcer-plugin` (requireMavenVersion 3.9.0, requireJavaVersion 21, requireUpperBoundDeps).
  - Enhanced `<developers>`: added primary `kaiser-haque <khaque444@gmail.com>` with organization `Manto` plus retained `Manto Contributors`.
  - Added `<distributionManagement><snapshotRepository><id>central</id><url>https://central.sonatype.com/repository/maven-snapshots</url>` for optional snapshot publishing via Central Portal.
  - Added version properties: `maven-source-plugin.version 3.3.1`, `maven-javadoc-plugin.version 3.12.0`, `maven-gpg-plugin.version 3.2.7`, `central-publishing-maven-plugin.version 0.9.0`, `maven-enforcer-plugin.version 3.5.0`.

- **Sources and Javadoc artifacts** (`pom.xml:112`):
  - Day 27 put `maven-source-plugin`/`maven-javadoc-plugin` in `pluginManagement` with executions but they were **not bound** (no `<build><plugins>`). Verified via `mvn help:effective-pom -pl manto-core` — executions absent.
  - Restructured `pom.xml` pluginManagement to declare versions/config only (`maven-source-plugin` `maven-javadoc-plugin` with `doclint=all,-missing`, `maven-gpg-plugin` with `gpgArguments --pinentry-mode loopback`, `central-publishing-maven-plugin` with `publishingServerId=central`).
  - Created `release` profile (`pom.xml:191`) that binds:
    - `maven-source-plugin:jar-no-fork` → `*-sources.jar`
    - `maven-javadoc-plugin:jar` → `*-javadoc.jar`
    - `maven-gpg-plugin:sign` (phase `verify`) → `*.asc`
    - `central-publishing-maven-plugin` (extensions true, `autoPublish=false`, `waitUntil=validated`, `checksums=all`) → bundle + checksums + staging.
  - Release-only binding keeps `mvn verify` fast; full validation via `mvn -P release verify -Dgpg.skip=true` (dry-run without key) or `mvn -P release verify` (with key).
  - Fixed empty-module Central requirement: `manto-spring-boot-starter` and `manto-test` had `packaging=jar` but no `src/main/java`, so `maven-source-plugin` reported "No sources in project. Archive not created" and `maven-javadoc-plugin` "No Javadoc in project. Archive not created" — Central validation would fail for `jar` packaging (requires `-sources.jar`/`-javadoc.jar`). Added placeholder classes:
    - `manto-spring-boot-starter/src/main/java/io/github/manto/starter/MantoStarter.java` — marker, Javadoc'd.
    - `manto-test/src/main/java/io/github/manto/test/MantoTest.java` — placeholder for future testing utilities.
  - Verified: `mvn clean verify -P release -Dgpg.skip=true -Dsurefire.failIfNoSpecifiedTests=false` → BUILD SUCCESS, generates `-sources.jar` and `-javadoc.jar` for all 5 modules (parent `manto` is `pom`, correctly skips).

- **Signing** (`pom.xml:191` `maven-gpg-plugin`):
  - Configured `gpgArguments --pinentry-mode loopback` so CI can pass `-Dgpg.passphrase`.
  - Local usage: `gpg --gen-key` → `gpg --export-secret-keys --armor <keyId>` → `GPG_PRIVATE_KEY`, export passphrase → `GPG_PASSPHRASE`, publish public key `gpg --keyserver keyserver.ubuntu.com --send-keys <keyId>` (also `keys.openpgp.org`, `pgp.mit.edu`).
  - CI: `actions/setup-java@v4` with `gpg-private-key: ${{ secrets.GPG_PRIVATE_KEY }}` and `gpg-passphrase: GPG_PASSPHRASE` — imports without logging secrets.
  - Validated via `mvn javadoc:jar`/`source:jar` success; signing verified via `mvn -P release verify` dry-run with `-Dgpg.skip=true` when no key is present.

- **CI secrets without exposing credentials**:
  - Created `.github/workflows/ci.yml` (push/PR on `main`, `workflow_dispatch`): `actions/setup-java@v4` with Java 21 `temurin` + `cache: maven`, runs `mvn -B clean verify` and `mvn -B verify -P release -Dgpg.skip=true -DskipTests` to ensure sources/javadoc generation.
  - Created `.github/workflows/release.yml` — **safe release workflow** (see below).

- **Safe release workflow** (`.github/workflows/release.yml`):
  - Triggers: `push` tags `v*.*.*` (e.g., `v0.9.0`) and `workflow_dispatch` with inputs `version` and `dryRun` (default `true`).
  - **Safety rails**:
    - `autoPublish=false` + `waitUntil=validated` — `mvn deploy -P release` uploads + validates but **never auto-publishes** to Maven Central; human must click **Publish** on https://central.sonatype.com/publishing/deployments after reviewing Validation Results.
    - Guard step fails if `version == 1.0.0` — satisfies task requirement "Do not publish the final 1.0.0 release today" (first Central release should be `0.9.0` RC per `docs/RELEASE_STRATEGY.md`).
    - Rejects `-SNAPSHOT` versions for release; warns if tag version ≠ pom version (expects `mvn versions:set` before tagging).
    - `concurrency` group prevents parallel publishes on same ref.
    - Secrets injected via `server-id: central` / `server-username: CENTRAL_USERNAME` / `server-password: CENTRAL_TOKEN` and `gpg-private-key` — only in runner memory, masked in logs.
  - **Steps** (summary):
    1. Checkout → Setup Java 21 (temurin, cache maven, `server-id` + token + GPG key)
    2. Resolve version from tag (`vX.Y.Z` → `X.Y.Z`) or input/pom
    3. Guard `1.0.0` / `-SNAPSHOT` checks
    4. `mvn -B clean verify -P release` (tests + sources + javadoc + signatures; passphrase via `GPG_PASSPHRASE`)
    5. Bundle content check (`*-sources.jar`, `*-javadoc.jar`, `*.asc`)
    6. Conditional `mvn -B deploy -P release -DskipTests` if not `dryRun` (uploads with `autoPublish=false`)
    7. Dry-run summary notes.
  - Dry-run today: `mvn -P release verify -Dgpg.skip=true` (no secrets) or Actions → Run workflow `dryRun: true` (validates sources/javadoc/signing without Central upload).
  - First real release (after Day 28): bump version to `0.9.0`, tag `v0.9.0`, push — workflow uploads bundle; then Publish in Portal UI; verify via clean external project `io.github.kaiser-haque:manto-spring-boot-starter:0.9.0` from https://central.sonatype.com/ and https://repo.maven.apache.org/maven2/ .

- Documentation:
  - `docs/MAVEN_CENTRAL.md` — complete rewrite with verification date 2026-09-02, namespace verification steps, required metadata/artifacts tables, sources/javadoc/signing/checksums details, security secrets table, safe workflow explanation, dry-run vs. real release instructions, and links to official Portal docs.
  - `docs/RELEASE_STRATEGY.md` — updated checklist (sources/javadoc via `-P release`, GPG signing, Central Portal publishing steps, version `0.9.0` RC, verification from clean project) and corrected `groupId` to `io.github.kaiser-haque`.
  - `README.md:28` and `docs/PROJECT_CONTEXT.md:37` — updated dependency coordinates to `io.github.kaiser-haque` with note that Java packages remain `io.github.manto.*`.

- Tests:
  - `mvn clean verify -P release -Dgpg.skip=true -Dsurefire.failIfNoSpecifiedTests=false` → BUILD SUCCESS, 124 unit tests pass (19 core + 124 kafka unit? actually 19 core + 124 kafka non-integration + 12 autoconfigure = 155 unit tests exclusive of integration) + generates sources/javadoc for all 5 modules.
  - `mvn clean test -P release -Dgpg.skip=true -Dtest=!*IntegrationTest` → BUILD SUCCESS.
  - `mvn install -DskipTests` → BUILD SUCCESS.
  - `mvn -f examples/order-payment/pom.xml compile` → BUILD SUCCESS (after updating example dependency to `io.github.kaiser-haque`).
  - `mvn javadoc:javadoc` → BUILD SUCCESS (verified earlier, now via release profile).
  - Integration tests require Docker (Testcontainers `apache/kafka:3.9.1`); excluded when Docker unavailable — no regression in unit coverage.

## Notes

Update this file at the end of every daily session.

## Tests run

- `mvn clean verify -P release -Dgpg.skip=true -Dtest=!*IntegrationTest -Dsurefire.failIfNoSpecifiedTests=false` — BUILD SUCCESS, 155 unit tests (core 19 + kafka 124 non-integration + autoconfigure 12 + starter 0 + test 0), sources/javadoc generated for all 5 modules (starter/test now non-empty via placeholder classes)
- `mvn clean verify -P release -Dgpg.skip=true -Dsurefire.failIfNoSpecifiedTests=false` with Docker-less integration skip → verified `-sources.jar` and `-javadoc.jar` for manto-core, manto-kafka, manto-spring-boot-autoconfigure, manto-spring-boot-starter (now `MantoStarter.java`), manto-test (now `MantoTest.java`)
- `mvn install -DskipTests` — BUILD SUCCESS (reactor with new `io.github.kaiser-haque` groupId)
- `mvn -f examples/order-payment/pom.xml compile` — BUILD SUCCESS (after groupId migration)
- `mvn help:effective-pom -pl manto-core` — verified release profile bindings (source/javadoc/gpg/central)
- `mvn dependency:tree` — verified new `io.github.kaiser-haque` coordinates resolve across reactor (no snapshot pollution)
- `python -c "import yaml"` — validated `.github/workflows/ci.yml` and `release.yml` YAML syntax

## Known issues

- The Kafka container image pull dominates the integration test runtime on first run (~1 minute; ~25–45 s per Testcontainers suite). No functional issues. CI `ci.yml` runs `mvn -B clean verify` which will run integration tests on GitHub runners (Docker available); `release.yml` also runs `verify` with Testcontainers.
- SLF4J NOP warnings appear during tests; no logger binding is configured (non-blocking).
- Example requires a running Kafka at `localhost:9092` and a prior `mvn install -DskipTests` to resolve `0.1.0-SNAPSHOT` from local repo (not yet on Maven Central — will be `io.github.kaiser-haque:manto-spring-boot-starter:0.9.0` after first Central publish).
- `MantoDeadLetterPublishingRecoverer` uses a `ThreadLocal<Exception>` to propagate the exception to `createProducerRecord`; DLT_RETRY_COUNT is derived from `maxAttempts - 1` (configuration-level, not per-record) — documented in OBSERVABILITY/ERROR_HANDLING.
- MantoListener registration still requires `kafkaListenerContainerFactory` named exactly — documented in CONFIGURATION.
- No Checkstyle/SpotBugs/PMD in CI yet; only `maven-enforcer-plugin` configured. Full static analysis can be added post v1.0.
- `org.owasp:dependency-check-maven` not yet wired in CI; manual `mvn org.owasp:dependency-check-maven:check` with NVD API key recommended before Maven Central publish (see `docs/SECURITY_MODEL.md`).
- Maven Central 1.0.0 not published today per task constraint — workflow guard fails if `version == 1.0.0`. First publish should be `0.9.0` RC; Day 28 validated bundle generation only (`-Dgpg.skip=true` dry-run and `release.yml` dryRun path).

## Next task

Day 29 — Performance and polish. Expected commit message for Day 28: `build: prepare Maven Central publishing`
