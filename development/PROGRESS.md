# Manto Development Progress

## Current day

Day 9 — complete.

Next session: Day 10 — consumer (handler invocation).

## Current version

0.1.0-SNAPSHOT

## Completed

- [x] Repository foundation
- [x] Maven modules
- [x] Dependency management
- [x] Core API
- [x] Producer
- [x] Listener registration
- [ ] Consumer
- [ ] Retry
- [ ] DLT
- [ ] Idempotency
- [ ] Metrics
- [ ] Integration tests
- [ ] Documentation
- [ ] Maven Central release

## Current task

Day 9 — MantoListener discovery and registration.

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

## Notes

Update this file at the end of every daily session.

## Tests run

- `mvn -pl manto-kafka -am test` — BUILD SUCCESS, 22 tests (15 core + 22 kafka).
- `mvn -pl manto-spring-boot-autoconfigure -am test` — BUILD SUCCESS, 38 tests including the listener-registration context test.
- `mvn clean verify` — BUILD SUCCESS, all 6 reactor modules (core 15 + kafka 22 + autoconfigure 1 tests, including the Testcontainers producer integration test).

## Known issues

- The Kafka container image pull dominates the integration test runtime on first run (~1 minute; ~65 s total for the test). No functional issues.
- SLF4J NOP warnings appear during tests; no logger binding is configured (non-blocking).

## Next task

Day 10 — consumer (Kafka consumption and handler invocation). Expected commit message for Day 9: `feat: add listener registration`.
