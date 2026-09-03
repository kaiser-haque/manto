# Changelog

## 0.9.0 (Release Candidate)

- Version `0.1.0-SNAPSHOT` → `0.9.0` across the reactor (`pom.xml`, all module parents); `examples/order-payment` now consumes `0.9.0`.
- Release-blocking fixes found by a clean external-consumer test (separate Spring Boot project resolving `io.github.kaiser-haque:manto-spring-boot-starter:0.9.0` as a binary artifact against real Kafka via Testcontainers):
  - `manto-spring-boot-autoconfigure` now depends on `spring-boot-starter-validation` (compile) so `@Validated` `MantoProperties` binds in apps without `spring-boot-starter-web`; without it external startup failed with `NoProviderFoundException`. Test-scope `hibernate-validator`/`jakarta.el` removed (now transitive).
  - `MantoListenerInterceptor` now overrides `success`/`failure` so `manto.processing.duration` and `manto.messages.failed` are actually recorded at runtime and the correlation `ThreadLocal` is cleared on both paths, as documented in `docs/OBSERVABILITY.md`.
  - `MantoErrorHandler` now registers a `RetryListener` so `manto.messages.retried` increments on each failed delivery.
- Docs: `README.md`/`docs/PROJECT_CONTEXT.md` dependency snippets updated to `0.9.0`; `README.md`/`docs/OBSERVABILITY.md` failed-metric recording path corrected.
- Verified: external project 7/7 pass (auto-configuration, producer, consumer, retry, DLT, idempotency, metrics); `mvn clean verify` 180 tests pass; `mvn -P release verify -Dgpg.skip=true` builds sources/javadoc; example compiles against `0.9.0`.

## Unreleased

Development of Manto v1.0 is in progress (through Day 27).

### Added (Day 26 — Documentation)

- Complete user-facing documentation: `README.md` full rewrite (installation, auto-configuration, producer/consumer quick start, correlation propagation, retry/DLT/idempotency/metrics), `docs/CONFIGURATION.md` (full `manto.*` property reference with `MantoProperties.java:19` types/defaults), `docs/ERROR_HANDLING.md` (retry/backoff/DLT lifecycle, 14-header table), `docs/OBSERVABILITY.md` (6 metrics at `MantoMetrics.java:16`, correlation `CorrelationIdContext.java:13`), `examples/order-payment/README.md` (feature table, `docker run apache/kafka:3.9.1`, `curl` demos). Verified `mvn install -DskipTests` + `mvn -f examples/order-payment/pom.xml compile` BUILD SUCCESS.

### Added (Day 27 — Quality and Security) — `chore: harden release quality`

- Dependency audit (`mvn dependency:tree`/`analyze`, BOM 3.5.16, `manto-core` zero deps per ADR-003)
- JavaDoc fixes (`MantoRecord.java:11` kafka-free, `MantoHeader.java:11`, `MantoRecord.java:18`, `MantoListener.java:18`, `MantoHeaders.java:8`, `MantoEventMetadata.java:26`; `mvn javadoc:javadoc` BUILD SUCCESS)
- Public API stability (no breaking changes)
- Logging/secrets: framework zero loggers, example only `orderId/correlationId`, `MantoDeserializationException.java:15` 200-char preview, no hardcoded secrets
- Exception/charset hardening: NPE fix (`MantoDeserializationException.java:15`), `StandardCharsets.UTF_8` (`MantoListenerInterceptor.java:59`, `MantoHeaderExtractor.java:64`), interrupt restore (`MantoKafkaProducer.java:77`)
- Build reproducibility: `project.build.outputTimestamp` (`pom.xml:30`), Maven Central metadata (`pom.xml:13` – `<url>`, `<licenses>` Apache-2.0, `<scm>`, `<developers>`), `maven-source-plugin:3.3.1` + `maven-javadoc-plugin:3.12.0` + `maven-enforcer-plugin:3.5.0`
- Tests: `mvn clean verify` 173 tests (core 19 + kafka 129 + autoconfigure 25) via Testcontainers `apache/kafka:3.9.1`

### Planned

- Maven Central release (Day 28)
- Producer/Consumer already shipped; retry/DLT/idempotency/metrics/experiments verified
