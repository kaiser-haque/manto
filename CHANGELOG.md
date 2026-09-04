# Changelog

## 1.0.0 (Stable)

- First stable release published to Maven Central as `io.github.kaiser-haque:manto-spring-boot-starter:1.0.0` (Java packages remain `io.github.manto.*`).
- Version `0.9.0` → `1.0.0` across the reactor (`pom.xml`, all module parents); `examples/order-payment` now consumes `1.0.0`.
- Release workflow ready for stable publishing; `v1.0.0` tag uploads a validated bundle (`autoPublish=false`, manual Publish in Portal).
- Docs: `README.md`/`docs/PROJECT_CONTEXT.md` now reference Central `1.0.0`; `docs/MAVEN_CENTRAL.md` updated for the stable release.
- Verified: `mvn clean verify` passes; `mvn -P release verify -Dgpg.skip=true` builds sources/javadoc for all modules.

## 0.9.0 (Release Candidate)

- Version `0.1.0-SNAPSHOT` → `0.9.0` across the reactor (`pom.xml`, all module parents); `examples/order-payment` now consumes `0.9.0`.
- Release-blocking fixes found by a clean external-consumer test (separate Spring Boot project resolving `io.github.kaiser-haque:manto-spring-boot-starter:0.9.0` as a binary artifact against real Kafka via Testcontainers):
  - `manto-spring-boot-autoconfigure` now depends on `spring-boot-starter-validation` (compile) so `@Validated` `MantoProperties` binds in apps without `spring-boot-starter-web`; without it external startup failed with `NoProviderFoundException`. Test-scope `hibernate-validator`/`jakarta.el` removed (now transitive).
  - `MantoListenerInterceptor` now overrides `success`/`failure` so `manto.processing.duration` and `manto.messages.failed` are actually recorded at runtime and the correlation `ThreadLocal` is cleared on both paths, as documented in `docs/OBSERVABILITY.md`.
  - `MantoErrorHandler` now registers a `RetryListener` so `manto.messages.retried` increments on each failed delivery.
- Docs: `README.md`/`docs/PROJECT_CONTEXT.md` dependency snippets updated to `0.9.0`; `README.md`/`docs/OBSERVABILITY.md` failed-metric recording path corrected.
- Verified: external project checks pass (auto-configuration, producer, consumer, retry, DLT, idempotency, metrics); `mvn clean verify` passes; `mvn -P release verify -Dgpg.skip=true` builds sources/javadoc; example compiles against `0.9.0`.

## Initial development

- Core framework: producer abstraction, `@MantoListener` consumer registration, JSON serialization, headers/correlation IDs, retry with exponential backoff, DLT with diagnostic headers, idempotency store, Micrometer metrics, Spring Boot auto-configuration and starter.
- Documentation: installation, configuration reference, error handling, observability, and runnable `examples/order-payment`.
- Quality: dependency audit, Javadoc coverage, public API stability, no hardcoded secrets, reproducible builds, Testcontainers integration tests.
