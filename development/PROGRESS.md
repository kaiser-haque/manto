# Manto Development Progress

## Current day

Day 3 — complete.

Next session: Day 4 — manto-core API.

## Current version

0.1.0-SNAPSHOT

## Completed

- [x] Repository foundation
- [x] Maven modules
- [x] Dependency management
- [ ] Core API
- [ ] Producer
- [ ] Consumer
- [ ] Retry
- [ ] DLT
- [ ] Idempotency
- [ ] Metrics
- [ ] Integration tests
- [ ] Documentation
- [ ] Maven Central release

## Current task

Day 3 — dependency management.

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

- `mvn clean verify` — BUILD SUCCESS, all 6 reactor modules.

## Known issues

- None.

## Next task

Day 4 — Core API (MantoProducer, MantoEventMetadata, core API types) in manto-core. Expected commit message for Day 3: `build: configure project dependencies`.
