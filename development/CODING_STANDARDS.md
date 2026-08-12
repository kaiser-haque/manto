# Coding Standards

## Java

- Java 21.
- Clear class and method names.
- Constructor injection.
- Prefer immutable value objects.
- Avoid unnecessary inheritance.
- Keep methods focused.
- Use interfaces where they represent meaningful extension points.

## Spring

- Use Spring Boot auto-configuration conventions.
- Use `@ConfigurationProperties` for Manto configuration.
- Avoid static mutable state.
- Avoid hidden global behavior.

## Exceptions

Use meaningful exception types. Do not swallow exceptions.

## Logging

Use structured, concise logging. Never log credentials or sensitive payloads.

## Tests

Test observable behavior. Prefer real Kafka for Kafka-specific integration behavior.

## Dependencies

Every dependency should have a clear reason. Keep `manto-core` free of Kafka/Spring dependencies wherever possible.
