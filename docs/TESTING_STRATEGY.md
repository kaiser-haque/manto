# Testing Strategy

## Unit tests

Use JUnit 5 and Mockito for:

- API behavior,
- metadata creation,
- serialization,
- retry policies,
- exception classification,
- idempotency logic.

## Integration tests

Use Testcontainers with real Kafka for:

- producer publishing,
- consumer delivery,
- serialization,
- retry,
- DLT,
- metadata/header propagation,
- end-to-end behavior.

## Test pyramid

```text
       E2E / Kafka integration
              /\
             /  \
            /    \
           /------\
          Unit tests
```

Most logic should be covered with fast unit tests. Kafka semantics must be verified with real Kafka.

## Test requirements

Every feature should include:

- happy path,
- failure path,
- boundary behavior,
- configuration behavior where applicable.

Avoid tests that merely verify implementation details.
