# Configuration

Initial configuration should use the `manto` prefix.

Example:

```yaml
manto:
  kafka:
    bootstrap-servers: localhost:9092

  retry:
    enabled: true
    max-attempts: 3
    backoff:
      initial-delay: 1000
      multiplier: 2

  dlt:
    enabled: true

  idempotency:
    enabled: true

  observability:
    enabled: true
```

## Principles

- sensible defaults,
- validation for invalid values,
- no hard-coded environment-specific addresses,
- avoid duplicating every Spring Kafka property,
- expose Manto-specific conventions while allowing Spring Kafka configuration where appropriate.

The exact property names may evolve during implementation; update this document when the API is finalized.
