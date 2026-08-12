# Observability

Manto uses Micrometer rather than a custom metrics API.

## Initial metrics

- `manto.messages.published`
- `manto.messages.consumed`
- `manto.messages.failed`
- `manto.messages.retried`
- `manto.messages.dlt`
- `manto.processing.duration`

## Tags

Use low-cardinality tags such as:

- topic,
- operation,
- outcome.

Do not use event IDs, offsets, arbitrary exception messages, or user-controlled values as unbounded metric tags.

## Correlation

Correlation IDs should be propagated through Kafka headers and made available to application logging where practical.

## Logging

Never log:

- passwords,
- tokens,
- credentials,
- full sensitive payloads.

Prefer concise structured diagnostics.
