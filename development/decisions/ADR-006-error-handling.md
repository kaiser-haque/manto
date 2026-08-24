# ADR-006: Error Handling Architecture

## Status

Accepted

## Decision

Manto provides framework-level error handling abstractions in `manto-core` with Spring Kafka implementations in `manto-kafka`. The abstractions are:

- `RetryPolicy`: Configures retry behavior (enabled/disabled, max attempts).
- `BackoffStrategy`: Calculates delay between retry attempts (exponential backoff).
- `ExceptionClassifier`: Classifies exceptions as retryable (transient) or non-retryable (permanent).
- `DeadLetterHandler`: Routes exhausted/non-retryable messages to a DLT with diagnostic metadata.
- `MantoRecord` / `MantoHeader`: Framework-agnostic record and header types to keep `manto-core` free of Kafka dependencies.

Spring Kafka's native retry/DLT mechanisms (`DefaultErrorHandler`, `ExponentialBackOff`, `DeadLetterPublishingRecoverer`) are configured via auto-configuration but are not directly exposed in Manto's public API.

## Rationale

- Keeps `manto-core` technology-agnostic (no Kafka/Spring dependencies).
- Provides clear extension points for custom retry/backoff/classification/DLT logic.
- Manto's `DefaultDeadLetterHandler` adds rich diagnostic headers (original topic/partition/offset, Manto event metadata, exception info, retry count, trace ID) without logging sensitive payloads.
- Spring Kafka integration is internal; users configure via `manto.retry.*`, `manto.dlt.*` properties.

## Consequences

- Users can customize behavior by providing their own beans for the interfaces.
- Full Spring Kafka error handling power remains accessible via direct bean overrides.
- DLT topic naming convention: `{original-topic}.DLT` (configurable suffix).
- Retry count in DLT headers reflects `maxAttempts - 1` (initial attempt + retries).