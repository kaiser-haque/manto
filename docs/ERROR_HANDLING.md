# Error Handling

## Goals

Manto must make transient failures recoverable and permanent failures diagnosable.

## Processing lifecycle

```text
consume
  |
  v
handler
  |
  +-- success --> acknowledge
  |
  +-- failure --> classify
                   |
                   +-- retryable --> retry/backoff
                   |
                   +-- non-retryable --> DLT
                   |
                   +-- attempts exhausted --> DLT
```

## Retry

Support:

- enabled/disabled,
- maximum attempts,
- fixed or exponential backoff,
- exception classification.

Do not implement an overly complex policy engine in v1.0.

## DLT

The DLT record should retain useful diagnostic context such as:

- original topic,
- partition,
- offset,
- event ID,
- correlation ID,
- exception class,
- exception message,
- retry count.

Avoid logging full payloads by default.

## Acknowledgement

Define acknowledgement semantics clearly in code and tests. Do not acknowledge a message as successfully processed before the intended application processing outcome is established.
