# Configuration

All Manto configuration lives under the `manto` prefix via `MantoProperties` (`manto-spring-boot-autoconfigure/src/main/java/io/github/manto/autoconfigure/MantoProperties.java:19`) annotated with `@ConfigurationProperties(prefix = "manto")` and `@Validated`. Any typo or constraint violation fails fast at startup.

## Full YAML Example

```yaml
manto:
  kafka:
    bootstrap-servers: localhost:9092   # required, @NotBlank, default localhost:9092

  retry:
    enabled: true                        # default true
    max-attempts: 3                      # @Min(1) @Max(100), default 3
    backoff:
      initial-delay: 1000                # Duration, must be >0, default 1s
      multiplier: 2.0                    # @DecimalMin(1.0) @DecimalMax(10.0), default 2.0
      max-delay: 30000                   # Duration, must be >0, default 30s

  dlt:
    enabled: false                       # default false
    topic-suffix: .DLT                   # default .DLT

  idempotency:
    enabled: true                        # default true

  observability:
    enabled: true                        # default true
```

`@DurationUnit` is not set, so `1000` is parsed as milliseconds. `1s`, `500ms`, `30s` etc. are recommended for clarity.

## Property Reference

| Property | Type | Default | Constraint | Description |
|---|---|---|---|---|
| `manto.kafka.bootstrap-servers` | `String` | `localhost:9092` | `@NotBlank` | Kafka bootstrap servers. The only required Manto property. Maps to `ProducerConfig.BOOTSTRAP_SERVERS_CONFIG` and `ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG`. |
| `manto.retry.enabled` | `boolean` | `true` | — | Whether failed handler invocations are retried before DLT/handling. When `false`, no `DefaultErrorHandler` is installed on the container factory. |
| `manto.retry.max-attempts` | `int` | `3` | `@Min(1) @Max(100)` | Total delivery attempts including the initial call. Internally mapped to Spring `ExponentialBackOff.setMaxAttempts(maxAttempts - 1)` (`MantoAutoConfiguration.java:198`). `1` means no retries. |
| `manto.retry.backoff.initial-delay` | `Duration` | `1000ms` | `@NotNull`, `>0` | Initial retry delay. Checked by `ExponentialBackoffStrategy` (`ExponentialBackoffStrategy.java:20`). |
| `manto.retry.backoff.multiplier` | `double` | `2.0` | `@DecimalMin(1.0) @DecimalMax(10.0)` | Exponential multiplier per retry. `delay(n) = min(initialDelay * multiplier^(n-1), maxDelay)`. |
| `manto.retry.backoff.max-delay` | `Duration` | `30000ms` | `@NotNull`, `>0` | Cap for exponential delay. |
| `manto.dlt.enabled` | `boolean` | `false` | — | Whether exhausted/non-retryable failures are published to a dead-letter topic. When `true`, the container factory uses `MantoDeadLetterPublishingRecoverer` as the recoverer for the `MantoErrorHandler`. |
| `manto.dlt.topic-suffix` | `String` | `.DLT` | `null` → `.DLT` | Suffix appended to the original topic name for the DLT. Destination is `record.topic() + topicSuffix`, partition is preserved (`MantoDeadLetterPublishingRecoverer.java:45`). |
| `manto.idempotency.enabled` | `boolean` | `true` | — | Gating flag for idempotency guard logic in application code. The `IdempotencyStore` bean is still available when `false`; the flag has no framework-side filtering — the handler decides. |
| `manto.observability.enabled` | `boolean` | `true` | — | Gating flag for `MantoMetrics`. When `false`, `MantoMetrics` methods are no-ops (`MantoMetrics.java:37`). A `SimpleMeterRegistry` is auto-configured when no `MeterRegistry` bean exists (`MantoAutoConfiguration.java:93`). |

There are no additional namespaces like `manto.producer.*` or `manto.consumer.*`. Only the `manto.*` tree above is bound.

## Relationship to Spring Kafka Properties

Manto does not duplicate every Spring Kafka property. It exposes Manto-specific conventions and leaves standard Spring Kafka tuning to `spring.kafka.*`. For example, to tune linger or acks, use `spring.kafka.producer.*`.

Manto's factories set the following defaults (overridable by providing your own `ProducerFactory`/`ConsumerFactory` beans via `@ConditionalOnMissingBean`):

- `ProducerFactory<String, Object>`: `KEY_SERIALIZER=StringSerializer`, `VALUE_SERIALIZER=JsonSerializer` (`MantoAutoConfiguration.java:76`)
- `ConsumerFactory<String, Object>`: `KEY_DESERIALIZER=StringDeserializer`, `VALUE_DESERIALIZER=JsonDeserializer`, `AUTO_OFFSET_RESET=earliest`, `TRUSTED_PACKAGES=*`, `USE_TYPE_INFO_HEADERS=true` (`MantoAutoConfiguration.java:113`)
- The `KafkaListenerContainerFactory` wires a `MantoListenerInterceptor` via `setRecordInterceptor` (`MantoAutoConfiguration.java:191`)

## Overriding Auto-Configuration

All factory and infrastructure beans are declared with `@ConditionalOnMissingBean` (`MantoAutoConfiguration.java:71`):

- `ProducerFactory<String, Object>` (`mantoProducerFactory`), `ProducerFactory<Object, Object>` (`mantoDltProducerFactory`), `KafkaTemplate<String, Object>`, `KafkaTemplate<Object, Object>`
- `ConsumerFactory<String, Object>`, `ConcurrentKafkaListenerContainerFactory<String, Object>` (`kafkaListenerContainerFactory`)
- `DefaultRetryPolicy`, `ExponentialBackoffStrategy`, `DefaultExceptionClassifier`, `DefaultDeadLetterHandler`, `IdempotencyStore`
- `MantoMetrics` (needs a `MeterRegistry`; `SimpleMeterRegistry` is provided as fallback), `MantoListenerInterceptor`
- `MantoProducer` (constructed as `new MantoKafkaProducer(template, "manto", metrics)`)

Provide your own bean of the same type to replace any of them. Example — custom exception classifier:

```java
@Bean
public DefaultExceptionClassifier mantoExceptionClassifier() {
    return new DefaultExceptionClassifier(Set.of(IllegalArgumentException.class));
}
```

## Validation

`MantoProperties` uses `jakarta.validation` constraints (`MantoProperties.java:3`) — invalid values throw `BindValidationException` at startup. Notable rejections:

- `manto.kafka.bootstrap-servers: ""` — `must not be blank`
- `manto.retry.max-attempts: 0` — `must be greater than or equal to 1`
- `manto.retry.max-attempts: 200` — `must be less than or equal to 100`
- `manto.retry.backoff.multiplier: 0.5` — `must be greater than or equal to 1.0`
- `manto.retry.backoff.initial-delay: null` — `must not be null`

`ExponentialBackoffStrategy` also validates at construction: `initialDelay` and `maxDelay` must be positive, `multiplier` at least `1.0` (`ExponentialBackoffStrategy.java:20`), and `nextDelay(attempt)` requires `attempt >= 1`.

## Minimal vs. Full Config

Minimal (single Kafka address, sane defaults for everything else):

```yaml
manto:
  kafka:
    bootstrap-servers: localhost:9092
```

Full integration-test / production-leaning (as in `examples/order-payment/src/main/resources/application.yml:4`):

```yaml
manto:
  kafka:
    bootstrap-servers: localhost:9092
  retry:
    enabled: true
    max-attempts: 3
    backoff:
      initial-delay: 1000
      multiplier: 2.0
      max-delay: 10000
  dlt:
    enabled: true
    topic-suffix: .DLT
  idempotency:
    enabled: true
  observability:
    enabled: true
```
