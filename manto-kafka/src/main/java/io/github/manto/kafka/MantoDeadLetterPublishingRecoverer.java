package io.github.manto.kafka;

import io.github.manto.core.MantoHeaders;
import io.github.manto.core.RetryPolicy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.lang.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import java.util.function.BiFunction;

/**
 * Spring Kafka recoverer that extends {@link DeadLetterPublishingRecoverer} to use
 * Manto's DLT topic naming convention and custom headers.
 */
public class MantoDeadLetterPublishingRecoverer extends DeadLetterPublishingRecoverer {

    private final DefaultDeadLetterHandler deadLetterHandler;
    private final DefaultExceptionClassifier exceptionClassifier;
    private final RetryPolicy retryPolicy;
    private final MantoMetrics metrics;
    private static final ThreadLocal<Exception> CURRENT_EXCEPTION = new ThreadLocal<>();

    public MantoDeadLetterPublishingRecoverer(DefaultDeadLetterHandler deadLetterHandler,
                                               DefaultExceptionClassifier exceptionClassifier,
                                               RetryPolicy retryPolicy,
                                               KafkaTemplate<Object, Object> kafkaTemplate,
                                               String topicSuffix,
                                               MantoMetrics metrics) {
        super(kafkaTemplate, createDestinationResolver(topicSuffix));
        this.deadLetterHandler = deadLetterHandler;
        this.exceptionClassifier = exceptionClassifier;
        this.retryPolicy = retryPolicy;
        this.metrics = metrics;
    }

    private static BiFunction<ConsumerRecord<?, ?>, Exception, org.apache.kafka.common.TopicPartition> createDestinationResolver(String topicSuffix) {
        return (record, exception) -> new org.apache.kafka.common.TopicPartition(record.topic() + topicSuffix, record.partition());
    }

    @Override
    protected ProducerRecord<Object, Object> createProducerRecord(ConsumerRecord<?, ?> record,
            TopicPartition topicPartition, Headers headers, @Nullable byte[] key, @Nullable byte[] value) {
        addMantoHeaders(record, headers);
        return super.createProducerRecord(record, topicPartition, headers, key, value);
    }

    @Override
    public void accept(ConsumerRecord<?, ?> record,
                       org.apache.kafka.clients.consumer.Consumer<?, ?> consumer,
                       Exception exception) {
        CURRENT_EXCEPTION.set(exception);
        try {
            super.accept(record, consumer, exception);
        } finally {
            CURRENT_EXCEPTION.remove();
        }
        if (metrics != null) {
            metrics.recordDlt(record.topic());
        }
    }

    private void addMantoHeaders(ConsumerRecord<?, ?> record, Headers headers) {
        String eventId = getHeaderValue(record, MantoHeaders.EVENT_ID);
        String eventType = getHeaderValue(record, MantoHeaders.EVENT_TYPE);
        String eventVersion = getHeaderValue(record, MantoHeaders.EVENT_VERSION);
        String correlationId = getHeaderValue(record, MantoHeaders.CORRELATION_ID);
        String source = getHeaderValue(record, MantoHeaders.SOURCE);

        if (eventId != null) addHeader(headers, MantoHeaders.EVENT_ID, eventId);
        if (eventType != null) addHeader(headers, MantoHeaders.EVENT_TYPE, eventType);
        if (eventVersion != null) addHeader(headers, MantoHeaders.EVENT_VERSION, eventVersion);
        if (correlationId != null) addHeader(headers, MantoHeaders.CORRELATION_ID, correlationId);
        if (source != null) addHeader(headers, MantoHeaders.SOURCE, source);

        addHeader(headers, MantoHeaders.DLT_ORIGINAL_TOPIC, record.topic());
        addHeader(headers, MantoHeaders.DLT_ORIGINAL_PARTITION, String.valueOf(record.partition()));
        addHeader(headers, MantoHeaders.DLT_ORIGINAL_OFFSET, String.valueOf(record.offset()));
        addHeader(headers, MantoHeaders.DLT_ORIGINAL_TIMESTAMP, String.valueOf(record.timestamp()));

        Exception exception = CURRENT_EXCEPTION.get();
        addHeader(headers, MantoHeaders.DLT_EXCEPTION_CLASS, exception != null ? exception.getClass().getName() : "null");
        addHeader(headers, MantoHeaders.DLT_EXCEPTION_MESSAGE, exception != null ? exception.getMessage() : "null");

        addHeader(headers, MantoHeaders.DLT_RETRY_COUNT, String.valueOf(retryPolicy.maxAttempts() - 1));
        addHeader(headers, MantoHeaders.DLT_FAILURE_TIMESTAMP, Instant.now().toString());
        addHeader(headers, MantoHeaders.DLT_TRACE_ID, UUID.randomUUID().toString());
    }

    private String getHeaderValue(ConsumerRecord<?, ?> record, String headerName) {
        org.apache.kafka.common.header.Header header = record.headers().lastHeader(headerName);
        return header != null ? new String(header.value(), StandardCharsets.UTF_8) : null;
    }

    private void addHeader(Headers headers, String key, String value) {
        if (value != null) {
            headers.add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
        }
    }
}