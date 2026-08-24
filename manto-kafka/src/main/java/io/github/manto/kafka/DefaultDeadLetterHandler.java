package io.github.manto.kafka;

import io.github.manto.core.DeadLetterHandler;
import io.github.manto.core.MantoHeader;
import io.github.manto.core.MantoHeaders;
import io.github.manto.core.MantoRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Default dead-letter handler that publishes failed messages to a DLT topic.
 *
 * <p>The DLT topic name is derived from the original topic by appending
 * a configurable suffix (default: {@code .DLT}). The published record
 * includes diagnostic headers with the original metadata, exception info,
 * and retry count.</p>
 */
public class DefaultDeadLetterHandler implements DeadLetterHandler {

    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final String topicSuffix;

    public DefaultDeadLetterHandler(KafkaTemplate<Object, Object> kafkaTemplate, String topicSuffix) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicSuffix = topicSuffix != null ? topicSuffix : ".DLT";
    }

    public KafkaTemplate<Object, Object> getKafkaTemplate() {
        return kafkaTemplate;
    }

    @Override
    public <K, V> void handle(MantoRecord<K, V> record, Throwable exception, int retryCount) {
        if (record instanceof KafkaMantoRecord<K, V> kafkaRecord) {
            handleKafkaRecord(kafkaRecord.delegate(), exception, retryCount);
        } else {
            // Fallback for non-Kafka records (should not happen in practice)
            String dltTopic = record.topic() + topicSuffix;
            ProducerRecord<Object, Object> dltRecord = new ProducerRecord<>(
                    dltTopic,
                    record.partition(),
                    record.timestamp(),
                    record.key(),
                    record.value(),
                    buildHeaders(record, exception, retryCount)
            );
            kafkaTemplate.send(dltTopic, dltRecord);
        }
    }

    /**
     * Handles a Kafka ConsumerRecord directly (for internal use).
     */
    public <K, V> void handleKafkaRecord(ConsumerRecord<K, V> record, Throwable exception, int retryCount) {
        String dltTopic = record.topic() + topicSuffix;

        long timestamp = record.timestamp();
        if (timestamp < 0) {
            timestamp = System.currentTimeMillis();
        }

        ProducerRecord<Object, Object> dltRecord = new ProducerRecord<>(
                dltTopic,
                record.partition(),
                timestamp,
                record.key(),
                record.value(),
                buildHeaders(record, exception, retryCount)
        );

        kafkaTemplate.send(dltTopic, dltRecord);
    }

    private RecordHeaders buildHeaders(MantoRecord<?, ?> record, Throwable exception, int retryCount) {
        List<Header> headers = new ArrayList<>();

        // Original record metadata
        addHeader(headers, MantoHeaders.DLT_ORIGINAL_TOPIC, record.topic());
        addHeader(headers, MantoHeaders.DLT_ORIGINAL_PARTITION, String.valueOf(record.partition()));
        addHeader(headers, MantoHeaders.DLT_ORIGINAL_OFFSET, String.valueOf(record.offset()));
        addHeader(headers, MantoHeaders.DLT_ORIGINAL_TIMESTAMP, String.valueOf(record.timestamp()));

        // Manto event metadata from original headers
        String eventId = record.header(MantoHeaders.EVENT_ID);
        String eventType = record.header(MantoHeaders.EVENT_TYPE);
        String eventVersion = record.header(MantoHeaders.EVENT_VERSION);
        String correlationId = record.header(MantoHeaders.CORRELATION_ID);
        String source = record.header(MantoHeaders.SOURCE);

        if (eventId != null) addHeader(headers, MantoHeaders.EVENT_ID, eventId);
        if (eventType != null) addHeader(headers, MantoHeaders.EVENT_TYPE, eventType);
        if (eventVersion != null) addHeader(headers, MantoHeaders.EVENT_VERSION, eventVersion);
        if (correlationId != null) addHeader(headers, MantoHeaders.CORRELATION_ID, correlationId);
        if (source != null) addHeader(headers, MantoHeaders.SOURCE, source);

        // DLT-specific metadata
        addHeader(headers, MantoHeaders.DLT_EXCEPTION_CLASS, exception != null ? exception.getClass().getName() : "null");
        addHeader(headers, MantoHeaders.DLT_EXCEPTION_MESSAGE, exception != null ? exception.getMessage() : "null");
        addHeader(headers, MantoHeaders.DLT_RETRY_COUNT, String.valueOf(retryCount));
        addHeader(headers, MantoHeaders.DLT_FAILURE_TIMESTAMP, Instant.now().toString());
        addHeader(headers, MantoHeaders.DLT_TRACE_ID, UUID.randomUUID().toString());

        return new RecordHeaders(headers);
    }

    private RecordHeaders buildHeaders(ConsumerRecord<?, ?> record, Throwable exception, int retryCount) {
        List<Header> headers = new ArrayList<>();

        // Original record metadata
        addHeader(headers, MantoHeaders.DLT_ORIGINAL_TOPIC, record.topic());
        addHeader(headers, MantoHeaders.DLT_ORIGINAL_PARTITION, String.valueOf(record.partition()));
        addHeader(headers, MantoHeaders.DLT_ORIGINAL_OFFSET, String.valueOf(record.offset()));
        addHeader(headers, MantoHeaders.DLT_ORIGINAL_TIMESTAMP, String.valueOf(record.timestamp()));

        // Manto event metadata from original headers
        String eventId = getHeaderAsString(record, MantoHeaders.EVENT_ID);
        String eventType = getHeaderAsString(record, MantoHeaders.EVENT_TYPE);
        String eventVersion = getHeaderAsString(record, MantoHeaders.EVENT_VERSION);
        String correlationId = getHeaderAsString(record, MantoHeaders.CORRELATION_ID);
        String source = getHeaderAsString(record, MantoHeaders.SOURCE);

        if (eventId != null) addHeader(headers, MantoHeaders.EVENT_ID, eventId);
        if (eventType != null) addHeader(headers, MantoHeaders.EVENT_TYPE, eventType);
        if (eventVersion != null) addHeader(headers, MantoHeaders.EVENT_VERSION, eventVersion);
        if (correlationId != null) addHeader(headers, MantoHeaders.CORRELATION_ID, correlationId);
        if (source != null) addHeader(headers, MantoHeaders.SOURCE, source);

        // DLT-specific metadata
        addHeader(headers, MantoHeaders.DLT_EXCEPTION_CLASS, exception != null ? exception.getClass().getName() : "null");
        addHeader(headers, MantoHeaders.DLT_EXCEPTION_MESSAGE, exception != null ? exception.getMessage() : "null");
        addHeader(headers, MantoHeaders.DLT_RETRY_COUNT, String.valueOf(retryCount));
        addHeader(headers, MantoHeaders.DLT_FAILURE_TIMESTAMP, Instant.now().toString());
        addHeader(headers, MantoHeaders.DLT_TRACE_ID, UUID.randomUUID().toString());

        return new RecordHeaders(headers);
    }

    private void addHeader(List<Header> headers, String key, String value) {
        if (value != null) {
            headers.add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
        }
    }

    private String getHeaderAsString(ConsumerRecord<?, ?> record, String headerName) {
        Header header = record.headers().lastHeader(headerName);
        return header != null ? new String(header.value(), StandardCharsets.UTF_8) : null;
    }

    /**
     * Adapter that wraps a Kafka ConsumerRecord as a MantoRecord.
     */
    public static class KafkaMantoRecord<K, V> implements MantoRecord<K, V> {
        private final ConsumerRecord<K, V> delegate;

        public KafkaMantoRecord(ConsumerRecord<K, V> delegate) {
            this.delegate = delegate;
        }

        public ConsumerRecord<K, V> delegate() {
            return delegate;
        }

        @Override
        public String topic() {
            return delegate.topic();
        }

        @Override
        public int partition() {
            return delegate.partition();
        }

        @Override
        public long offset() {
            return delegate.offset();
        }

        @Override
        public long timestamp() {
            return delegate.timestamp();
        }

        @Override
        public K key() {
            return delegate.key();
        }

        @Override
        public V value() {
            return delegate.value();
        }

        @Override
        public List<MantoHeader> headers() {
            List<MantoHeader> result = new ArrayList<>();
            for (Header header : delegate.headers()) {
                result.add(new KafkaMantoHeader(header));
            }
            return result;
        }
    }

    /**
     * Adapter that wraps a Kafka Header as a MantoHeader.
     */
    private static class KafkaMantoHeader implements MantoHeader {
        private final Header delegate;

        KafkaMantoHeader(Header delegate) {
            this.delegate = delegate;
        }

        @Override
        public String key() {
            return delegate.key();
        }

        @Override
        public String value() {
            return new String(delegate.value(), StandardCharsets.UTF_8);
        }

        @Override
        public byte[] valueBytes() {
            return delegate.value();
        }
    }
}