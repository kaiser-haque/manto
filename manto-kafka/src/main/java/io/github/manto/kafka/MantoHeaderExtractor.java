package io.github.manto.kafka;

import io.github.manto.core.MantoEventMetadata;
import io.github.manto.core.MantoHeaders;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.messaging.Message;

import java.time.Instant;
import java.util.UUID;

/**
 * Extracts standardized Manto metadata from Kafka messages.
 *
 * <p>Supports both Spring Kafka {@link Message} and raw Kafka {@link ConsumerRecord}
 * types. Missing headers are replaced with sensible defaults, ensuring the extractor
 * never throws during normal message processing.</p>
 */
public final class MantoHeaderExtractor {

    private MantoHeaderExtractor() {
    }

    /**
     * Extracts Manto metadata from a Spring Kafka message.
     *
     * @param message the message containing Manto headers
     * @return the extracted metadata, never null
     */
    public static MantoEventMetadata extract(Message<?> message) {
        String eventId = getHeader(message, MantoHeaders.EVENT_ID, UUID.randomUUID().toString());
        String eventType = getHeader(message, MantoHeaders.EVENT_TYPE, "UnknownEvent");
        String eventVersion = getHeader(message, MantoHeaders.EVENT_VERSION, "1.0");
        String correlationId = getHeader(message, MantoHeaders.CORRELATION_ID, eventId);
        String source = getHeader(message, MantoHeaders.SOURCE, "unknown");
        Instant timestamp = Instant.now();

        return new MantoEventMetadata(eventId, eventType, eventVersion, correlationId, source, timestamp);
    }

    /**
     * Extracts Manto metadata from a raw Kafka consumer record.
     *
     * @param record the consumer record containing Manto headers
     * @return the extracted metadata, never null
     */
    public static MantoEventMetadata extract(ConsumerRecord<?, ?> record) {
        String eventId = getHeader(record, MantoHeaders.EVENT_ID, UUID.randomUUID().toString());
        String eventType = getHeader(record, MantoHeaders.EVENT_TYPE, "UnknownEvent");
        String eventVersion = getHeader(record, MantoHeaders.EVENT_VERSION, "1.0");
        String correlationId = getHeader(record, MantoHeaders.CORRELATION_ID, eventId);
        String source = getHeader(record, MantoHeaders.SOURCE, "unknown");
        Instant timestamp = record.timestamp() > 0 ? Instant.ofEpochMilli(record.timestamp()) : Instant.now();

        return new MantoEventMetadata(eventId, eventType, eventVersion, correlationId, source, timestamp);
    }

    private static String getHeader(Message<?> message, String headerName, String defaultValue) {
        Object value = message.getHeaders().get(headerName);
        return value != null ? value.toString() : defaultValue;
    }

    private static String getHeader(ConsumerRecord<?, ?> record, String headerName, String defaultValue) {
        org.apache.kafka.common.header.Header header = record.headers().lastHeader(headerName);
        return header != null ? new String(header.value(), java.nio.charset.StandardCharsets.UTF_8) : defaultValue;
    }
}