package io.github.manto.kafka;

import io.github.manto.core.MantoEventMetadata;
import io.github.manto.core.MantoHeaders;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class MantoHeaderExtractorTest {

    private record OrderCreatedEvent(String orderId, long amount) {
    }

    @Test
    void extractsAllHeadersFromSpringMessage() {
        OrderCreatedEvent event = new OrderCreatedEvent("order-1", 42);
        Message<OrderCreatedEvent> message = MessageBuilder.withPayload(event)
                .setHeader(MantoHeaders.EVENT_ID, "evt-123")
                .setHeader(MantoHeaders.EVENT_TYPE, "OrderCreatedEvent")
                .setHeader(MantoHeaders.EVENT_VERSION, "2.0")
                .setHeader(MantoHeaders.CORRELATION_ID, "corr-456")
                .setHeader(MantoHeaders.SOURCE, "order-service")
                .build();

        MantoEventMetadata metadata = MantoHeaderExtractor.extract(message);

        assertEquals("evt-123", metadata.eventId());
        assertEquals("OrderCreatedEvent", metadata.eventType());
        assertEquals("2.0", metadata.eventVersion());
        assertEquals("corr-456", metadata.correlationId());
        assertEquals("order-service", metadata.source());
        assertNotNull(metadata.timestamp());
    }

    @Test
    void usesDefaultsForMissingHeadersInSpringMessage() {
        OrderCreatedEvent event = new OrderCreatedEvent("order-1", 42);
        Message<OrderCreatedEvent> message = MessageBuilder.withPayload(event).build();

        MantoEventMetadata metadata = MantoHeaderExtractor.extract(message);

        assertNotNull(metadata.eventId());
        assertEquals("UnknownEvent", metadata.eventType());
        assertEquals("1.0", metadata.eventVersion());
        assertEquals(metadata.eventId(), metadata.correlationId());
        assertEquals("unknown", metadata.source());
        assertNotNull(metadata.timestamp());
    }

    @Test
    void extractsAllHeadersFromConsumerRecord() {
        Headers headers = new RecordHeaders();
        headers.add(MantoHeaders.EVENT_ID, "evt-789".getBytes(StandardCharsets.UTF_8));
        headers.add(MantoHeaders.EVENT_TYPE, "OrderCreatedEvent".getBytes(StandardCharsets.UTF_8));
        headers.add(MantoHeaders.EVENT_VERSION, "1.5".getBytes(StandardCharsets.UTF_8));
        headers.add(MantoHeaders.CORRELATION_ID, "corr-999".getBytes(StandardCharsets.UTF_8));
        headers.add(MantoHeaders.SOURCE, "inventory-service".getBytes(StandardCharsets.UTF_8));

        long timestamp = Instant.parse("2026-08-15T10:00:00Z").toEpochMilli();
        ConsumerRecord<String, String> record = new ConsumerRecord<String, String>(
                "order-events", 0, 0, timestamp,
                org.apache.kafka.common.record.TimestampType.CREATE_TIME, -1L, -1, -1,
                "key", "{}", headers);

        MantoEventMetadata metadata = MantoHeaderExtractor.extract(record);

        assertEquals("evt-789", metadata.eventId());
        assertEquals("OrderCreatedEvent", metadata.eventType());
        assertEquals("1.5", metadata.eventVersion());
        assertEquals("corr-999", metadata.correlationId());
        assertEquals("inventory-service", metadata.source());
        assertNotNull(metadata.timestamp());
    }

    @Test
    void usesDefaultsForMissingHeadersInConsumerRecord() {
        Headers headers = new RecordHeaders();
        ConsumerRecord<String, String> record = new ConsumerRecord<String, String>(
                "order-events", 0, 0, System.currentTimeMillis(),
                org.apache.kafka.common.record.TimestampType.CREATE_TIME, -1L, -1, -1,
                "key", "{}", headers);

        MantoEventMetadata metadata = MantoHeaderExtractor.extract(record);

        assertNotNull(metadata.eventId());
        assertEquals("UnknownEvent", metadata.eventType());
        assertEquals("1.0", metadata.eventVersion());
        assertEquals(metadata.eventId(), metadata.correlationId());
        assertEquals("unknown", metadata.source());
        assertNotNull(metadata.timestamp());
    }

    @Test
    void usesRecordTimestampWhenAvailable() {
        Headers headers = new RecordHeaders();
        // Use the full constructor to preserve timestamp
        long timestamp = Instant.parse("2026-08-15T10:00:00Z").toEpochMilli();
        ConsumerRecord<String, String> record = new ConsumerRecord<String, String>(
                "order-events", 0, 0, timestamp,
                org.apache.kafka.common.record.TimestampType.CREATE_TIME, -1L, -1, -1,
                "key", "{}", headers);

        MantoEventMetadata metadata = MantoHeaderExtractor.extract(record);

        assertEquals(Instant.parse("2026-08-15T10:00:00Z"), metadata.timestamp());
    }
}