package io.github.manto.core;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MantoEventMetadataTest {

    private final Instant timestamp = Instant.parse("2026-08-15T10:00:00Z");

    @Test
    void storesAllFields() {
        MantoEventMetadata metadata = new MantoEventMetadata(
                "evt-1", "OrderCreated", "1.0", "corr-42", "order-service", timestamp);

        assertEquals("evt-1", metadata.eventId());
        assertEquals("OrderCreated", metadata.eventType());
        assertEquals("1.0", metadata.eventVersion());
        assertEquals("corr-42", metadata.correlationId());
        assertEquals("order-service", metadata.source());
        assertEquals(timestamp, metadata.timestamp());
    }

    @Test
    void equalsAndHashCodeAreValueBased() {
        MantoEventMetadata a = new MantoEventMetadata("evt-1", "OrderCreated", "1.0", "corr-42", "order-service", timestamp);
        MantoEventMetadata b = new MantoEventMetadata("evt-1", "OrderCreated", "1.0", "corr-42", "order-service", timestamp);

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    void equalityConsidersAllFields() {
        MantoEventMetadata a = new MantoEventMetadata("evt-1", "OrderCreated", "1.0", "corr-42", "order-service", timestamp);
        MantoEventMetadata b = new MantoEventMetadata("evt-2", "OrderCreated", "1.0", "corr-42", "order-service", timestamp);

        assertNotEquals(a, b);
    }

    @Test
    void rejectsNullEventId() {
        assertThrows(IllegalArgumentException.class,
                () -> new MantoEventMetadata(null, "OrderCreated", "1.0", "corr-42", "order-service", timestamp));
    }

    @Test
    void rejectsBlankEventId() {
        assertThrows(IllegalArgumentException.class,
                () -> new MantoEventMetadata(" ", "OrderCreated", "1.0", "corr-42", "order-service", timestamp));
    }

    @Test
    void rejectsNullEventType() {
        assertThrows(IllegalArgumentException.class,
                () -> new MantoEventMetadata("evt-1", null, "1.0", "corr-42", "order-service", timestamp));
    }

    @Test
    void rejectsBlankEventVersion() {
        assertThrows(IllegalArgumentException.class,
                () -> new MantoEventMetadata("evt-1", "OrderCreated", "", "corr-42", "order-service", timestamp));
    }

    @Test
    void rejectsBlankCorrelationId() {
        assertThrows(IllegalArgumentException.class,
                () -> new MantoEventMetadata("evt-1", "OrderCreated", "1.0", "  ", "order-service", timestamp));
    }

    @Test
    void rejectsNullSource() {
        assertThrows(IllegalArgumentException.class,
                () -> new MantoEventMetadata("evt-1", "OrderCreated", "1.0", "corr-42", null, timestamp));
    }

    @Test
    void rejectsNullTimestamp() {
        assertThrows(IllegalArgumentException.class,
                () -> new MantoEventMetadata("evt-1", "OrderCreated", "1.0", "corr-42", "order-service", null));
    }
}
