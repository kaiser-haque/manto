package io.github.manto.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DeadLetterHandlerTest {

    @Test
    void shouldImplementDeadLetterHandler() {
        DeadLetterHandler handler = new DeadLetterHandler() {
            @Override
            public <K, V> void handle(MantoRecord<K, V> record, Throwable exception, int retryCount) {
                assertNotNull(record);
                assertEquals("test-topic", record.topic());
                assertEquals(0, record.partition());
                assertEquals(10L, record.offset());
                assertEquals("test-value", record.value());
            }
        };

        MantoRecord<String, String> record = new MantoRecord<>() {
            @Override public String topic() { return "test-topic"; }
            @Override public int partition() { return 0; }
            @Override public long offset() { return 10L; }
            @Override public long timestamp() { return System.currentTimeMillis(); }
            @Override public String key() { return "test-key"; }
            @Override public String value() { return "test-value"; }
            @Override public java.util.List<MantoHeader> headers() { return java.util.List.of(); }
        };

        assertDoesNotThrow(() -> handler.handle(record, new RuntimeException("test"), 2));
    }
}