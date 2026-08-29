package io.github.manto.kafka;

import io.github.manto.core.MantoHeaders;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MantoListenerInterceptorTest {

    @AfterEach
    void cleanup() {
        CorrelationIdContext.clear();
    }

    private static ConsumerRecord<String, Object> recordWithHeaders(
            String topic, RecordHeaders headers) {
        ConsumerRecord<String, Object> base = new ConsumerRecord<>(topic, 0, 0L, null, "value");
        return new ConsumerRecord<>(base.topic(), base.partition(), base.offset(),
                base.timestamp(), org.apache.kafka.common.record.TimestampType.NO_TIMESTAMP_TYPE,
                (long) 0, 0, 0, null, base.value(), headers, java.util.Optional.empty());
    }

    private static ConsumerRecord<String, Object> recordWithHeader(
            String topic, String headerName, String headerValue) {
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(headerName, headerValue.getBytes()));
        return recordWithHeaders(topic, headers);
    }

    @Test
    void recordsConsumedOnIntercept() {
        MeterRegistry registry = new SimpleMeterRegistry();
        MantoMetrics metrics = new MantoMetrics(registry, true);
        MantoListenerInterceptor interceptor = new MantoListenerInterceptor(metrics);

        ConsumerRecord<String, Object> record = new ConsumerRecord<>("test-topic", 0, 0, "key", "value");
        Consumer<String, Object> consumer = mock(Consumer.class);

        ConsumerRecord<String, Object> result = interceptor.intercept(record, consumer);

        assertEquals(record, result);
        assertEquals(1, registry.get("manto.messages.consumed")
                .tag("topic", "test-topic")
                .tag("operation", "consume")
                .tag("outcome", "success")
                .counter()
                .count());
    }

    @Test
    void recordsProcessingDurationOnSuccess() {
        MeterRegistry registry = new SimpleMeterRegistry();
        MantoMetrics metrics = new MantoMetrics(registry, true);
        MantoListenerInterceptor interceptor = new MantoListenerInterceptor(metrics);

        ConsumerRecord<String, Object> record = new ConsumerRecord<>("test-topic", 0, 0, "key", "value");
        Consumer<String, Object> consumer = mock(Consumer.class);

        interceptor.intercept(record, consumer);
        interceptor.recordProcessingDuration("test-topic");

        assertNotNull(registry.get("manto.processing.duration")
                .tag("topic", "test-topic")
                .tag("operation", "process")
                .timer());
    }

    @Test
    void recordsFailedOnError() {
        MeterRegistry registry = new SimpleMeterRegistry();
        MantoMetrics metrics = new MantoMetrics(registry, true);
        MantoListenerInterceptor interceptor = new MantoListenerInterceptor(metrics);

        interceptor.recordFailed("test-topic");

        assertEquals(1, registry.get("manto.messages.failed")
                .tag("topic", "test-topic")
                .tag("operation", "consume")
                .tag("outcome", "failure")
                .counter()
                .count());
    }

    @Test
    void doesNotRecordWhenDisabled() {
        MeterRegistry registry = new SimpleMeterRegistry();
        MantoMetrics metrics = new MantoMetrics(registry, false);
        MantoListenerInterceptor interceptor = new MantoListenerInterceptor(metrics);

        ConsumerRecord<String, Object> record = new ConsumerRecord<>("test-topic", 0, 0, "key", "value");
        Consumer<String, Object> consumer = mock(Consumer.class);

        interceptor.intercept(record, consumer);
        interceptor.recordProcessingDuration("test-topic");
        interceptor.recordFailed("test-topic");

        assertEquals(0, registry.getMeters().size());
    }

    @Test
    void setsCorrelationIdInContextOnIntercept() {
        MantoListenerInterceptor interceptor = new MantoListenerInterceptor(null);

        ConsumerRecord<String, Object> record = recordWithHeader("test-topic",
                MantoHeaders.CORRELATION_ID, "corr-123");
        Consumer<String, Object> consumer = mock(Consumer.class);

        interceptor.intercept(record, consumer);

        assertEquals("corr-123", CorrelationIdContext.get());
    }

    @Test
    void clearsCorrelationIdContextAfterProcessingDuration() {
        MantoListenerInterceptor interceptor = new MantoListenerInterceptor(null);

        ConsumerRecord<String, Object> record = recordWithHeader("test-topic",
                MantoHeaders.CORRELATION_ID, "corr-456");
        Consumer<String, Object> consumer = mock(Consumer.class);

        interceptor.intercept(record, consumer);
        assertEquals("corr-456", CorrelationIdContext.get());

        interceptor.recordProcessingDuration("test-topic");
        assertNull(CorrelationIdContext.get());
    }

    @Test
    void clearsCorrelationIdContextAfterFailed() {
        MantoListenerInterceptor interceptor = new MantoListenerInterceptor(null);

        ConsumerRecord<String, Object> record = recordWithHeader("test-topic",
                MantoHeaders.CORRELATION_ID, "corr-789");
        Consumer<String, Object> consumer = mock(Consumer.class);

        interceptor.intercept(record, consumer);
        assertEquals("corr-789", CorrelationIdContext.get());

        interceptor.recordFailed("test-topic");
        assertNull(CorrelationIdContext.get());
    }

    @Test
    void fallsBackToEventIdWhenCorrelationIdMissing() {
        MantoListenerInterceptor interceptor = new MantoListenerInterceptor(null);

        ConsumerRecord<String, Object> record = recordWithHeader("test-topic",
                MantoHeaders.EVENT_ID, "evt-000");
        Consumer<String, Object> consumer = mock(Consumer.class);

        interceptor.intercept(record, consumer);

        assertEquals("evt-000", CorrelationIdContext.get());
    }

    @Test
    void setsNullContextWhenNoHeadersPresent() {
        MantoListenerInterceptor interceptor = new MantoListenerInterceptor(null);

        ConsumerRecord<String, Object> record = new ConsumerRecord<>("test-topic", 0, 0, "key", "value");
        Consumer<String, Object> consumer = mock(Consumer.class);

        interceptor.intercept(record, consumer);

        assertNull(CorrelationIdContext.get());
    }
}