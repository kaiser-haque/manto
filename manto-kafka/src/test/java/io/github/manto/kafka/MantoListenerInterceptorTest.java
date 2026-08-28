package io.github.manto.kafka;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MantoListenerInterceptorTest {

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
}