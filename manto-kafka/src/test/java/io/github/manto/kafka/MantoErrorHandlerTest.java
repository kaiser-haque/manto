package io.github.manto.kafka;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.util.backoff.FixedBackOff;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies that {@link MantoErrorHandler} records retry metrics through its
 * registered {@link RetryListener}.
 */
class MantoErrorHandlerTest {

    /** Exposes the protected retry listeners for verification. */
    static class TestableErrorHandler extends MantoErrorHandler {
        TestableErrorHandler(MantoListenerInterceptor interceptor, FixedBackOff backOff) {
            super(interceptor, backOff);
        }

        TestableErrorHandler(MantoListenerInterceptor interceptor,
                             org.springframework.kafka.listener.ConsumerRecordRecoverer recoverer,
                             FixedBackOff backOff) {
            super(interceptor, recoverer, backOff);
        }

        List<RetryListener> retryListeners() {
            return getRetryListeners();
        }
    }

    @Test
    void recordsRetriedOnFailedDelivery() {
        MeterRegistry registry = new SimpleMeterRegistry();
        MantoMetrics metrics = new MantoMetrics(registry, true);
        MantoListenerInterceptor interceptor = new MantoListenerInterceptor(metrics);
        TestableErrorHandler handler = new TestableErrorHandler(interceptor, new FixedBackOff(100L, 2L));

        ConsumerRecord<?, ?> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "value");
        handler.retryListeners()
                .forEach(listener -> listener.failedDelivery(record, new RuntimeException("boom"), 1));

        assertEquals(1, registry.get("manto.messages.retried")
                .tag("topic", "test-topic")
                .tag("operation", "retry")
                .tag("outcome", "attempt")
                .counter()
                .count());
    }

    @Test
    void recordsRetriedWithRecovererConstructor() {
        MeterRegistry registry = new SimpleMeterRegistry();
        MantoMetrics metrics = new MantoMetrics(registry, true);
        MantoListenerInterceptor interceptor = new MantoListenerInterceptor(metrics);
        TestableErrorHandler handler = new TestableErrorHandler(interceptor,
                (record, exception) -> {
                }, new FixedBackOff(100L, 2L));

        ConsumerRecord<?, ?> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "value");
        handler.retryListeners()
                .forEach(listener -> listener.failedDelivery(record, new RuntimeException("boom"), 2));

        assertEquals(1, registry.get("manto.messages.retried")
                .tag("topic", "test-topic")
                .tag("operation", "retry")
                .tag("outcome", "attempt")
                .counter()
                .count());
    }

    @Test
    void retryListenerIsNullSafeWithoutMetrics() {
        MantoListenerInterceptor interceptor = new MantoListenerInterceptor(null);
        TestableErrorHandler handler = new TestableErrorHandler(interceptor, new FixedBackOff(100L, 2L));

        ConsumerRecord<?, ?> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "value");
        handler.retryListeners()
                .forEach(listener -> listener.failedDelivery(record, new RuntimeException("boom"), 1));
    }

    @Test
    void doesNotRecordWhenDisabled() {
        MeterRegistry registry = new SimpleMeterRegistry();
        MantoMetrics metrics = new MantoMetrics(registry, false);
        MantoListenerInterceptor interceptor = new MantoListenerInterceptor(metrics);
        TestableErrorHandler handler = new TestableErrorHandler(interceptor, new FixedBackOff(100L, 2L));

        ConsumerRecord<?, ?> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "value");
        handler.retryListeners()
                .forEach(listener -> listener.failedDelivery(record, new RuntimeException("boom"), 1));

        assertEquals(0, registry.getMeters().size());
    }
}
