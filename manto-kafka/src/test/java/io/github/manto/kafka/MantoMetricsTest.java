package io.github.manto.kafka;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MantoMetricsTest {

    @Test
    void recordsPublishedMetric() {
        MeterRegistry registry = new SimpleMeterRegistry();
        MantoMetrics metrics = new MantoMetrics(registry, true);

        metrics.recordPublished("test-topic");

        assertEquals(1, registry.get("manto.messages.published")
                .tag("topic", "test-topic")
                .tag("operation", "publish")
                .tag("outcome", "success")
                .counter()
                .count());
    }

    @Test
    void recordsPublishedFailureMetric() {
        MeterRegistry registry = new SimpleMeterRegistry();
        MantoMetrics metrics = new MantoMetrics(registry, true);

        metrics.recordPublishedFailure("test-topic");

        assertEquals(1, registry.get("manto.messages.published")
                .tag("topic", "test-topic")
                .tag("operation", "publish")
                .tag("outcome", "failure")
                .counter()
                .count());
    }

    @Test
    void recordsConsumedMetric() {
        MeterRegistry registry = new SimpleMeterRegistry();
        MantoMetrics metrics = new MantoMetrics(registry, true);

        metrics.recordConsumed("test-topic");

        assertEquals(1, registry.get("manto.messages.consumed")
                .tag("topic", "test-topic")
                .tag("operation", "consume")
                .tag("outcome", "success")
                .counter()
                .count());
    }

    @Test
    void recordsFailedMetric() {
        MeterRegistry registry = new SimpleMeterRegistry();
        MantoMetrics metrics = new MantoMetrics(registry, true);

        metrics.recordFailed("test-topic");

        assertEquals(1, registry.get("manto.messages.failed")
                .tag("topic", "test-topic")
                .tag("operation", "consume")
                .tag("outcome", "failure")
                .counter()
                .count());
    }

    @Test
    void recordsRetriedMetric() {
        MeterRegistry registry = new SimpleMeterRegistry();
        MantoMetrics metrics = new MantoMetrics(registry, true);

        metrics.recordRetried("test-topic");

        assertEquals(1, registry.get("manto.messages.retried")
                .tag("topic", "test-topic")
                .tag("operation", "retry")
                .tag("outcome", "attempt")
                .counter()
                .count());
    }

    @Test
    void recordsDltMetric() {
        MeterRegistry registry = new SimpleMeterRegistry();
        MantoMetrics metrics = new MantoMetrics(registry, true);

        metrics.recordDlt("test-topic");

        assertEquals(1, registry.get("manto.messages.dlt")
                .tag("topic", "test-topic")
                .tag("operation", "dlt")
                .tag("outcome", "published")
                .counter()
                .count());
    }

    @Test
    void recordsProcessingDuration() {
        MeterRegistry registry = new SimpleMeterRegistry();
        MantoMetrics metrics = new MantoMetrics(registry, true);

        Timer.Sample sample = metrics.startProcessingTimer();
        metrics.recordProcessingDuration(sample, "test-topic");

        assertNotNull(registry.get("manto.processing.duration")
                .tag("topic", "test-topic")
                .tag("operation", "process")
                .timer());
    }

    @Test
    void recordsProcessingDurationDirectly() {
        MeterRegistry registry = new SimpleMeterRegistry();
        MantoMetrics metrics = new MantoMetrics(registry, true);

        metrics.recordProcessingDuration(Duration.ofMillis(100), "test-topic");

        assertNotNull(registry.get("manto.processing.duration")
                .tag("topic", "test-topic")
                .tag("operation", "process")
                .timer());
    }

    @Test
    void doesNotRecordWhenDisabled() {
        MeterRegistry registry = new SimpleMeterRegistry();
        MantoMetrics metrics = new MantoMetrics(registry, false);

        metrics.recordPublished("test-topic");
        metrics.recordConsumed("test-topic");
        metrics.recordFailed("test-topic");
        metrics.recordRetried("test-topic");
        metrics.recordDlt("test-topic");
        metrics.recordProcessingDuration(Duration.ofMillis(100), "test-topic");

        assertEquals(0, registry.getMeters().size());
    }

    @Test
    void usesLowCardinalityTags() {
        MeterRegistry registry = new SimpleMeterRegistry();
        MantoMetrics metrics = new MantoMetrics(registry, true);

        metrics.recordPublished("test-topic");
        metrics.recordConsumed("test-topic");
        metrics.recordFailed("test-topic");
        metrics.recordRetried("test-topic");
        metrics.recordDlt("test-topic");

        // Verify no high-cardinality tags like event IDs or exception messages are used
        registry.getMeters().forEach(meter -> {
            meter.getId().getTags().forEach(tag -> {
                assertTrue(tag.getKey().equals("topic") ||
                        tag.getKey().equals("operation") ||
                        tag.getKey().equals("outcome"),
                        "Only low-cardinality tags should be used: " + tag.getKey());
                assertTrue(tag.getValue().matches("[a-zA-Z0-9._-]+"),
                        "Tag values should be simple: " + tag.getValue());
            });
        });
    }
}