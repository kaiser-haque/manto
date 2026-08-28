package io.github.manto.kafka;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.util.List;

/**
 * Micrometer metrics for Manto Kafka operations.
 *
 * <p>Provides low-cardinality metrics for published, consumed, failed, retried,
 * DLT messages, and processing duration.</p>
 */
public class MantoMetrics {

    public static final String PUBLISHED = "manto.messages.published";
    public static final String CONSUMED = "manto.messages.consumed";
    public static final String FAILED = "manto.messages.failed";
    public static final String RETRIED = "manto.messages.retried";
    public static final String DLT = "manto.messages.dlt";
    public static final String PROCESSING_DURATION = "manto.processing.duration";

    private static final String TAG_TOPIC = "topic";
    private static final String TAG_OPERATION = "operation";
    private static final String TAG_OUTCOME = "outcome";

    private final MeterRegistry registry;
    private final boolean enabled;

    public MantoMetrics(MeterRegistry registry, boolean enabled) {
        this.registry = registry;
        this.enabled = enabled;
    }

    public void recordPublished(String topic) {
        if (!enabled) return;
        registry.counter(PUBLISHED, TAG_TOPIC, topic, TAG_OPERATION, "publish", TAG_OUTCOME, "success").increment();
    }

    public void recordPublishedFailure(String topic) {
        if (!enabled) return;
        registry.counter(PUBLISHED, TAG_TOPIC, topic, TAG_OPERATION, "publish", TAG_OUTCOME, "failure").increment();
    }

    public void recordConsumed(String topic) {
        if (!enabled) return;
        registry.counter(CONSUMED, TAG_TOPIC, topic, TAG_OPERATION, "consume", TAG_OUTCOME, "success").increment();
    }

    public void recordFailed(String topic) {
        if (!enabled) return;
        registry.counter(FAILED, TAG_TOPIC, topic, TAG_OPERATION, "consume", TAG_OUTCOME, "failure").increment();
    }

    public void recordRetried(String topic) {
        if (!enabled) return;
        registry.counter(RETRIED, TAG_TOPIC, topic, TAG_OPERATION, "retry", TAG_OUTCOME, "attempt").increment();
    }

    public void recordDlt(String topic) {
        if (!enabled) return;
        registry.counter(DLT, TAG_TOPIC, topic, TAG_OPERATION, "dlt", TAG_OUTCOME, "published").increment();
    }

    public Timer.Sample startProcessingTimer() {
        return Timer.start(registry);
    }

    public void recordProcessingDuration(Timer.Sample sample, String topic) {
        if (!enabled) return;
        sample.stop(Timer.builder(PROCESSING_DURATION)
                .tag(TAG_TOPIC, topic)
                .tag(TAG_OPERATION, "process")
                .publishPercentileHistogram()
                .register(registry));
    }

    public void recordProcessingDuration(Duration duration, String topic) {
        if (!enabled) return;
        registry.timer(PROCESSING_DURATION, TAG_TOPIC, topic, TAG_OPERATION, "process").record(duration);
    }
}