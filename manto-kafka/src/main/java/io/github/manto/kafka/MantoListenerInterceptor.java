package io.github.manto.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.kafka.listener.RecordInterceptor;

/**
 * Interceptor for recording consumer metrics.
 */
public class MantoListenerInterceptor implements RecordInterceptor<String, Object> {

    private final MantoMetrics metrics;
    private static final ThreadLocal<io.micrometer.core.instrument.Timer.Sample> processingTimer = new ThreadLocal<>();

    public MantoListenerInterceptor(MantoMetrics metrics) {
        this.metrics = metrics;
    }

    public MantoMetrics getMetrics() {
        return metrics;
    }

    @Override
    public ConsumerRecord<String, Object> intercept(ConsumerRecord<String, Object> record, Consumer<String, Object> consumer) {
        if (metrics != null) {
            metrics.recordConsumed(record.topic());
            processingTimer.set(metrics.startProcessingTimer());
        }
        return record;
    }

    public void recordProcessingDuration(String topic) {
        io.micrometer.core.instrument.Timer.Sample sample = processingTimer.get();
        if (sample != null && metrics != null) {
            metrics.recordProcessingDuration(sample, topic);
            processingTimer.remove();
        }
    }

    public void recordFailed(String topic) {
        processingTimer.remove();
        if (metrics != null) {
            metrics.recordFailed(topic);
        }
    }
}