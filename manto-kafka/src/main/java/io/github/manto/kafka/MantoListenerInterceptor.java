package io.github.manto.kafka;

import io.github.manto.core.MantoHeaders;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.springframework.kafka.listener.RecordInterceptor;

/**
 * Interceptor for recording consumer metrics and propagating correlation IDs.
 *
 * <p>Sets the correlation ID from the incoming record's {@code Manto-Correlation-Id}
 * header into the {@link CorrelationIdContext} so application code can access it
 * during processing. Clears the context after processing completes.</p>
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
        String correlationId = extractCorrelationId(record);
        CorrelationIdContext.set(correlationId);
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
        CorrelationIdContext.clear();
    }

    public void recordFailed(String topic) {
        processingTimer.remove();
        CorrelationIdContext.clear();
        if (metrics != null) {
            metrics.recordFailed(topic);
        }
    }

    private String extractCorrelationId(ConsumerRecord<?, ?> record) {
        org.apache.kafka.common.header.Header header = record.headers().lastHeader(MantoHeaders.CORRELATION_ID);
        if (header != null) {
            return new String(header.value(), java.nio.charset.StandardCharsets.UTF_8);
        }
        org.apache.kafka.common.header.Header eventIdHeader = record.headers().lastHeader(MantoHeaders.EVENT_ID);
        return eventIdHeader != null ? new String(eventIdHeader.value(), java.nio.charset.StandardCharsets.UTF_8) : null;
    }
}