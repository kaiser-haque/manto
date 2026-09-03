package io.github.manto.kafka;

import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.util.backoff.BackOff;

/**
 * Error handler that records retry and failure metrics.
 *
 * <p>Registers a {@link RetryListener} so every failed delivery handled here
 * increments {@code manto.messages.retried}. Per-record failure and processing
 * duration metrics are recorded by {@link MantoListenerInterceptor} through the
 * container's success/failure callbacks.</p>
 */
public class MantoErrorHandler extends DefaultErrorHandler {

    private final MantoListenerInterceptor interceptor;

    public MantoErrorHandler(MantoListenerInterceptor interceptor, BackOff backOff) {
        super(backOff);
        this.interceptor = interceptor;
        setRetryListeners(retryListener());
    }

    public MantoErrorHandler(MantoListenerInterceptor interceptor, ConsumerRecordRecoverer recoverer, BackOff backOff) {
        super(recoverer, backOff);
        this.interceptor = interceptor;
        setRetryListeners(retryListener());
    }

    private RetryListener retryListener() {
        return (record, exception, deliveryAttempt) -> {
            MantoMetrics metrics = interceptor != null ? interceptor.getMetrics() : null;
            if (metrics != null) {
                metrics.recordRetried(record.topic());
            }
        };
    }
}