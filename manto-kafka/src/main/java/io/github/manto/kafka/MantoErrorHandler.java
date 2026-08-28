package io.github.manto.kafka;

import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.BackOff;

/**
 * Error handler that records retry and failure metrics.
 */
public class MantoErrorHandler extends DefaultErrorHandler {

    private final MantoListenerInterceptor interceptor;

    public MantoErrorHandler(MantoListenerInterceptor interceptor, BackOff backOff) {
        super(backOff);
        this.interceptor = interceptor;
    }

    public MantoErrorHandler(MantoListenerInterceptor interceptor, ConsumerRecordRecoverer recoverer, BackOff backOff) {
        super(recoverer, backOff);
        this.interceptor = interceptor;
    }
}