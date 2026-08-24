package io.github.manto.kafka;

import io.github.manto.core.RetryPolicy;

/**
 * Default implementation of {@link RetryPolicy} backed by configuration.
 */
public class DefaultRetryPolicy implements RetryPolicy {

    private final boolean enabled;
    private final int maxAttempts;

    public DefaultRetryPolicy(boolean enabled, int maxAttempts) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1");
        }
        this.enabled = enabled;
        this.maxAttempts = maxAttempts;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public int maxAttempts() {
        return maxAttempts;
    }
}