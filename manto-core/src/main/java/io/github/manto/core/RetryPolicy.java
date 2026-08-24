package io.github.manto.core;

/**
 * Defines the retry behavior for failed message processing.
 *
 * <p>Implementations determine whether retries are enabled and the maximum
 * number of attempts. The actual retry execution and backoff timing are
 * handled by the underlying messaging infrastructure (Spring Kafka).</p>
 */
public interface RetryPolicy {

    /**
     * Returns whether retry is enabled.
     *
     * @return true if retries should be attempted, false otherwise
     */
    boolean isEnabled();

    /**
     * Returns the maximum number of processing attempts including the initial attempt.
     *
     * @return maximum attempts, must be at least 1
     */
    int maxAttempts();
}