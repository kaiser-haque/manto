package io.github.manto.core;

import java.time.Duration;

/**
 * Defines the backoff delay between retry attempts.
 *
 * <p>Implementations calculate the delay before the next retry attempt
 * based on the attempt number (1-based, where 1 is the first retry after
 * the initial failure).</p>
 */
public interface BackoffStrategy {

    /**
     * Returns the delay before the next retry attempt.
     *
     * @param attempt the retry attempt number (1 = first retry, 2 = second, etc.)
     * @return the delay duration, never null
     */
    Duration nextDelay(int attempt);
}