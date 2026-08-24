package io.github.manto.kafka;

import io.github.manto.core.BackoffStrategy;

import java.time.Duration;

/**
 * Exponential backoff strategy with configurable initial delay, multiplier, and maximum delay.
 *
 * <p>The delay for attempt {@code n} is calculated as:
 * {@code min(initialDelay * multiplier^(n-1), maxDelay)}</p>
 */
public class ExponentialBackoffStrategy implements BackoffStrategy {

    private final Duration initialDelay;
    private final double multiplier;
    private final Duration maxDelay;

    public ExponentialBackoffStrategy(Duration initialDelay, double multiplier, Duration maxDelay) {
        if (initialDelay == null || initialDelay.isNegative() || initialDelay.isZero()) {
            throw new IllegalArgumentException("initialDelay must be positive");
        }
        if (multiplier < 1.0) {
            throw new IllegalArgumentException("multiplier must be at least 1.0");
        }
        if (maxDelay == null || maxDelay.isNegative() || maxDelay.isZero()) {
            throw new IllegalArgumentException("maxDelay must be positive");
        }
        this.initialDelay = initialDelay;
        this.multiplier = multiplier;
        this.maxDelay = maxDelay;
    }

    public Duration getInitialDelay() {
        return initialDelay;
    }

    public double getMultiplier() {
        return multiplier;
    }

    public Duration getMaxDelay() {
        return maxDelay;
    }

    @Override
    public Duration nextDelay(int attempt) {
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be at least 1");
        }
        long delayMillis = (long) (initialDelay.toMillis() * Math.pow(multiplier, attempt - 1));
        Duration calculated = Duration.ofMillis(delayMillis);
        return calculated.compareTo(maxDelay) > 0 ? maxDelay : calculated;
    }
}