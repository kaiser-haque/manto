package io.github.manto.kafka;

import io.github.manto.core.BackoffStrategy;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class ExponentialBackoffStrategyTest {

    @Test
    void shouldCalculateExponentialBackoff() {
        BackoffStrategy strategy = new ExponentialBackoffStrategy(
                Duration.ofMillis(1000),
                2.0,
                Duration.ofMillis(30000)
        );

        assertEquals(Duration.ofMillis(1000), strategy.nextDelay(1));
        assertEquals(Duration.ofMillis(2000), strategy.nextDelay(2));
        assertEquals(Duration.ofMillis(4000), strategy.nextDelay(3));
        assertEquals(Duration.ofMillis(8000), strategy.nextDelay(4));
    }

    @Test
    void shouldCapAtMaxDelay() {
        BackoffStrategy strategy = new ExponentialBackoffStrategy(
                Duration.ofMillis(10000),
                2.0,
                Duration.ofMillis(15000)
        );

        assertEquals(Duration.ofMillis(10000), strategy.nextDelay(1));
        assertEquals(Duration.ofMillis(15000), strategy.nextDelay(2)); // capped
        assertEquals(Duration.ofMillis(15000), strategy.nextDelay(3)); // capped
    }

    @Test
    void shouldThrowWhenAttemptLessThanOne() {
        BackoffStrategy strategy = new ExponentialBackoffStrategy(
                Duration.ofMillis(1000), 2.0, Duration.ofMillis(30000)
        );

        assertThrows(IllegalArgumentException.class, () -> strategy.nextDelay(0));
        assertThrows(IllegalArgumentException.class, () -> strategy.nextDelay(-1));
    }

    @Test
    void shouldThrowWhenInitialDelayNotPositive() {
        assertThrows(IllegalArgumentException.class,
                () -> new ExponentialBackoffStrategy(Duration.ZERO, 2.0, Duration.ofMillis(30000)));
        assertThrows(IllegalArgumentException.class,
                () -> new ExponentialBackoffStrategy(Duration.ofMillis(-1), 2.0, Duration.ofMillis(30000)));
    }

    @Test
    void shouldThrowWhenMultiplierLessThanOne() {
        assertThrows(IllegalArgumentException.class,
                () -> new ExponentialBackoffStrategy(Duration.ofMillis(1000), 0.5, Duration.ofMillis(30000)));
    }

    @Test
    void shouldThrowWhenMaxDelayNotPositive() {
        assertThrows(IllegalArgumentException.class,
                () -> new ExponentialBackoffStrategy(Duration.ofMillis(1000), 2.0, Duration.ZERO));
        assertThrows(IllegalArgumentException.class,
                () -> new ExponentialBackoffStrategy(Duration.ofMillis(1000), 2.0, Duration.ofMillis(-1)));
    }
}