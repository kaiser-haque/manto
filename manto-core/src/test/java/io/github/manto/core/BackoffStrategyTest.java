package io.github.manto.core;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class BackoffStrategyTest {

    @Test
    void shouldImplementBackoffStrategy() {
        BackoffStrategy strategy = attempt -> Duration.ofMillis(100L * attempt);

        assertEquals(Duration.ofMillis(100), strategy.nextDelay(1));
        assertEquals(Duration.ofMillis(200), strategy.nextDelay(2));
        assertEquals(Duration.ofMillis(300), strategy.nextDelay(3));
    }
}