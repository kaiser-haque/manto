package io.github.manto.core;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class RetryPolicyTest {

    @Test
    void shouldImplementRetryPolicy() {
        RetryPolicy policy = new RetryPolicy() {
            @Override
            public boolean isEnabled() {
                return true;
            }

            @Override
            public int maxAttempts() {
                return 3;
            }
        };

        assertTrue(policy.isEnabled());
        assertEquals(3, policy.maxAttempts());
    }
}