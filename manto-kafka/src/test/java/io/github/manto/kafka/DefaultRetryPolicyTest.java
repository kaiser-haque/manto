package io.github.manto.kafka;

import io.github.manto.core.RetryPolicy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DefaultRetryPolicyTest {

    @Test
    void shouldCreateWithEnabledAndMaxAttempts() {
        RetryPolicy policy = new DefaultRetryPolicy(true, 5);

        assertTrue(policy.isEnabled());
        assertEquals(5, policy.maxAttempts());
    }

    @Test
    void shouldCreateWithDisabled() {
        RetryPolicy policy = new DefaultRetryPolicy(false, 3);

        assertFalse(policy.isEnabled());
        assertEquals(3, policy.maxAttempts());
    }

    @Test
    void shouldThrowWhenMaxAttemptsLessThanOne() {
        assertThrows(IllegalArgumentException.class, () -> new DefaultRetryPolicy(true, 0));
        assertThrows(IllegalArgumentException.class, () -> new DefaultRetryPolicy(true, -1));
    }
}