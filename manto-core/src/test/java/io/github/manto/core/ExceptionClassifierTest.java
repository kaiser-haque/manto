package io.github.manto.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ExceptionClassifierTest {

    @Test
    void shouldImplementExceptionClassifier() {
        ExceptionClassifier classifier = throwable -> !(throwable instanceof IllegalArgumentException);

        assertTrue(classifier.isRetryable(new RuntimeException("transient")));
        assertFalse(classifier.isRetryable(new IllegalArgumentException("permanent")));
        assertTrue(classifier.isRetryable(null));
    }
}