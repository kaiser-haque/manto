package io.github.manto.kafka;

import io.github.manto.core.ExceptionClassifier;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DefaultExceptionClassifierTest {

    @Test
    void shouldClassifyRuntimeExceptionAsRetryable() {
        ExceptionClassifier classifier = new DefaultExceptionClassifier();

        assertTrue(classifier.isRetryable(new RuntimeException("transient")));
        assertTrue(classifier.isRetryable(new Exception("generic")));
    }

    @Test
    void shouldClassifyIllegalArgumentExceptionAsNonRetryable() {
        ExceptionClassifier classifier = new DefaultExceptionClassifier();

        assertFalse(classifier.isRetryable(new IllegalArgumentException("bad data")));
    }

    @Test
    void shouldClassifyIllegalStateExceptionAsNonRetryable() {
        ExceptionClassifier classifier = new DefaultExceptionClassifier();

        assertFalse(classifier.isRetryable(new IllegalStateException("state issue")));
    }

    @Test
    void shouldClassifyNullPointerExceptionAsNonRetryable() {
        ExceptionClassifier classifier = new DefaultExceptionClassifier();

        assertFalse(classifier.isRetryable(new NullPointerException("null ref")));
    }

    @Test
    void shouldClassifySecurityExceptionAsNonRetryable() {
        ExceptionClassifier classifier = new DefaultExceptionClassifier();

        assertFalse(classifier.isRetryable(new SecurityException("access denied")));
    }

    @Test
    void shouldClassifyNullAsRetryable() {
        ExceptionClassifier classifier = new DefaultExceptionClassifier();

        assertTrue(classifier.isRetryable(null));
    }

    @Test
    void shouldAllowCustomNonRetryableTypes() {
        ExceptionClassifier classifier = new DefaultExceptionClassifier(
                java.util.Set.of(CustomException.class)
        );

        assertFalse(classifier.isRetryable(new CustomException()));
        assertTrue(classifier.isRetryable(new RuntimeException()));
    }

    static class CustomException extends RuntimeException {
    }
}