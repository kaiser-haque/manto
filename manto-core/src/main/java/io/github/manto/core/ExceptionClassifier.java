package io.github.manto.core;

/**
 * Classifies exceptions as retryable or non-retryable.
 *
 * <p>Retryable exceptions indicate transient failures (e.g., network issues,
 * temporary resource unavailability) where a retry may succeed. Non-retryable
 * exceptions indicate permanent failures (e.g., data corruption, invalid schema)
 * where retries would not help and the message should go directly to the DLT.</p>
 */
public interface ExceptionClassifier {

    /**
     * Determines if the given exception is retryable.
     *
     * @param throwable the exception to classify
     * @return true if the exception is retryable, false if it should go directly to DLT
     */
    boolean isRetryable(Throwable throwable);
}