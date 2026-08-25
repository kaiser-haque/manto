package io.github.manto.kafka;

import io.github.manto.core.ExceptionClassifier;

import java.util.Set;

/**
 * Default exception classifier that treats specific exception types as non-retryable.
 *
 * <p>By default, the following are considered non-retryable (permanent failures):
 * <ul>
 *   <li>{@link IllegalArgumentException} - invalid message format/data</li>
 *   <li>{@link IllegalStateException} - invalid state for processing</li>
 *   <li>{@link NullPointerException} - programming error</li>
 *   <li>{@link SecurityException} - authorization failure</li>
 * </ul>
 *
 * <p>All other exceptions are considered retryable (transient failures).</p>
 */
public class DefaultExceptionClassifier implements ExceptionClassifier {

    private final Set<Class<? extends Throwable>> nonRetryableTypes;

    public DefaultExceptionClassifier() {
        this(Set.of(
                IllegalArgumentException.class,
                IllegalStateException.class,
                NullPointerException.class,
                SecurityException.class
        ));
    }

    public DefaultExceptionClassifier(Set<Class<? extends Throwable>> nonRetryableTypes) {
        this.nonRetryableTypes = Set.copyOf(nonRetryableTypes);
    }

    public Set<Class<? extends Throwable>> getNonRetryableTypes() {
        return nonRetryableTypes;
    }

    @Override
    public boolean isRetryable(Throwable throwable) {
        if (throwable == null) {
            return true;
        }
        for (Class<? extends Throwable> nonRetryableType : nonRetryableTypes) {
            if (nonRetryableType.isInstance(throwable)) {
                return false;
            }
        }
        return true;
    }
}