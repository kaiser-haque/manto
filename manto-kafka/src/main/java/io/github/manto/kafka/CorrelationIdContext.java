package io.github.manto.kafka;

/**
 * Thread-local holder for the current correlation ID.
 *
 * <p>Set by the {@link MantoListenerInterceptor} when a message arrives and cleared
 * after processing completes, so application code and logging frameworks can
 * access the correlation ID of the message currently being processed.</p>
 *
 * <p>This class is not thread-safe across threads; each consumer thread
 * maintains its own correlation ID.</p>
 */
public final class CorrelationIdContext {

    private static final ThreadLocal<String> CURRENT = new ThreadLocal<>();

    private CorrelationIdContext() {
    }

    /**
     * Returns the correlation ID for the message currently being processed,
     * or {@code null} if no correlation ID is set.
     *
     * @return the current correlation ID, or {@code null}
     */
    public static String get() {
        return CURRENT.get();
    }

    /**
     * Sets the correlation ID for the current thread.
     *
     * @param correlationId the correlation ID to set, must not be {@code null}
     */
    public static void set(String correlationId) {
        CURRENT.set(correlationId);
    }

    /**
     * Clears the correlation ID for the current thread.
     * Must be called after message processing completes to prevent leaks
     * across messages on pooled threads.
     */
    public static void clear() {
        CURRENT.remove();
    }
}
