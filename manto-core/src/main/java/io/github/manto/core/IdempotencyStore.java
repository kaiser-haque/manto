package io.github.manto.core;

/**
 * Tracks which events have already been processed.
 *
 * <p>The in-memory implementation provided in v1.0 is not suitable for multi-instance
 * production deployments (ADR-005); external stores may be added later.</p>
 */
public interface IdempotencyStore {

    /**
     * Returns whether the event with the given id has already been processed.
     *
     * @param eventId the event id
     * @return {@code true} if the event was already processed
     */
    boolean isProcessed(String eventId);

    /**
     * Marks the event with the given id as processed.
     *
     * @param eventId the event id
     */
    void markProcessed(String eventId);
}