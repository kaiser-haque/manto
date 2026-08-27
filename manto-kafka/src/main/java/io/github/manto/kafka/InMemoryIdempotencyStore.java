package io.github.manto.kafka;

import io.github.manto.core.IdempotencyStore;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory implementation of {@link IdempotencyStore}.
 *
 * <p>Uses a thread-safe {@link ConcurrentHashMap} to track processed event IDs.
 * This implementation is <strong>not suitable for multi-instance production deployments</strong>
 * because the store is not shared across instances. For production use with multiple instances,
 * a distributed store (e.g., Redis, database) must be used.
 *
 * <p>This implementation is intended for:
 * <ul>
 *   <li>Single-instance deployments</li>
 *   <li>Development and testing</li>
 *   <li>As a reference for custom implementations</li>
 * </ul>
 */
public class InMemoryIdempotencyStore implements IdempotencyStore {

    private final Set<String> processedEvents = ConcurrentHashMap.newKeySet();

    @Override
    public boolean isProcessed(String eventId) {
        return processedEvents.contains(eventId);
    }

    @Override
    public void markProcessed(String eventId) {
        processedEvents.add(eventId);
    }
}