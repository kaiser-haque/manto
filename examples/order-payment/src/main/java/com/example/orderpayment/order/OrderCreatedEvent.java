package com.example.orderpayment.order;

/**
 * Event published by {@link OrderService} to {@code order-events}.
 *
 * <p>Kept intentionally small: just the identifiers a downstream service needs.
 * Manto serializes this as JSON and adds standard headers
 * ({@code Manto-Event-Id}, {@code Manto-Correlation-Id}, etc.) automatically.
 */
public record OrderCreatedEvent(
        String orderId,
        long amount
) {
}
