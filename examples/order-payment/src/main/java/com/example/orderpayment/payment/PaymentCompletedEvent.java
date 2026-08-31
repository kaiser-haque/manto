package com.example.orderpayment.payment;

import java.time.Instant;

/**
 * Event published by {@link PaymentService} to {@code payment-events}
 * after a payment is processed.
 */
public record PaymentCompletedEvent(
        String orderId,
        String status,
        Instant processedAt
) {
}
