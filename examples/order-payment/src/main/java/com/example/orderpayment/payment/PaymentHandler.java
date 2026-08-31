package com.example.orderpayment.payment;

import com.example.orderpayment.order.OrderCreatedEvent;
import io.github.manto.core.IdempotencyStore;
import io.github.manto.core.MantoListener;
import io.github.manto.kafka.CorrelationIdContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Payment Service — consumer side.
 *
 * <p>Demonstrates:
 * <ul>
 *   <li>{@code @MantoListener} — one annotation, no manual container setup</li>
 *   <li>metadata — {@link CorrelationIdContext} exposes the {@code Manto-Correlation-Id}
 *       header that Manto injects on every publish</li>
 *   <li>retry — transient failures ({@link RuntimeException}) are retried with
 *       exponential backoff per {@code manto.retry.*} properties</li>
 *   <li>DLT — exhausted retries or non-retryable failures route to
 *       {@code order-events.DLT} (see {@link PaymentDltHandler})</li>
 *   <li>idempotency — {@link IdempotencyStore} guards against duplicate processing
 *       on redelivery (at-least-once semantics)</li>
 * </ul>
 */
@Component
public class PaymentHandler {

    private static final Logger log = LoggerFactory.getLogger(PaymentHandler.class);

    private final PaymentService paymentService;
    private final IdempotencyStore idempotencyStore;

    public PaymentHandler(PaymentService paymentService, IdempotencyStore idempotencyStore) {
        this.paymentService = paymentService;
        this.idempotencyStore = idempotencyStore;
    }

    @MantoListener(topic = "order-events", groupId = "payment-service")
    public void handle(OrderCreatedEvent event) {
        // metadata: Manto's interceptor populates CorrelationIdContext from the
        // Manto-Correlation-Id header (falls back to Manto-Event-Id).
        String correlationId = CorrelationIdContext.get();
        log.info("Handling orderId={} amount={} correlationId={}",
                event.orderId(), event.amount(), correlationId);

        // idempotency: skip duplicates. In this minimal example the correlationId
        // doubles as the idempotency key (by default it equals the producer's eventId).
        // Real systems might use eventId or a business key (e.g. orderId) instead.
        if (correlationId != null && idempotencyStore.isProcessed(correlationId)) {
            log.info("Duplicate detected correlationId={} orderId={} - skipping", correlationId, event.orderId());
            return;
        }

        // Permanent failure — classified as non-retryable (IllegalArgumentException):
        // Manto routes straight to DLT without retry. See docs/ERROR_HANDLING.md.
        if (event.amount() <= 0) {
            throw new IllegalArgumentException("Invalid amount: " + event.amount());
        }

        // Transient failure — classified as retryable (RuntimeException):
        // Manto retries with exponential backoff (manto.retry.backoff.*) up to
        // manto.retry.max-attempts, then routes to DLT.
        // Use amount==999 to simulate a flaky payment gateway in demos.
        if (event.amount() == 999) {
            throw new RuntimeException("Transient payment gateway timeout orderId=" + event.orderId());
        }

        paymentService.completePayment(event.orderId());

        // Mark as processed only after successful handling — at-least-once + idempotency
        // gives effectively-once for this single-instance demo. For multi-instance
        // deployments provide a distributed IdempotencyStore (Redis/DB) bean.
        if (correlationId != null) {
            idempotencyStore.markProcessed(correlationId);
        }
    }
}
