package com.example.orderpayment.payment;

import com.example.orderpayment.order.OrderCreatedEvent;
import io.github.manto.core.MantoListener;
import io.github.manto.kafka.CorrelationIdContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Observes the dead-letter topic for exhausted retries.
 *
 * <p>When {@link PaymentHandler} throws beyond {@code manto.retry.max-attempts},
 * or throws a non-retryable exception, the error handler publishes the original
 * record to {@code order-events.DLT} with diagnostic headers:
 * {@code Manto-DLT-Original-Topic}, {@code Manto-DLT-Exception-Class},
 * {@code Manto-DLT-Exception-Message}, {@code Manto-DLT-Retry-Count}, etc.
 *
 * <p>This handler exists purely for visibility — in production you might alert,
 * persist to a store, or trigger manual replay. No retry or idempotency is
 * applied here; the message is already considered poison.
 */
@Component
public class PaymentDltHandler {

    private static final Logger log = LoggerFactory.getLogger(PaymentDltHandler.class);

    @MantoListener(topic = "order-events.DLT", groupId = "payment-service-dlt")
    public void handleDlt(OrderCreatedEvent event) {
        String correlationId = CorrelationIdContext.get();
        // Never log full sensitive payloads; orderId + correlationId is enough to correlate.
        log.warn("DLT received orderId={} amount={} correlationId={} - requires manual investigation",
                event.orderId(), event.amount(), correlationId);
    }
}
