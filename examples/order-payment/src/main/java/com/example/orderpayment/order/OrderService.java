package com.example.orderpayment.order;

import io.github.manto.core.MantoProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Order Service — producer side.
 *
 * <p>Demonstrates {@link MantoProducer}: one line to publish a typed event.
 * Manto handles JSON serialization, header injection (eventId, correlationId,
 * eventType, source, timestamp), and Micrometer metrics - no KafkaTemplate
 * boilerplate in application code.
 */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final MantoProducer producer;

    public OrderService(MantoProducer producer) {
        this.producer = producer;
    }

    /**
     * Publishes an {@link OrderCreatedEvent} to {@code order-events}.
     *
     * @param orderId business order identifier
     * @param amount  order amount in minor units (e.g. cents)
     */
    public void placeOrder(String orderId, long amount) {
        OrderCreatedEvent event = new OrderCreatedEvent(orderId, amount);
        // producer: Manto adds Manto-Event-Id, Manto-Correlation-Id (=eventId by default),
        // Manto-Event-Type (=OrderCreatedEvent), Manto-Source, etc.
        producer.publish("order-events", event);
        log.info("Order published orderId={} amount={}", orderId, amount);
    }
}
