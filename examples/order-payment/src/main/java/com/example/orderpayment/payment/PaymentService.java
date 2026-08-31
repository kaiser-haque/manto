package com.example.orderpayment.payment;

import io.github.manto.core.MantoProducer;
import io.github.manto.kafka.CorrelationIdContext;
import io.github.manto.kafka.MantoKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;

/**
 * Payment Service — producer side for the second hop.
 *
 * <p>Demonstrates correlation ID propagation: the incoming
 * {@code Manto-Correlation-Id} from {@code order-events} is forwarded to
 * {@code payment-events} so logs and traces can be correlated across services.
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final MantoProducer producer;

    public PaymentService(MantoProducer producer) {
        this.producer = producer;
    }

    public void completePayment(String orderId) {
        PaymentCompletedEvent event = new PaymentCompletedEvent(orderId, "COMPLETED", Instant.now());
        String correlationId = CorrelationIdContext.get();

        // metadata: propagate the upstream correlation ID downstream.
        // If correlationId is null (e.g. outside a listener), Manto generates one.
        if (producer instanceof MantoKafkaProducer kafkaProducer) {
            kafkaProducer.publish("payment-events", event, correlationId);
        } else {
            producer.publish("payment-events", event);
        }
        log.info("Payment completed orderId={} correlationId={}", orderId, correlationId);
    }
}
