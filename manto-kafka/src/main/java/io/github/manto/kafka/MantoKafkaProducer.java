package io.github.manto.kafka;

import io.github.manto.core.MantoHeaders;
import io.github.manto.core.MantoProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * Kafka-backed {@link MantoProducer} implementation.
 *
 * <p>Publishes typed events through Spring Kafka. The underlying
 * {@link KafkaTemplate} is expected to use Spring's JSON serializer so the
 * event payload is encoded as JSON (FR-03). Publishing is synchronous: the
 * call returns only after the broker acknowledges the send, and failures are
 * surfaced to the caller.</p>
 *
 * <p>Standardized Manto headers are added to each published message:
 * {@code Manto-Event-Id}, {@code Manto-Event-Type}, {@code Manto-Event-Version},
 * {@code Manto-Correlation-Id}, and {@code Manto-Source}.</p>
 */
public class MantoKafkaProducer implements MantoProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String source;
    private final MantoMetrics metrics;

    public MantoKafkaProducer(KafkaTemplate<String, Object> kafkaTemplate, String source, MantoMetrics metrics) {
        this.kafkaTemplate = kafkaTemplate;
        this.source = source;
        this.metrics = metrics;
    }

    public MantoKafkaProducer(KafkaTemplate<String, Object> kafkaTemplate, String source) {
        this(kafkaTemplate, source, null);
    }

    public MantoKafkaProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this(kafkaTemplate, "unknown");
    }

    @Override
    public <T> void publish(String topic, T event) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be null or blank");
        }
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
        try {
            Message<T> message = buildMessage(topic, event);
            kafkaTemplate.send(message).get();
            if (metrics != null) {
                metrics.recordPublished(topic);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (metrics != null) {
                metrics.recordPublishedFailure(topic);
            }
            throw new MantoProducerException("Interrupted while publishing event to topic '" + topic + "'", e);
        } catch (ExecutionException e) {
            if (metrics != null) {
                metrics.recordPublishedFailure(topic);
            }
            throw new MantoProducerException("Failed to publish event to topic '" + topic + "'", e.getCause());
        }
    }

    private <T> Message<T> buildMessage(String topic, T event) {
        String eventId = UUID.randomUUID().toString();
        String eventType = event.getClass().getSimpleName();
        String eventVersion = "1.0";
        String correlationId = eventId;

        return MessageBuilder.withPayload(event)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(MantoHeaders.EVENT_ID, eventId)
                .setHeader(MantoHeaders.EVENT_TYPE, eventType)
                .setHeader(MantoHeaders.EVENT_VERSION, eventVersion)
                .setHeader(MantoHeaders.CORRELATION_ID, correlationId)
                .setHeader(MantoHeaders.SOURCE, source)
                .build();
    }
}