package io.github.manto.kafka;

import io.github.manto.core.MantoProducer;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.ExecutionException;

/**
 * Kafka-backed {@link MantoProducer} implementation.
 *
 * <p>Publishes typed events through Spring Kafka. The underlying
 * {@link KafkaTemplate} is expected to use Spring's JSON serializer so the
 * event payload is encoded as JSON (FR-03). Publishing is synchronous: the
 * call returns only after the broker acknowledges the send, and failures are
 * surfaced to the caller.</p>
 */
public class MantoKafkaProducer implements MantoProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public MantoKafkaProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
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
            kafkaTemplate.send(topic, event).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new MantoProducerException("Interrupted while publishing event to topic '" + topic + "'", e);
        } catch (ExecutionException e) {
            throw new MantoProducerException("Failed to publish event to topic '" + topic + "'", e.getCause());
        }
    }
}