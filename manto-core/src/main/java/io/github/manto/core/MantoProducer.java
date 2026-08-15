package io.github.manto.core;

/**
 * Publishes typed events to a topic.
 *
 * <p>Manto wraps Spring Kafka rather than replacing it (ADR-004). This interface is the
 * framework abstraction; the Kafka-backed implementation lives in manto-kafka.</p>
 */
public interface MantoProducer {

    /**
     * Publishes an event to the given topic.
     *
     * @param topic the destination topic, must not be blank
     * @param event the event payload, must not be null
     * @param <T>   the event type
     */
    <T> void publish(String topic, T event);
}