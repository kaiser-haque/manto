package io.github.manto.core;

import java.util.List;
import java.util.Map;

/**
 * Framework-agnostic representation of a consumed message record.
 *
 * <p>This interface allows {@link DeadLetterHandler} to operate without
 * depending on Kafka-specific types. Implementations in manto-kafka adapt
 * Kafka's {@link org.apache.kafka.clients.consumer.ConsumerRecord} to this interface.</p>
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public interface MantoRecord<K, V> {

    /**
     * Returns the topic this record was received from.
     */
    String topic();

    /**
     * Returns the partition this record was received from.
     */
    int partition();

    /**
     * Returns the offset of this record in the partition.
     */
    long offset();

    /**
     * Returns the timestamp of this record.
     */
    long timestamp();

    /**
     * Returns the key of this record, or null if not present.
     */
    K key();

    /**
     * Returns the value of this record, or null if not present.
     */
    V value();

    /**
     * Returns all headers for this record.
     */
    List<MantoHeader> headers();

    /**
     * Returns the header value as a string, or null if not present.
     */
    default String header(String name) {
        return headers().stream()
                .filter(h -> h.key().equals(name))
                .findFirst()
                .map(MantoHeader::value)
                .orElse(null);
    }
}