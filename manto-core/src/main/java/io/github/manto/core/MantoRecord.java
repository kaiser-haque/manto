package io.github.manto.core;

import java.util.List;
import java.util.Map;

/**
 * Framework-agnostic representation of a consumed message record.
 *
 * <p>This interface allows {@link DeadLetterHandler} to operate without
 * depending on Kafka-specific types. Implementations in manto-kafka adapt
 * Kafka's {@code ConsumerRecord} to this interface.</p>
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public interface MantoRecord<K, V> {

    /**
     * Returns the topic this record was received from.
     *
     * @return the topic name
     */
    String topic();

    /**
     * Returns the partition this record was received from.
     *
     * @return the partition number
     */
    int partition();

    /**
     * Returns the offset of this record in the partition.
     *
     * @return the offset
     */
    long offset();

    /**
     * Returns the timestamp of this record.
     *
     * @return the record timestamp in milliseconds since epoch
     */
    long timestamp();

    /**
     * Returns the key of this record, or null if not present.
     *
     * @return the key, or null
     */
    K key();

    /**
     * Returns the value of this record, or null if not present.
     *
     * @return the value, or null
     */
    V value();

    /**
     * Returns all headers for this record.
     *
     * @return the list of headers, never null
     */
    List<MantoHeader> headers();

    /**
     * Returns the header value as a string, or null if not present.
     *
     * @param name the header name to look up
     * @return the header value, or null if not present
     */
    default String header(String name) {
        return headers().stream()
                .filter(h -> h.key().equals(name))
                .findFirst()
                .map(MantoHeader::value)
                .orElse(null);
    }
}