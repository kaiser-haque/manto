package io.github.manto.kafka;

/**
 * Thrown when a Manto producer operation fails, e.g. Kafka send failures.
 *
 * <p>Wraps the underlying cause so callers can react to publish failures
 * without depending on Spring Kafka exception types.</p>
 */
public class MantoProducerException extends RuntimeException {

    public MantoProducerException(String message, Throwable cause) {
        super(message, cause);
    }
}