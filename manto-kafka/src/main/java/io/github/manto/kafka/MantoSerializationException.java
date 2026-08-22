package io.github.manto.kafka;

/**
 * Exception thrown when JSON serialization fails.
 */
public class MantoSerializationException extends RuntimeException {

    public MantoSerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}