package io.github.manto.kafka;

/**
 * Exception thrown when JSON deserialization fails.
 *
 * <p>Wraps the underlying Jackson exception with context about the target
 * type and the payload that failed to deserialize, enabling diagnostic
 * logging without exposing sensitive payload content by default.</p>
 */
public class MantoDeserializationException extends RuntimeException {

    private final Class<?> targetType;
    private final String payloadPreview;

    public MantoDeserializationException(Class<?> targetType, String payloadPreview, Throwable cause) {
        super("Failed to deserialize payload to type " + (targetType != null ? targetType.getName() : "unknown"), cause);
        this.targetType = targetType;
        this.payloadPreview = payloadPreview;
    }

    /**
     * Returns the target type that deserialization was attempted for.
     *
     * @return the target class, never null
     */
    public Class<?> getTargetType() {
        return targetType;
    }

    /**
     * Returns a preview of the payload that failed to deserialize.
     *
     * <p>The preview is truncated to avoid logging large or sensitive payloads
     * in full. Implementations should ensure sensitive data is not included
     * or is masked.</p>
     *
     * @return a preview of the problematic payload, never null
     */
    public String getPayloadPreview() {
        return payloadPreview;
    }
}