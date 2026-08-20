package io.github.manto.kafka;

/**
 * Thrown when a {@link io.github.manto.core.MantoListener} method is invalid
 * or cannot be registered.
 *
 * <p>This is a configuration-time error: it is reported at application
 * startup and stops context refresh so the misconfiguration fails fast.</p>
 */
public class MantoListenerConfigurationException extends IllegalStateException {

    public MantoListenerConfigurationException(String message) {
        super(message);
    }

    public MantoListenerConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}