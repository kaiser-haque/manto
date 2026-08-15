package io.github.manto.core;

/**
 * Standardized Manto-prefixed Kafka header names.
 *
 * <p>Sensitive information must never be serialized into these headers.</p>
 */
public final class MantoHeaders {

    public static final String EVENT_ID = "Manto-Event-Id";
    public static final String EVENT_TYPE = "Manto-Event-Type";
    public static final String EVENT_VERSION = "Manto-Event-Version";
    public static final String CORRELATION_ID = "Manto-Correlation-Id";
    public static final String SOURCE = "Manto-Source";

    private MantoHeaders() {
    }
}