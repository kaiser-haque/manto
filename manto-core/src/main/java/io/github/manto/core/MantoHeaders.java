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

    // Dead Letter Topic headers
    public static final String DLT_ORIGINAL_TOPIC = "Manto-DLT-Original-Topic";
    public static final String DLT_ORIGINAL_PARTITION = "Manto-DLT-Original-Partition";
    public static final String DLT_ORIGINAL_OFFSET = "Manto-DLT-Original-Offset";
    public static final String DLT_ORIGINAL_TIMESTAMP = "Manto-DLT-Original-Timestamp";
    public static final String DLT_EXCEPTION_CLASS = "Manto-DLT-Exception-Class";
    public static final String DLT_EXCEPTION_MESSAGE = "Manto-DLT-Exception-Message";
    public static final String DLT_RETRY_COUNT = "Manto-DLT-Retry-Count";
    public static final String DLT_FAILURE_TIMESTAMP = "Manto-DLT-Failure-Timestamp";
    public static final String DLT_TRACE_ID = "Manto-DLT-Trace-Id";

    private MantoHeaders() {
    }
}