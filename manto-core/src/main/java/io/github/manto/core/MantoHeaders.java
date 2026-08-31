package io.github.manto.core;

/**
 * Standardized Manto-prefixed Kafka header names.
 *
 * <p>Sensitive information must never be serialized into these headers.</p>
 */
public final class MantoHeaders {

    /** Header for the unique event identifier. */
    public static final String EVENT_ID = "Manto-Event-Id";
    /** Header for the event type name. */
    public static final String EVENT_TYPE = "Manto-Event-Type";
    /** Header for the event schema version. */
    public static final String EVENT_VERSION = "Manto-Event-Version";
    /** Header for the correlation identifier. */
    public static final String CORRELATION_ID = "Manto-Correlation-Id";
    /** Header for the source service identifier. */
    public static final String SOURCE = "Manto-Source";

    // Dead Letter Topic headers
    /** DLT header carrying the original topic name. */
    public static final String DLT_ORIGINAL_TOPIC = "Manto-DLT-Original-Topic";
    /** DLT header carrying the original partition. */
    public static final String DLT_ORIGINAL_PARTITION = "Manto-DLT-Original-Partition";
    /** DLT header carrying the original offset. */
    public static final String DLT_ORIGINAL_OFFSET = "Manto-DLT-Original-Offset";
    /** DLT header carrying the original timestamp. */
    public static final String DLT_ORIGINAL_TIMESTAMP = "Manto-DLT-Original-Timestamp";
    /** DLT header carrying the exception class name. */
    public static final String DLT_EXCEPTION_CLASS = "Manto-DLT-Exception-Class";
    /** DLT header carrying the exception message. */
    public static final String DLT_EXCEPTION_MESSAGE = "Manto-DLT-Exception-Message";
    /** DLT header carrying the retry count. */
    public static final String DLT_RETRY_COUNT = "Manto-DLT-Retry-Count";
    /** DLT header carrying the failure timestamp. */
    public static final String DLT_FAILURE_TIMESTAMP = "Manto-DLT-Failure-Timestamp";
    /** DLT header carrying the trace identifier. */
    public static final String DLT_TRACE_ID = "Manto-DLT-Trace-Id";

    private MantoHeaders() {
    }
}