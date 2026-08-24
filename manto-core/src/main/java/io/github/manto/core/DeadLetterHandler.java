package io.github.manto.core;

/**
 * Handles messages that have exhausted all retry attempts or are non-retryable.
 *
 * <p>Implementations are responsible for routing the failed message to a
 * dead-letter topic with diagnostic metadata. The handler receives the
 * original record, the exception that caused the failure, and contextual
 * metadata including retry count.</p>
 */
public interface DeadLetterHandler {

    /**
     * Handles a failed message by sending it to a dead-letter topic.
     *
     * @param record       the original consumer record that failed
     * @param exception    the exception that caused the failure
     * @param retryCount   the number of retry attempts that were made
     * @param <K>          the record key type
     * @param <V>          the record value type
     */
    <K, V> void handle(MantoRecord<K, V> record, Throwable exception, int retryCount);
}