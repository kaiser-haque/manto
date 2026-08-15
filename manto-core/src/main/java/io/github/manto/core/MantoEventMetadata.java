package io.github.manto.core;

import java.time.Instant;

/**
 * Standard metadata carried by every Manto event.
 *
 * <p>Immutable value object. Serialization to and from Kafka headers is handled by the
 * manto-kafka module and does not change this contract.</p>
 *
 * @param eventId        unique identifier of the event
 * @param eventType      type name of the event
 * @param eventVersion   version of the event schema, e.g. "1.0"
 * @param correlationId  identifier correlating related events
 * @param source         application or service that produced the event
 * @param timestamp      instant at which the event was produced
 */
public record MantoEventMetadata(
        String eventId,
        String eventType,
        String eventVersion,
        String correlationId,
        String source,
        Instant timestamp) {
}