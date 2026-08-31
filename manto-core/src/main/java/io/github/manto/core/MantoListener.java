package io.github.manto.core;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a method as a Manto event handler for a topic.
 *
 * <p>The Spring Boot auto-configuration module registers methods annotated with this
 * annotation as Kafka listeners.</p>
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface MantoListener {

    /**
     * The topic this handler consumes events from.
     *
     * @return the topic name, never blank
     */
    String topic();

    /**
     * The consumer group id for this handler.
     *
     * @return the consumer group id, never blank
     */
    String groupId();
}