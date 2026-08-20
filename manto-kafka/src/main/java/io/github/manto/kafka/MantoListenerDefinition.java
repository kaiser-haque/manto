package io.github.manto.kafka;

import java.lang.reflect.Method;

/**
 * A discovered {@link io.github.manto.core.MantoListener} handler.
 *
 * <p>Binds an annotated method on a bean instance to a topic and consumer
 * group. Instances are immutable.</p>
 *
 * @param bean    the bean instance owning the annotated method
 * @param method  the annotated handler method
 * @param topic   the topic the handler consumes from
 * @param groupId the consumer group id of the handler
 */
public record MantoListenerDefinition(Object bean, Method method, String topic, String groupId) {
}