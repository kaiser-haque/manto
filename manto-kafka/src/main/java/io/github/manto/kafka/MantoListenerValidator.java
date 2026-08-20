package io.github.manto.kafka;

import io.github.manto.core.MantoListener;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Validates that a method annotated with {@link MantoListener} is a legal
 * event handler.
 *
 * <p>A legal handler is a public, non-static method with exactly one
 * parameter (the event payload) and a non-blank topic and group id.
 * Violations are configuration errors and fail fast at startup.</p>
 */
public class MantoListenerValidator {

    /**
     * Validates the given method and annotation values.
     *
     * @param method  the annotated method
     * @param topic   the topic from the annotation
     * @param groupId the group id from the annotation
     * @throws MantoListenerConfigurationException if the handler is not valid
     */
    public void validate(Method method, String topic, String groupId) {
        Class<?> declaringClass = method.getDeclaringClass();
        String location = declaringClass.getName() + "#" + method.getName();

        if (topic == null || topic.isBlank()) {
            throw new MantoListenerConfigurationException(
                    "MantoListener on " + location + " must declare a non-blank topic");
        }
        if (groupId == null || groupId.isBlank()) {
            throw new MantoListenerConfigurationException(
                    "MantoListener on " + location + " must declare a non-blank groupId");
        }
        if (!Modifier.isPublic(method.getModifiers())) {
            throw new MantoListenerConfigurationException(
                    "MantoListener method " + location + " must be public");
        }
        if (Modifier.isStatic(method.getModifiers())) {
            throw new MantoListenerConfigurationException(
                    "MantoListener method " + location + " must not be static");
        }
        if (method.getParameterCount() != 1) {
            throw new MantoListenerConfigurationException(
                    "MantoListener method " + location + " must declare exactly one parameter (the event payload), "
                            + "but declares " + method.getParameterCount());
        }
    }
}