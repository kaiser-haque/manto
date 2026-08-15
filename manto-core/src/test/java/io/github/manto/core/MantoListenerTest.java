package io.github.manto.core;

import org.junit.jupiter.api.Test;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class MantoListenerTest {

    private static class Handler {

        @MantoListener(topic = "order-events", groupId = "payment-service")
        void handleOrder() {
        }
    }

    private MantoListener listenerOn(String methodName) throws Exception {
        return Handler.class.getDeclaredMethod(methodName).getAnnotation(MantoListener.class);
    }

    @Test
    void isRetainedAtRuntime() {
        Retention retention = MantoListener.class.getAnnotation(Retention.class);

        assertNotNull(retention);
        assertEquals(RetentionPolicy.RUNTIME, retention.value());
    }

    @Test
    void targetsMethods() {
        Target target = MantoListener.class.getAnnotation(Target.class);

        assertNotNull(target);
        assertArrayEquals(new ElementType[]{ElementType.METHOD}, target.value());
    }

    @Test
    void isDocumented() {
        assertNotNull(MantoListener.class.getAnnotation(Documented.class));
    }

    @Test
    void exposesTopicAndGroupIdValues() throws Exception {
        MantoListener annotation = listenerOn("handleOrder");

        assertEquals("order-events", annotation.topic());
        assertEquals("payment-service", annotation.groupId());
    }
}