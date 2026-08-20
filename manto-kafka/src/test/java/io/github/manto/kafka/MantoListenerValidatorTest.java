package io.github.manto.kafka;

import io.github.manto.core.MantoListener;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MantoListenerValidatorTest {

    private final MantoListenerValidator validator = new MantoListenerValidator();

    private Method method(String name) throws NoSuchMethodException {
        if (name.equals("noParameters")) {
            return Handlers.class.getDeclaredMethod(name);
        }
        if (name.equals("nonPublic")) {
            return Handlers.class.getDeclaredMethod(name, String.class);
        }
        if (name.equals("twoParameters")) {
            return Handlers.class.getDeclaredMethod(name, String.class, String.class);
        }
        return Handlers.class.getMethod(name, String.class);
    }

    @Test
    void acceptsValidHandler() throws Exception {
        assertDoesNotThrow(() -> validator.validate(method("valid"), "order-events", "payment-service"));
    }

    @Test
    void rejectsBlankTopic() throws Exception {
        MantoListenerConfigurationException e = assertThrows(MantoListenerConfigurationException.class,
                () -> validator.validate(method("valid"), "  ", "payment-service"));
        assertTrue(e.getMessage().contains("topic"));
        assertTrue(e.getMessage().contains("Handlers#valid"));
    }

    @Test
    void rejectsBlankGroupId() throws Exception {
        MantoListenerConfigurationException e = assertThrows(MantoListenerConfigurationException.class,
                () -> validator.validate(method("valid"), "order-events", ""));
        assertTrue(e.getMessage().contains("groupId"));
    }

    @Test
    void rejectsNonPublicMethod() throws Exception {
        MantoListenerConfigurationException e = assertThrows(MantoListenerConfigurationException.class,
                () -> validator.validate(method("nonPublic"), "order-events", "payment-service"));
        assertTrue(e.getMessage().contains("public"));
    }

    @Test
    void rejectsStaticMethod() throws Exception {
        MantoListenerConfigurationException e = assertThrows(MantoListenerConfigurationException.class,
                () -> validator.validate(method("staticMethod"), "order-events", "payment-service"));
        assertTrue(e.getMessage().contains("static"));
    }

    @Test
    void rejectsMethodWithoutParameters() throws Exception {
        MantoListenerConfigurationException e = assertThrows(MantoListenerConfigurationException.class,
                () -> validator.validate(method("noParameters"), "order-events", "payment-service"));
        assertTrue(e.getMessage().contains("exactly one parameter"));
    }

    @Test
    void rejectsMethodWithMultipleParameters() throws Exception {
        MantoListenerConfigurationException e = assertThrows(MantoListenerConfigurationException.class,
                () -> validator.validate(method("twoParameters"), "order-events", "payment-service"));
        assertTrue(e.getMessage().contains("exactly one parameter"));
    }

    static class Handlers {

        @MantoListener(topic = "order-events", groupId = "payment-service")
        public void valid(String event) {
        }

        @MantoListener(topic = "order-events", groupId = "payment-service")
        private void nonPublic(String event) {
        }

        @MantoListener(topic = "order-events", groupId = "payment-service")
        public static void staticMethod(String event) {
        }

        @MantoListener(topic = "order-events", groupId = "payment-service")
        public void noParameters() {
        }

        @MantoListener(topic = "order-events", groupId = "payment-service")
        public void twoParameters(String event, String other) {
        }
    }
}