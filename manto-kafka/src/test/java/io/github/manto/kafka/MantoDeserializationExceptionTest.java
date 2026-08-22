package io.github.manto.kafka;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("MantoDeserializationException tests")
class MantoDeserializationExceptionTest {

    @Test
    @DisplayName("Creates exception with target type, payload preview, and cause")
    void createsExceptionWithAllFields() {
        String preview = "{\"invalid\": json}";
        IllegalArgumentException cause = new IllegalArgumentException("Bad JSON");
        MantoDeserializationException ex = new MantoDeserializationException(TestEvent.class, preview, cause);

        assertEquals(TestEvent.class, ex.getTargetType());
        assertEquals(preview, ex.getPayloadPreview());
        assertSame(cause, ex.getCause());
        assertTrue(ex.getMessage().contains("TestEvent"));
    }

    @Test
    @DisplayName("Message contains target type name")
    void messageContainsTargetType() {
        MantoDeserializationException ex = new MantoDeserializationException(
                TestEvent.class, "preview", new RuntimeException());

        assertTrue(ex.getMessage().contains("TestEvent"));
    }

    private record TestEvent(String id, long value) {}
}