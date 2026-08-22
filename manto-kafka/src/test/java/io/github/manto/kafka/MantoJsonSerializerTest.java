package io.github.manto.kafka;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("MantoJsonSerializer tests")
class MantoJsonSerializerTest {

    private final MantoJsonSerializer serializer = new MantoJsonSerializer();

    @Nested
    @DisplayName("Valid payload serialization")
    class ValidPayloads {

        @Test
        @DisplayName("Serializes simple POJO to JSON bytes")
        void serializesSimplePojo() {
            TestEvent event = new TestEvent("event-1", 42);
            byte[] result = serializer.serialize("test-topic", event);

            assertNotNull(result);
            String json = new String(result);
            assertTrue(json.contains("\"id\":\"event-1\""));
            assertTrue(json.contains("\"value\":42"));
        }

        @Test
        @DisplayName("Serializes record with Java time types")
        void serializesRecordWithJavaTime() {
            Instant now = Instant.parse("2024-01-15T10:30:00Z");
            EventWithTime event = new EventWithTime("evt-1", now);
            byte[] result = serializer.serialize("test-topic", event);

            assertNotNull(result);
            String json = new String(result);
            assertTrue(json.contains("\"id\":\"evt-1\""));
            assertTrue(json.contains("2024-01-15T10:30:00Z"));
        }

        @Test
        @DisplayName("Serializes nested objects")
        void serializesNestedObjects() {
            OuterEvent event = new OuterEvent("outer-1", new InnerEvent("inner-1", 100));
            byte[] result = serializer.serialize("test-topic", event);

            assertNotNull(result);
            String json = new String(result);
            assertTrue(json.contains("\"outerId\":\"outer-1\""));
            assertTrue(json.contains("\"innerId\":\"inner-1\""));
            assertTrue(json.contains("\"innerValue\":100"));
        }

        @Test
        @DisplayName("Serializes collections")
        void serializesCollections() {
            ListEvent event = new ListEvent("list-1", List.of("a", "b", "c"));
            byte[] result = serializer.serialize("test-topic", event);

            assertNotNull(result);
            String json = new String(result);
            assertTrue(json.contains("\"id\":\"list-1\""));
            assertTrue(json.contains("\"items\":[\"a\",\"b\",\"c\"]"));
        }

        @Test
        @DisplayName("Serializes map")
        void serializesMap() {
            MapEvent event = new MapEvent("map-1", Map.of("key1", "val1", "key2", "val2"));
            byte[] result = serializer.serialize("test-topic", event);

            assertNotNull(result);
            String json = new String(result);
            assertTrue(json.contains("\"id\":\"map-1\""));
            assertTrue(json.contains("\"key1\":\"val1\""));
            assertTrue(json.contains("\"key2\":\"val2\""));
        }

        @Test
        @DisplayName("Returns null for null input")
        void returnsNullForNullInput() {
            byte[] result = serializer.serialize("test-topic", null);
            assertNull(result);
        }
    }

    @Nested
    @DisplayName("Error handling")
    class ErrorHandling {

        @Test
        @DisplayName("Throws MantoSerializationException for non-serializable object")
        void throwsExceptionForNonSerializable() {
            NonSerializableEvent event = new NonSerializableEvent();
            MantoSerializationException ex = assertThrows(MantoSerializationException.class,
                    () -> serializer.serialize("test-topic", event));

            assertNotNull(ex.getCause());
            assertTrue(ex.getMessage().contains("NonSerializableEvent"));
        }
    }

    // Test event classes
    private record TestEvent(String id, long value) {}
    private record EventWithTime(String id, Instant timestamp) {}
    private record InnerEvent(String innerId, int innerValue) {}
    private record OuterEvent(String outerId, InnerEvent inner) {}
    private record ListEvent(String id, List<String> items) {}
    private record MapEvent(String id, Map<String, String> data) {}
    private static class NonSerializableEvent {
        private final Object nonSerializable = new Object() {
            @Override
            public String toString() {
                throw new UnsupportedOperationException("Cannot serialize");
            }
        };
    }
}