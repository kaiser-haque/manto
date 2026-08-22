package io.github.manto.kafka;

import com.fasterxml.jackson.databind.JavaType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.BeforeEach;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("MantoJsonDeserializer tests")
class MantoJsonDeserializerTest {

    private MantoJsonDeserializer deserializer;

    @Nested
    @DisplayName("Valid payload deserialization")
    class ValidPayloads {

        @Test
        @DisplayName("Deserializes JSON to simple POJO using constructor")
        void deserializesToPojoWithConstructor() {
            deserializer = new MantoJsonDeserializer(TestEvent.class);
            String json = "{\"id\":\"event-1\",\"value\":42}";
            byte[] data = json.getBytes();

            TestEvent result = (TestEvent) deserializer.deserialize("test-topic", data);

            assertNotNull(result);
            assertEquals("event-1", result.id());
            assertEquals(42, result.value());
        }

        @Test
        @DisplayName("Deserializes JSON to record with Java time types")
        void deserializesRecordWithJavaTime() {
            deserializer = new MantoJsonDeserializer(EventWithTime.class);
            String json = "{\"id\":\"evt-1\",\"timestamp\":\"2024-01-15T10:30:00Z\"}";
            byte[] data = json.getBytes();

            EventWithTime result = (EventWithTime) deserializer.deserialize("test-topic", data);

            assertNotNull(result);
            assertEquals("evt-1", result.id());
            assertEquals(Instant.parse("2024-01-15T10:30:00Z"), result.timestamp());
        }

        @Test
        @DisplayName("Deserializes JSON to nested objects")
        void deserializesNestedObjects() {
            deserializer = new MantoJsonDeserializer(OuterEvent.class);
            String json = "{\"outerId\":\"outer-1\",\"inner\":{\"innerId\":\"inner-1\",\"innerValue\":100}}";
            byte[] data = json.getBytes();

            OuterEvent result = (OuterEvent) deserializer.deserialize("test-topic", data);

            assertNotNull(result);
            assertEquals("outer-1", result.outerId());
            assertNotNull(result.inner());
            assertEquals("inner-1", result.inner().innerId());
            assertEquals(100, result.inner().innerValue());
        }

        @Test
        @DisplayName("Deserializes JSON to collection")
        void deserializesCollection() {
            JavaType type = MantoJsonDeserializer.getObjectMapper().getTypeFactory()
                    .constructParametricType(ListEvent.class, String.class);
            deserializer = new MantoJsonDeserializer(type);
            String json = "{\"id\":\"list-1\",\"items\":[\"a\",\"b\",\"c\"]}";
            byte[] data = json.getBytes();

            ListEvent result = (ListEvent) deserializer.deserialize("test-topic", data);

            assertNotNull(result);
            assertEquals("list-1", result.id());
            assertEquals(List.of("a", "b", "c"), result.items());
        }

        @Test
        @DisplayName("Deserializes JSON to map")
        void deserializesMap() {
            JavaType type = MantoJsonDeserializer.getObjectMapper().getTypeFactory()
                    .constructParametricType(MapEvent.class, String.class, String.class);
            deserializer = new MantoJsonDeserializer(type);
            String json = "{\"id\":\"map-1\",\"data\":{\"key1\":\"val1\",\"key2\":\"val2\"}}";
            byte[] data = json.getBytes();

            MapEvent result = (MapEvent) deserializer.deserialize("test-topic", data);

            assertNotNull(result);
            assertEquals("map-1", result.id());
            assertEquals(Map.of("key1", "val1", "key2", "val2"), result.data());
        }

        @Test
        @DisplayName("Returns null for null input")
        void returnsNullForNullInput() {
            deserializer = new MantoJsonDeserializer(TestEvent.class);
            Object result = deserializer.deserialize("test-topic", null);
            assertNull(result);
        }

        @Test
        @DisplayName("Returns null for empty byte array")
        void returnsNullForEmptyArray() {
            deserializer = new MantoJsonDeserializer(TestEvent.class);
            Object result = deserializer.deserialize("test-topic", new byte[0]);
            assertNull(result);
        }

        @Test
        @DisplayName("Deserializes using configure with target type property")
        void deserializesUsingConfigure() {
            deserializer = new MantoJsonDeserializer();
            deserializer.configure(Map.of("manto.deserializer.target.type",
                    "io.github.manto.kafka.MantoJsonDeserializerTest$TestEvent"), false);

            String json = "{\"id\":\"event-1\",\"value\":42}";
            byte[] data = json.getBytes();

            TestEvent result = (TestEvent) deserializer.deserialize("test-topic", data);

            assertNotNull(result);
            assertEquals("event-1", result.id());
            assertEquals(42, result.value());
        }
    }

    @Nested
    @DisplayName("Error handling")
    class ErrorHandling {

        @BeforeEach
        void setup() {
            deserializer = new MantoJsonDeserializer(TestEvent.class);
        }

        @Test
        @DisplayName("Throws MantoDeserializationException for invalid JSON")
        void throwsExceptionForInvalidJson() {
            byte[] data = "{invalid json}".getBytes();

            MantoDeserializationException ex = assertThrows(MantoDeserializationException.class,
                    () -> deserializer.deserialize("test-topic", data));

            assertEquals(TestEvent.class, ex.getTargetType());
            assertNotNull(ex.getPayloadPreview());
            assertNotNull(ex.getCause());
        }

        @Test
        @DisplayName("Throws MantoDeserializationException for mismatched fields")
        void throwsExceptionForMismatchedFields() {
            String json = "{\"wrongField\":\"value\"}";
            byte[] data = json.getBytes();

            MantoDeserializationException ex = assertThrows(MantoDeserializationException.class,
                    () -> deserializer.deserialize("test-topic", data));

            assertEquals(TestEvent.class, ex.getTargetType());
            assertTrue(ex.getPayloadPreview().contains("wrongField"));
        }

        @Test
        @DisplayName("Throws MantoDeserializationException for wrong type")
        void throwsExceptionForWrongType() {
            String json = "\"just a string\"";
            byte[] data = json.getBytes();

            MantoDeserializationException ex = assertThrows(MantoDeserializationException.class,
                    () -> deserializer.deserialize("test-topic", data));

            assertEquals(TestEvent.class, ex.getTargetType());
        }

        @Test
        @DisplayName("Throws MantoDeserializationException when target type not configured")
        void throwsExceptionWhenTargetTypeNotConfigured() {
            deserializer = new MantoJsonDeserializer();
            byte[] data = "{}".getBytes();

            MantoDeserializationException ex = assertThrows(MantoDeserializationException.class,
                    () -> deserializer.deserialize("test-topic", data));

            assertEquals(Object.class, ex.getTargetType());
            assertNotNull(ex.getCause());
            assertTrue(ex.getCause().getMessage().contains("Target type not configured"));
        }

        @Test
        @DisplayName("Throws exception when configure target class not found")
        void throwsExceptionWhenConfigureClassNotFound() {
            deserializer = new MantoJsonDeserializer();

            IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                    () -> deserializer.configure(Map.of("manto.deserializer.target.type",
                            "non.existent.Class"), false));

            assertTrue(ex.getMessage().contains("Target type class not found"));
        }

        @Test
        @DisplayName("Payload preview is truncated for large payloads")
        void payloadPreviewIsTruncated() {
            String largeJson = "{\"id\":\"x\".repeat(500)}".replace(".repeat(500)", "x".repeat(500));
            byte[] data = largeJson.getBytes();

            MantoDeserializationException ex = assertThrows(MantoDeserializationException.class,
                    () -> deserializer.deserialize("test-topic", data));

            String preview = ex.getPayloadPreview();
            assertTrue(preview.length() <= 250); // 200 + truncation message
            assertTrue(preview.contains("truncated"));
        }
    }

    // Test event classes
    private record TestEvent(String id, long value) {}
    private record EventWithTime(String id, Instant timestamp) {}
    private record InnerEvent(String innerId, int innerValue) {}
    private record OuterEvent(String outerId, InnerEvent inner) {}
    private record ListEvent<T>(String id, List<T> items) {}
    private record MapEvent<K, V>(String id, Map<K, V> data) {}
}