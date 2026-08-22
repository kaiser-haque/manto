package io.github.manto.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * JSON serializer for Manto events using Jackson.
 *
 * <p>Serializes objects to UTF-8 encoded JSON bytes. Uses a shared,
 * configured {@link ObjectMapper} with JavaTimeModule for proper
 * {@code java.time} type serialization.</p>
 */
public class MantoJsonSerializer implements Serializer<Object> {

    private static final ObjectMapper OBJECT_MAPPER = createObjectMapper();

    private static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        if (data == null) {
            return null;
        }
        try {
            return OBJECT_MAPPER.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new MantoSerializationException("Failed to serialize object of type " + data.getClass().getName(), e);
        }
    }

    @Override
    public void close() {
    }

    /**
     * Returns the shared {@link ObjectMapper} instance used for serialization.
     * Exposed for testing and advanced configuration.
     *
     * @return the shared ObjectMapper
     */
    public static ObjectMapper getObjectMapper() {
        return OBJECT_MAPPER;
    }
}