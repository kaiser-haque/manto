package io.github.manto.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * JSON deserializer for Manto events using Jackson with typed deserialization.
 *
 * <p>Deserializes UTF-8 encoded JSON bytes to a specific target type.
 * The target type must be configured via {@link #configure(Map, boolean)} using
 * the {@code manto.deserializer.target.type} property (fully qualified class name),
 * or by constructing the deserializer with a specific {@link Class} or
 * {@link JavaType}.</p>
 *
 * <p>Uses a shared, configured {@link ObjectMapper} with JavaTimeModule for
 * proper {@code java.time} type deserialization.</p>
 */
public class MantoJsonDeserializer implements Deserializer<Object> {

    private static final ObjectMapper OBJECT_MAPPER = createObjectMapper();
    private static final String TARGET_TYPE_CONFIG = "manto.deserializer.target.type";

    private static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }

    private Class<?> targetClass;
    private JavaType targetType;

    /**
     * Default constructor for Kafka's deserializer instantiation.
     * Target type must be configured via {@link #configure(Map, boolean)}.
     */
    public MantoJsonDeserializer() {
    }

    /**
     * Creates a deserializer for the specified target class.
     *
     * @param targetClass the class to deserialize to, not null
     */
    public MantoJsonDeserializer(Class<?> targetClass) {
        this.targetClass = targetClass;
        this.targetType = OBJECT_MAPPER.getTypeFactory().constructType(targetClass);
    }

    /**
     * Creates a deserializer for the specified Java type (supports generics).
     *
     * @param targetType the Java type to deserialize to, not null
     */
    public MantoJsonDeserializer(JavaType targetType) {
        this.targetType = targetType;
        this.targetClass = targetType.getRawClass();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (configs != null && targetType == null) {
            String className = (String) configs.get(TARGET_TYPE_CONFIG);
            if (className != null && !className.isBlank()) {
                try {
                    this.targetClass = Class.forName(className);
                    this.targetType = OBJECT_MAPPER.getTypeFactory().constructType(this.targetClass);
                } catch (ClassNotFoundException e) {
                    throw new IllegalArgumentException("Target type class not found: " + className, e);
                }
            }
        }
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        if (targetType == null) {
            throw new MantoDeserializationException(
                    Object.class,
                    previewPayload(data),
                    new IllegalStateException("Target type not configured for deserialization. " +
                            "Set 'manto.deserializer.target.type' config or use constructor with target type."));
        }
        try {
            return OBJECT_MAPPER.readValue(data, targetType);
        } catch (JsonProcessingException e) {
            throw new MantoDeserializationException(targetClass, previewPayload(data), e);
        } catch (IOException e) {
            throw new MantoDeserializationException(targetClass, previewPayload(data), e);
        }
    }

    @Override
    public void close() {
    }

    private String previewPayload(byte[] data) {
        if (data == null) {
            return "null";
        }
        String str = new String(data, StandardCharsets.UTF_8);
        if (str.length() > 200) {
            return str.substring(0, 200) + "... (truncated, total " + str.length() + " chars)";
        }
        return str;
    }

    /**
     * Returns the shared {@link ObjectMapper} instance used for deserialization.
     * Exposed for testing and advanced configuration.
     *
     * @return the shared ObjectMapper
     */
    public static ObjectMapper getObjectMapper() {
        return OBJECT_MAPPER;
    }

    /**
     * Returns the configured target class for deserialization.
     *
     * @return the target class, or null if not configured
     */
    public Class<?> getTargetClass() {
        return targetClass;
    }

    /**
     * Returns the configured target Java type for deserialization.
     *
     * @return the target type, or null if not configured
     */
    public JavaType getTargetType() {
        return targetType;
    }
}