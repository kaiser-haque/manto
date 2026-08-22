package io.github.manto.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.manto.core.MantoHeaders;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Verifies {@link MantoKafkaProducer} end-to-end against a real Kafka broker
 * running in a Testcontainers container: publish through the framework
 * abstraction and read the produced record back from Kafka.
 */
@Testcontainers
class MantoKafkaProducerIntegrationTest {

    private static final String TOPIC = "order-events";

    @Container
    private static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));

    private final ObjectMapper objectMapper = new ObjectMapper();

    private record OrderCreatedEvent(String orderId, long amount) {
    }

    @BeforeAll
    static void createTopic() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
        }
    }

    @Test
    void publishesEventToKafka() throws Exception {
        KafkaTemplate<String, Object> kafkaTemplate = new KafkaTemplate<>(producerFactory());

        new MantoKafkaProducer(kafkaTemplate, "integration-test-source").publish(TOPIC, new OrderCreatedEvent("order-1", 42));

        ConsumerRecord<String, String> record = consumeRecord();
        JsonNode payload = objectMapper.readTree(record.value());
        assertEquals("order-1", payload.get("orderId").asText());
        assertEquals(42, payload.get("amount").asLong());
    }

    @Test
    void propagatesMantoHeadersToConsumer() throws Exception {
        KafkaTemplate<String, Object> kafkaTemplate = new KafkaTemplate<>(producerFactory());

        new MantoKafkaProducer(kafkaTemplate, "integration-test-source").publish(TOPIC, new OrderCreatedEvent("order-1", 42));

        ConsumerRecord<String, String> record = consumeRecord();

        String eventId = headerValue(record, MantoHeaders.EVENT_ID);
        String eventType = headerValue(record, MantoHeaders.EVENT_TYPE);
        String eventVersion = headerValue(record, MantoHeaders.EVENT_VERSION);
        String correlationId = headerValue(record, MantoHeaders.CORRELATION_ID);
        String source = headerValue(record, MantoHeaders.SOURCE);

        assertNotNull(eventId, "Manto-Event-Id header should be present");
        assertEquals("OrderCreatedEvent", eventType);
        assertEquals("1.0", eventVersion);
        assertEquals(eventId, correlationId);
        assertEquals("integration-test-source", source);
    }

    private DefaultKafkaProducerFactory<String, Object> producerFactory() {
        Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    private ConsumerRecord<String, String> consumeRecord() {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        Instant deadline = Instant.now().plus(Duration.ofSeconds(30));
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.assign(List.of(new TopicPartition(TOPIC, 0)));
            while (Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (!records.isEmpty()) {
                    return records.iterator().next();
                }
            }
        }
        fail("no record received on topic '" + TOPIC + "' within 30 seconds");
        return null;
    }

    private String headerValue(ConsumerRecord<?, ?> record, String headerName) {
        org.apache.kafka.common.header.Header header = record.headers().lastHeader(headerName);
        return header != null ? new String(header.value()) : null;
    }
}