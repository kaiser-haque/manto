package io.github.manto.kafka;

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
import org.junit.jupiter.api.AfterEach;
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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Verifies correlation ID propagation end-to-end: Producer sets header,
 * consumer reads header and makes it available in application context.
 */
@Testcontainers
class CorrelationIdPropagationIntegrationTest {

    @Container
    private static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));

    private final AtomicInteger topicCounter = new AtomicInteger(0);

    private record OrderCreatedEvent(String orderId, long amount) {
    }

    @AfterEach
    void cleanup() {
        CorrelationIdContext.clear();
    }

    @Test
    void propagatesExplicitCorrelationIdFromProducerToConsumer() throws Exception {
        String topic = "corr-explicit-" + topicCounter.incrementAndGet();
        createTopic(topic);
        String expectedCorrelationId = "upstream-service-abc-123";
        KafkaTemplate<String, Object> kafkaTemplate = new KafkaTemplate<>(producerFactory());

        new MantoKafkaProducer(kafkaTemplate, "integration-test")
                .publish(topic, new OrderCreatedEvent("order-1", 42), expectedCorrelationId);

        ConsumerRecord<String, String> record = consumeRecord(topic);
        String correlationId = headerValue(record, MantoHeaders.CORRELATION_ID);
        String eventId = headerValue(record, MantoHeaders.EVENT_ID);

        assertNotNull(correlationId, "Manto-Correlation-Id header should be present");
        assertEquals(expectedCorrelationId, correlationId);
        assertNotNull(eventId, "Manto-Event-Id header should be present");
    }

    @Test
    void generatesCorrelationIdEqualToEventIdWhenNotExplicit() throws Exception {
        String topic = "corr-default-" + topicCounter.incrementAndGet();
        createTopic(topic);
        KafkaTemplate<String, Object> kafkaTemplate = new KafkaTemplate<>(producerFactory());

        new MantoKafkaProducer(kafkaTemplate, "integration-test")
                .publish(topic, new OrderCreatedEvent("order-2", 99));

        ConsumerRecord<String, String> record = consumeRecord(topic);
        String correlationId = headerValue(record, MantoHeaders.CORRELATION_ID);
        String eventId = headerValue(record, MantoHeaders.EVENT_ID);

        assertNotNull(correlationId, "Manto-Correlation-Id header should be present");
        assertNotNull(eventId, "Manto-Event-Id header should be present");
        assertEquals(eventId, correlationId, "Default correlation ID should equal event ID");
    }

    @Test
    void interceptorSetsCorrelationIdInContext() throws Exception {
        String topic = "corr-context-" + topicCounter.incrementAndGet();
        createTopic(topic);
        String expectedCorrelationId = "context-test-456";
        KafkaTemplate<String, Object> kafkaTemplate = new KafkaTemplate<>(producerFactory());

        new MantoKafkaProducer(kafkaTemplate, "integration-test")
                .publish(topic, new OrderCreatedEvent("order-3", 77), expectedCorrelationId);

        ConsumerRecord<String, String> record = consumeRecord(topic);

        MantoListenerInterceptor interceptor = new MantoListenerInterceptor(null);
        org.apache.kafka.clients.consumer.Consumer<String, Object> consumer =
                org.mockito.Mockito.mock(org.apache.kafka.clients.consumer.Consumer.class);

        org.apache.kafka.common.header.internals.RecordHeaders headers =
                new org.apache.kafka.common.header.internals.RecordHeaders();
        for (org.apache.kafka.common.header.Header h : record.headers()) {
            headers.add(h);
        }
        ConsumerRecord<String, Object> typedRecord = new ConsumerRecord<>(
                record.topic(), record.partition(), record.offset(),
                record.timestamp(), org.apache.kafka.common.record.TimestampType.NO_TIMESTAMP_TYPE,
                (long) 0, 0, 0, null, record.value(), headers, java.util.Optional.empty());

        interceptor.intercept(typedRecord, consumer);
        assertEquals(expectedCorrelationId, CorrelationIdContext.get());

        interceptor.recordProcessingDuration(topic);
        assertNull(CorrelationIdContext.get());
    }

    private void createTopic(String topic) throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        }
    }

    private DefaultKafkaProducerFactory<String, Object> producerFactory() {
        Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    private ConsumerRecord<String, String> consumeRecord(String topic) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        Instant deadline = Instant.now().plus(Duration.ofSeconds(30));
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.assign(List.of(new TopicPartition(topic, 0)));
            while (Instant.now().isBefore(deadline)) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (!records.isEmpty()) {
                    return records.iterator().next();
                }
            }
        }
        fail("no record received on topic '" + topic + "' within 30 seconds");
        return null;
    }

    private String headerValue(ConsumerRecord<?, ?> record, String headerName) {
        org.apache.kafka.common.header.Header header = record.headers().lastHeader(headerName);
        return header != null ? new String(header.value()) : null;
    }
}
