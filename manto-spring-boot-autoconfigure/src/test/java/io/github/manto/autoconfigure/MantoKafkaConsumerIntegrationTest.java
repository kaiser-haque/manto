package io.github.manto.autoconfigure;

import io.github.manto.core.MantoListener;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies {@link MantoListenerRegistrar} end-to-end against a real Kafka broker
 * running in a Testcontainers container: produce a JSON message to Kafka and
 * verify the {@code @MantoListener} handler is invoked with the correctly
 * deserialized event object.
 */
@Testcontainers
class MantoKafkaConsumerIntegrationTest {

    private static final String TOPIC = "order-events";
    private static final String GROUP_ID = "payment-service";

    @Container
    private static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));

    private static AnnotationConfigApplicationContext context;
    private static KafkaTemplate<String, Object> kafkaTemplate;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private record OrderCreatedEvent(String orderId, long amount) {
    }

    @BeforeAll
    static void setup() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
        }

        context = new AnnotationConfigApplicationContext();
        context.getEnvironment().getPropertySources().addFirst(
                new org.springframework.core.env.MapPropertySource("mantoTestProps", Map.of("manto.kafka.bootstrap-servers", KAFKA.getBootstrapServers())));
        context.register(TestConfig.class);
        context.refresh();
        context.start();

        kafkaTemplate = context.getBean(KafkaTemplate.class);
    }

    @AfterAll
    static void teardown() {
        if (context != null) {
            context.close();
        }
    }

    @Test
    void invokesMantoListenerWithDeserializedEvent() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<OrderCreatedEvent> receivedEvent = new AtomicReference<>();

        TestHandler handler = context.getBean(TestHandler.class);
        handler.setLatch(latch);
        handler.setReceivedEvent(receivedEvent);

        OrderCreatedEvent event = new OrderCreatedEvent("order-1", 42);
        kafkaTemplate.send(TOPIC, event).get(10, TimeUnit.SECONDS);

        boolean received = latch.await(30, TimeUnit.SECONDS);
        assertTrue(received, "Handler was not invoked within 30 seconds");

        OrderCreatedEvent receivedEventResult = receivedEvent.get();
        assertNotNull(receivedEventResult, "Received event should not be null");
        assertEquals("order-1", receivedEventResult.orderId());
        assertEquals(42, receivedEventResult.amount());
    }

    @Configuration
    @EnableKafka
    @Import(MantoAutoConfiguration.class)
    static class TestConfig {

        @Bean
        TestHandler testHandler() {
            return new TestHandler();
        }
    }

    static class TestHandler {

        private CountDownLatch latch;
        private AtomicReference<OrderCreatedEvent> receivedEvent;

        void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }

        void setReceivedEvent(AtomicReference<OrderCreatedEvent> receivedEvent) {
            this.receivedEvent = receivedEvent;
        }

        @MantoListener(topic = TOPIC, groupId = GROUP_ID)
        public void handleOrder(OrderCreatedEvent event) {
            receivedEvent.set(event);
            latch.countDown();
        }
    }
}