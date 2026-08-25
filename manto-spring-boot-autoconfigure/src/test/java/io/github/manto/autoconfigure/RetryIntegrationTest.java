package io.github.manto.autoconfigure;

import io.github.manto.core.MantoListener;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies retry behavior with a real Kafka broker.
 */
@Testcontainers
class RetryIntegrationTest {

    private static final String TOPIC = "retry-test-events";
    private static final String GROUP_ID = "retry-test-group";

    @Container
    private static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));

    private static AnnotationConfigApplicationContext context;
    private static KafkaTemplate<String, Object> kafkaTemplate;

    private record TestEvent(String id, String payload) {
    }

    @BeforeAll
    static void setup() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
        }

        context = new AnnotationConfigApplicationContext();
        context.getEnvironment().getPropertySources().addFirst(
                new org.springframework.core.env.MapPropertySource("mantoTestProps", Map.of(
                        "manto.kafka.bootstrap-servers", KAFKA.getBootstrapServers(),
                        "manto.retry.enabled", "true",
                        "manto.retry.max-attempts", "3",
                        "manto.retry.backoff.initial-delay", "100",
                        "manto.retry.backoff.multiplier", "2.0",
                        "manto.retry.backoff.max-delay", "1000"
                )));
        context.register(TestConfig.class);
        context.refresh();
        context.start();

        kafkaTemplate = context.getBean("mantoKafkaTemplate", KafkaTemplate.class);
    }

    @AfterAll
    static void teardown() {
        if (context != null) {
            context.close();
        }
    }

    @Test
    void successOnFirstAttempt() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<TestEvent> receivedEvent = new AtomicReference<>();
        AtomicInteger attemptCount = new AtomicInteger(0);

        TestHandler handler = context.getBean(TestHandler.class);
        handler.setLatch(latch);
        handler.setReceivedEvent(receivedEvent);
        handler.setAttemptCount(attemptCount);
        handler.setFailCount(0); // no failures

        TestEvent event = new TestEvent("event-1", "success");
        kafkaTemplate.send(TOPIC, event).get(10, TimeUnit.SECONDS);

        boolean received = latch.await(30, TimeUnit.SECONDS);
        assertTrue(received, "Handler was not invoked within 30 seconds");

        TestEvent receivedEventResult = receivedEvent.get();
        assertNotNull(receivedEventResult, "Received event should not be null");
        assertEquals("event-1", receivedEventResult.id());
        assertEquals("success", receivedEventResult.payload());
        assertEquals(1, attemptCount.get(), "Should only attempt once on success");
    }

    @Test
    void failureFollowedBySuccess() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<TestEvent> receivedEvent = new AtomicReference<>();
        AtomicInteger attemptCount = new AtomicInteger(0);

        TestHandler handler = context.getBean(TestHandler.class);
        handler.setLatch(latch);
        handler.setReceivedEvent(receivedEvent);
        handler.setAttemptCount(attemptCount);
        handler.setFailCount(1); // fail once, then succeed

        TestEvent event = new TestEvent("event-2", "retry-success");
        kafkaTemplate.send(TOPIC, event).get(10, TimeUnit.SECONDS);

        boolean received = latch.await(30, TimeUnit.SECONDS);
        assertTrue(received, "Handler was not invoked within 30 seconds");

        TestEvent receivedEventResult = receivedEvent.get();
        assertNotNull(receivedEventResult, "Received event should not be null");
        assertEquals("event-2", receivedEventResult.id());
        assertEquals("retry-success", receivedEventResult.payload());
        assertEquals(2, attemptCount.get(), "Should attempt twice (fail once, then succeed)");
    }

    @Test
    void failureOnAllAttempts() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<TestEvent> receivedEvent = new AtomicReference<>();
        AtomicInteger attemptCount = new AtomicInteger(0);

        TestHandler handler = context.getBean(TestHandler.class);
        handler.setLatch(latch);
        handler.setReceivedEvent(receivedEvent);
        handler.setAttemptCount(attemptCount);
        handler.setFailCount(3); // fail all 3 attempts

        TestEvent event = new TestEvent("event-3", "fail-all");
        kafkaTemplate.send(TOPIC, event).get(10, TimeUnit.SECONDS);

        // Wait for all retries to be exhausted - the latch will only count down on final failure
        // Since we don't have a DLT recoverer configured, the message will be retried and then...
        // Actually with DefaultErrorHandler without recoverer, it will log and stop after max attempts
        // We need to wait for the retries to complete
        Thread.sleep(2000); // wait for retries (100ms + 200ms + 400ms backoff)

        assertEquals(3, attemptCount.get(), "Should attempt 3 times (max attempts)");
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
        private AtomicReference<TestEvent> receivedEvent;
        private AtomicInteger attemptCount;
        private int failCount;
        private int currentAttempt = 0;

        void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }

        void setReceivedEvent(AtomicReference<TestEvent> receivedEvent) {
            this.receivedEvent = receivedEvent;
        }

        void setAttemptCount(AtomicInteger attemptCount) {
            this.attemptCount = attemptCount;
        }

        void setFailCount(int failCount) {
            this.failCount = failCount;
            this.currentAttempt = 0;
        }

        @MantoListener(topic = TOPIC, groupId = GROUP_ID)
        public void handleEvent(TestEvent event) {
            currentAttempt++;
            attemptCount.incrementAndGet();

            if (currentAttempt <= failCount) {
                throw new RuntimeException("Simulated failure on attempt " + currentAttempt);
            }

            receivedEvent.set(event);
            latch.countDown();
        }
    }
}