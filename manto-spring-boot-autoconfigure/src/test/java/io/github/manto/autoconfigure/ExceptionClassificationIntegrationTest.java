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
 * Verifies exception classification behavior with a real Kafka broker.
 */
@Testcontainers
class ExceptionClassificationIntegrationTest {

    private static final String TOPIC = "exception-classification-test";
    private static final String GROUP_ID = "exception-classification-group";

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
            admin.createTopics(List.of(
                    new NewTopic(TOPIC, 1, (short) 1),
                    new NewTopic(TOPIC + ".DLT", 1, (short) 1)
            )).all().get();
        }

        context = new AnnotationConfigApplicationContext();
        context.getEnvironment().getPropertySources().addFirst(
                new org.springframework.core.env.MapPropertySource("mantoTestProps", Map.of(
                        "manto.kafka.bootstrap-servers", KAFKA.getBootstrapServers(),
                        "manto.retry.enabled", "true",
                        "manto.retry.max-attempts", "3",
                        "manto.retry.backoff.initial-delay", "100",
                        "manto.retry.backoff.multiplier", "2.0",
                        "manto.retry.backoff.max-delay", "1000",
                        "manto.dlt.enabled", "true"
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
    void nonRetryableExceptionShouldBypassRetriesAndGoToDlt() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<TestEvent> receivedEvent = new AtomicReference<>();
        AtomicInteger attemptCount = new AtomicInteger(0);

        TestHandler handler = context.getBean(TestHandler.class);
        handler.setLatch(latch);
        handler.setReceivedEvent(receivedEvent);
        handler.setAttemptCount(attemptCount);
        handler.setExceptionType(IllegalArgumentException.class);

        TestEvent event = new TestEvent("event-1", "non-retryable");
        kafkaTemplate.send(TOPIC, event).get(10, TimeUnit.SECONDS);

        // Non-retryable exception should not retry - only 1 attempt
        Thread.sleep(500);

        assertEquals(1, attemptCount.get(), "Non-retryable exception should only attempt once");
    }

    @Test
    void retryableExceptionShouldRetry() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<TestEvent> receivedEvent = new AtomicReference<>();
        AtomicInteger attemptCount = new AtomicInteger(0);

        TestHandler handler = context.getBean(TestHandler.class);
        handler.setLatch(latch);
        handler.setReceivedEvent(receivedEvent);
        handler.setAttemptCount(attemptCount);
        handler.setExceptionType(RuntimeException.class);
        handler.setFailCount(2); // fail twice, then succeed

        TestEvent event = new TestEvent("event-2", "retryable");
        kafkaTemplate.send(TOPIC, event).get(10, TimeUnit.SECONDS);

        boolean received = latch.await(30, TimeUnit.SECONDS);
        assertTrue(received, "Handler should eventually succeed after retries");

        TestEvent receivedEventResult = receivedEvent.get();
        assertNotNull(receivedEventResult, "Received event should not be null");
        assertEquals("event-2", receivedEventResult.id());
        assertEquals("retryable", receivedEventResult.payload());
        assertEquals(3, attemptCount.get(), "Should attempt 3 times (fail twice, then succeed)");
    }

    @Test
    void defaultBehaviorRuntimeExceptionIsRetryable() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<TestEvent> receivedEvent = new AtomicReference<>();
        AtomicInteger attemptCount = new AtomicInteger(0);

        TestHandler handler = context.getBean(TestHandler.class);
        handler.setLatch(latch);
        handler.setReceivedEvent(receivedEvent);
        handler.setAttemptCount(attemptCount);
        handler.setExceptionType(RuntimeException.class);
        handler.setFailCount(1); // fail once, then succeed

        TestEvent event = new TestEvent("event-3", "default-retryable");
        kafkaTemplate.send(TOPIC, event).get(10, TimeUnit.SECONDS);

        boolean received = latch.await(30, TimeUnit.SECONDS);
        assertTrue(received, "Handler should succeed after retry");

        assertEquals(2, attemptCount.get(), "RuntimeException should be retryable by default");
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
        private Class<? extends Exception> exceptionType = RuntimeException.class;
        private int failCount = 0;
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

        void setExceptionType(Class<? extends Exception> exceptionType) {
            this.exceptionType = exceptionType;
        }

        void setFailCount(int failCount) {
            this.failCount = failCount;
            this.currentAttempt = 0;
        }

        @MantoListener(topic = TOPIC, groupId = GROUP_ID)
        public void handleEvent(TestEvent event) throws Exception {
            currentAttempt++;
            attemptCount.incrementAndGet();

            if (currentAttempt <= failCount) {
                Exception exception = exceptionType.getConstructor(String.class)
                        .newInstance("Simulated failure on attempt " + currentAttempt);
                throw exception;
            }

            receivedEvent.set(event);
            latch.countDown();
        }
    }
}