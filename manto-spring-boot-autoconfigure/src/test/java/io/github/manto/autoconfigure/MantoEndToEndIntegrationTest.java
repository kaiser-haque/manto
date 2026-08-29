package io.github.manto.autoconfigure;

import io.github.manto.core.IdempotencyStore;
import io.github.manto.core.MantoHeaders;
import io.github.manto.core.MantoListener;
import io.github.manto.core.MantoProducer;
import io.github.manto.kafka.CorrelationIdContext;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.MapPropertySource;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Comprehensive end-to-end integration tests for Manto Kafka framework.
 * Verifies all major features against a real Kafka broker using Testcontainers:
 * publish/consume, JSON serialization, metadata propagation, retry, exponential
 * backoff, non-retryable failure, DLT, DLT metadata, idempotency, and correlation ID.
 */
@Testcontainers
class MantoEndToEndIntegrationTest {

    private static final String TOPIC_BASIC = "e2e-basic";
    private static final String TOPIC_JSON = "e2e-json";
    private static final String TOPIC_METADATA = "e2e-metadata";
    private static final String TOPIC_RETRY_SUCCESS = "e2e-retry-success";
    private static final String TOPIC_RETRY_EXHAUSTED = "e2e-retry-exhausted";
    private static final String TOPIC_RETRY_TIMING = "e2e-retry-timing";
    private static final String TOPIC_NON_RETRYABLE = "e2e-non-retryable";
    private static final String TOPIC_DLT = "e2e-dlt";
    private static final String TOPIC_IDEMPOTENCY = "e2e-idempotency";
    private static final String TOPIC_CORRELATION = "e2e-correlation";

    private static final String GROUP_PREFIX = "e2e-group";

    @Container
    private static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));

    private static AnnotationConfigApplicationContext context;
    private static KafkaTemplate<String, Object> kafkaTemplate;
    private static MantoProducer mantoProducer;

    @AfterEach
    void cleanup() {
        CorrelationIdContext.clear();
    }

    @BeforeAll
    static void setup() throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic(TOPIC_BASIC, 1, (short) 1),
                    new NewTopic(TOPIC_JSON, 1, (short) 1),
                    new NewTopic(TOPIC_METADATA, 1, (short) 1),
                    new NewTopic(TOPIC_RETRY_SUCCESS, 1, (short) 1),
                    new NewTopic(TOPIC_RETRY_EXHAUSTED, 1, (short) 1),
                    new NewTopic(TOPIC_RETRY_TIMING, 1, (short) 1),
                    new NewTopic(TOPIC_NON_RETRYABLE, 1, (short) 1),
                    new NewTopic(TOPIC_DLT, 1, (short) 1),
                    new NewTopic(TOPIC_IDEMPOTENCY, 1, (short) 1),
                    new NewTopic(TOPIC_CORRELATION, 1, (short) 1)
            )).all().get();
        }

        context = new AnnotationConfigApplicationContext();
        context.getEnvironment().getPropertySources().addFirst(
                new MapPropertySource("mantoTestProps", Map.of(
                        "manto.kafka.bootstrap-servers", KAFKA.getBootstrapServers(),
                        "manto.retry.enabled", "true",
                        "manto.retry.max-attempts", "3",
                        "manto.retry.backoff.initial-delay", "100",
                        "manto.retry.backoff.multiplier", "2.0",
                        "manto.retry.backoff.max-delay", "1000",
                        "manto.dlt.enabled", "true",
                        "manto.dlt.topic-suffix", ".DLT",
                        "manto.idempotency.enabled", "true"
                )));
        context.register(TestConfig.class);
        context.refresh();
        context.start();

        kafkaTemplate = context.getBean("mantoKafkaTemplate", KafkaTemplate.class);
        mantoProducer = context.getBean(MantoProducer.class);
    }

    @AfterAll
    static void teardown() {
        if (context != null) {
            context.close();
        }
    }

    // ── 1. Successful publish/consume ──────────────────────────────────────

    @Test
    void successfulPublishConsume() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<BasicEvent> received = new AtomicReference<>();

        BasicHandler handler = context.getBean(BasicHandler.class);
        handler.reset(latch, received);

        kafkaTemplate.send(TOPIC_BASIC, new BasicEvent("evt-1", "hello")).get(10, TimeUnit.SECONDS);

        assertTrue(latch.await(30, TimeUnit.SECONDS), "Handler not invoked within 30s");
        assertEquals("evt-1", received.get().id());
        assertEquals("hello", received.get().data());
    }

    // ── 2. JSON serialization ──────────────────────────────────────────────

    @Test
    void jsonSerialization() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<JsonEvent> received = new AtomicReference<>();

        JsonHandler handler = context.getBean(JsonHandler.class);
        handler.reset(latch, received);

        kafkaTemplate.send(TOPIC_JSON, new JsonEvent("order-99", 999, true)).get(10, TimeUnit.SECONDS);

        assertTrue(latch.await(30, TimeUnit.SECONDS), "Handler not invoked within 30s");
        JsonEvent event = received.get();
        assertEquals("order-99", event.orderId());
        assertEquals(999, event.amount());
        assertTrue(event.priority());
    }

    // ── 3. Metadata propagation ────────────────────────────────────────────

    @Test
    void metadataPropagation() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<BasicEvent> received = new AtomicReference<>();

        MetadataHandler handler = context.getBean(MetadataHandler.class);
        handler.reset(latch, received);

        mantoProducer.publish(TOPIC_METADATA, new BasicEvent("meta-1", "test"));

        assertTrue(latch.await(30, TimeUnit.SECONDS), "Handler not invoked within 30s");

        ConsumerRecord<String, String> record = consumeRaw(TOPIC_METADATA);
        assertNotNull(headerValue(record, MantoHeaders.EVENT_ID), "Event ID header missing");
        assertNotNull(headerValue(record, MantoHeaders.EVENT_TYPE), "Event type header missing");
        assertNotNull(headerValue(record, MantoHeaders.EVENT_VERSION), "Event version header missing");
        assertNotNull(headerValue(record, MantoHeaders.CORRELATION_ID), "Correlation ID header missing");
        assertNotNull(headerValue(record, MantoHeaders.SOURCE), "Source header missing");
        assertEquals("BasicEvent", headerValue(record, MantoHeaders.EVENT_TYPE));
        assertEquals("1.0", headerValue(record, MantoHeaders.EVENT_VERSION));
        assertEquals("manto", headerValue(record, MantoHeaders.SOURCE));
    }

    // ── 4. Retry (failure then success) ────────────────────────────────────

    @Test
    void retryFailureFollowedBySuccess() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<BasicEvent> received = new AtomicReference<>();
        AtomicInteger attempts = new AtomicInteger(0);

        RetryHandler handler = context.getBean(RetryHandler.class);
        handler.reset(latch, received, attempts, 1);

        kafkaTemplate.send(TOPIC_RETRY_SUCCESS, new BasicEvent("retry-1", "data")).get(10, TimeUnit.SECONDS);

        assertTrue(latch.await(30, TimeUnit.SECONDS), "Handler not invoked within 30s");
        assertEquals("retry-1", received.get().id());
        assertEquals(2, attempts.get(), "Should attempt twice (fail once, succeed on 2nd)");
    }

    // ── 5. Exponential backoff behavior ────────────────────────────────────

    @Test
    void exponentialBackoffTiming() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<BasicEvent> received = new AtomicReference<>();
        AtomicInteger attempts = new AtomicInteger(0);
        AtomicReference<long[]> timestamps = new AtomicReference<>(new long[3]);

        RetryHandler handler = context.getBean(RetryHandler.class);
        handler.reset(latch, received, attempts, 2);
        handler.setTimestamps(timestamps.get());

        long start = System.currentTimeMillis();
        kafkaTemplate.send(TOPIC_RETRY_TIMING, new BasicEvent("backoff-1", "data")).get(10, TimeUnit.SECONDS);

        assertTrue(latch.await(30, TimeUnit.SECONDS), "Handler not invoked within 30s");
        assertEquals(3, attempts.get(), "Should attempt 3 times");

        long[] ts = timestamps.get();
        long totalTime = ts[2] - ts[0];

        assertTrue(totalTime >= 200,
                "Total retry time should be at least 200ms (100ms + 200ms backoff), was " + totalTime + "ms");
    }

    // ── 6. Non-retryable failure ───────────────────────────────────────────

    @Test
    void nonRetryableFailure() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger(0);

        NonRetryableHandler handler = context.getBean(NonRetryableHandler.class);
        handler.reset(latch, attempts);

        kafkaTemplate.send(TOPIC_NON_RETRYABLE, new BasicEvent("nr-1", "data")).get(10, TimeUnit.SECONDS);

        latch.await(15, TimeUnit.SECONDS);
        Thread.sleep(1000);
        assertEquals(1, attempts.get(), "Non-retryable exception should not be retried");
    }

    // ── 7 & 8. DLT routing with metadata ───────────────────────────────────

    @Test
    void dltRoutingAndMetadata() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger attempts = new AtomicInteger(0);

        DltHandler handler = context.getBean(DltHandler.class);
        handler.reset(latch, attempts);

        mantoProducer.publish(TOPIC_DLT, new BasicEvent("dlt-1", "fail-me"));

        latch.await(15, TimeUnit.SECONDS);
        Thread.sleep(2000);

        ConsumerRecord<String, String> dltRecord = consumeRaw(TOPIC_DLT + ".DLT");

        assertNotNull(headerValue(dltRecord, MantoHeaders.DLT_ORIGINAL_TOPIC), "DLT original topic header missing");
        assertEquals(TOPIC_DLT, headerValue(dltRecord, MantoHeaders.DLT_ORIGINAL_TOPIC));
        assertNotNull(headerValue(dltRecord, MantoHeaders.DLT_EXCEPTION_CLASS), "DLT exception class header missing");
        assertNotNull(headerValue(dltRecord, MantoHeaders.DLT_EXCEPTION_MESSAGE), "DLT exception message header missing");
        assertNotNull(headerValue(dltRecord, MantoHeaders.DLT_RETRY_COUNT), "DLT retry count header missing");
        assertNotNull(headerValue(dltRecord, MantoHeaders.DLT_FAILURE_TIMESTAMP), "DLT failure timestamp header missing");
        assertNotNull(headerValue(dltRecord, MantoHeaders.DLT_TRACE_ID), "DLT trace ID header missing");
        assertNotNull(headerValue(dltRecord, MantoHeaders.EVENT_ID), "Event ID header missing from DLT");
        assertNotNull(headerValue(dltRecord, MantoHeaders.CORRELATION_ID), "Correlation ID header missing from DLT");
    }

    // ── 9. Idempotency ─────────────────────────────────────────────────────

    @Test
    void idempotency() throws Exception {
        IdempotencyHandler handler = context.getBean(IdempotencyHandler.class);
        IdempotencyStore store = context.getBean(IdempotencyStore.class);
        handler.setStore(store);

        String idempotencyKey = "idempotent-event-key-123";
        AtomicInteger processCount = new AtomicInteger(0);

        CountDownLatch latch1 = new CountDownLatch(1);
        AtomicReference<BasicEvent> ref1 = new AtomicReference<>();
        handler.reset(latch1, ref1, processCount);

        ((io.github.manto.kafka.MantoKafkaProducer) mantoProducer)
                .publish(TOPIC_IDEMPOTENCY, new BasicEvent("idem-1", "first"), idempotencyKey);
        assertTrue(latch1.await(30, TimeUnit.SECONDS), "First invocation not received");
        assertEquals(1, processCount.get(), "First message should be processed");

        CountDownLatch latch2 = new CountDownLatch(1);
        AtomicReference<BasicEvent> ref2 = new AtomicReference<>();
        handler.reset(latch2, ref2, processCount);

        ((io.github.manto.kafka.MantoKafkaProducer) mantoProducer)
                .publish(TOPIC_IDEMPOTENCY, new BasicEvent("idem-1", "duplicate"), idempotencyKey);
        latch2.await(5, TimeUnit.SECONDS);

        Thread.sleep(1000);
        assertEquals(1, handler.getSkippedCount().get(), "Second invocation should be skipped");
        assertEquals(1, processCount.get(), "Handler should only process once");
    }

    // ── 10. Correlation ID ──────────────────────────────────────────────────

    @Test
    void correlationId() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> capturedCorrelationId = new AtomicReference<>();

        CorrelationHandler handler = context.getBean(CorrelationHandler.class);
        handler.reset(latch, capturedCorrelationId);

        String expectedCorrelationId = "upstream-order-service-789";
        ((io.github.manto.kafka.MantoKafkaProducer) mantoProducer)
                .publish(TOPIC_CORRELATION, new BasicEvent("corr-1", "data"), expectedCorrelationId);

        assertTrue(latch.await(30, TimeUnit.SECONDS), "Handler not invoked within 30s");
        assertNotNull(capturedCorrelationId.get(), "Correlation ID should be available in handler");
        assertEquals(expectedCorrelationId, capturedCorrelationId.get());
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private ConsumerRecord<String, String> consumeRaw(String topic) {
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
        fail("No record received on topic '" + topic + "' within 30s");
        return null;
    }

    private String headerValue(ConsumerRecord<?, ?> record, String headerName) {
        org.apache.kafka.common.header.Header header = record.headers().lastHeader(headerName);
        return header != null ? new String(header.value()) : null;
    }

    // ── Event records ──────────────────────────────────────────────────────

    record BasicEvent(String id, String data) {
    }

    record JsonEvent(String orderId, long amount, boolean priority) {
    }

    // ── Configuration ──────────────────────────────────────────────────────

    @Configuration
    @EnableKafka
    @Import(MantoAutoConfiguration.class)
    static class TestConfig {

        @Bean
        BasicHandler basicHandler() {
            return new BasicHandler();
        }

        @Bean
        JsonHandler jsonHandler() {
            return new JsonHandler();
        }

        @Bean
        MetadataHandler metadataHandler() {
            return new MetadataHandler();
        }

        @Bean
        RetryHandler retryHandler() {
            return new RetryHandler();
        }

        @Bean
        NonRetryableHandler nonRetryableHandler() {
            return new NonRetryableHandler();
        }

        @Bean
        DltHandler dltHandler() {
            return new DltHandler();
        }

        @Bean
        IdempotencyHandler idempotencyHandler() {
            return new IdempotencyHandler();
        }

        @Bean
        CorrelationHandler correlationHandler() {
            return new CorrelationHandler();
        }
    }

    // ── Handlers ───────────────────────────────────────────────────────────

    static class BasicHandler {
        private CountDownLatch latch;
        private AtomicReference<BasicEvent> received;

        void reset(CountDownLatch latch, AtomicReference<BasicEvent> received) {
            this.latch = latch;
            this.received = received;
        }

        @MantoListener(topic = TOPIC_BASIC, groupId = GROUP_PREFIX + "-basic")
        public void handle(BasicEvent event) {
            received.set(event);
            latch.countDown();
        }
    }

    static class JsonHandler {
        private CountDownLatch latch;
        private AtomicReference<JsonEvent> received;

        void reset(CountDownLatch latch, AtomicReference<JsonEvent> received) {
            this.latch = latch;
            this.received = received;
        }

        @MantoListener(topic = TOPIC_JSON, groupId = GROUP_PREFIX + "-json")
        public void handle(JsonEvent event) {
            received.set(event);
            latch.countDown();
        }
    }

    static class MetadataHandler {
        private CountDownLatch latch;
        private AtomicReference<BasicEvent> received;

        void reset(CountDownLatch latch, AtomicReference<BasicEvent> received) {
            this.latch = latch;
            this.received = received;
        }

        @MantoListener(topic = TOPIC_METADATA, groupId = GROUP_PREFIX + "-metadata")
        public void handle(BasicEvent event) {
            received.set(event);
            latch.countDown();
        }
    }

    static class RetryHandler {
        private CountDownLatch latch;
        private AtomicReference<BasicEvent> received;
        private AtomicInteger attempts;
        private int failCount;
        private int currentAttempt;
        private long[] timestamps;

        void reset(CountDownLatch latch, AtomicReference<BasicEvent> received,
                   AtomicInteger attempts, int failCount) {
            this.latch = latch;
            this.received = received;
            this.attempts = attempts;
            this.failCount = failCount;
            this.currentAttempt = 0;
            this.timestamps = null;
        }

        void setTimestamps(long[] timestamps) {
            this.timestamps = timestamps;
        }

        @MantoListener(topic = TOPIC_RETRY_SUCCESS, groupId = GROUP_PREFIX + "-retry-success")
        public void handleRetrySuccess(BasicEvent event) {
            currentAttempt++;
            attempts.incrementAndGet();
            if (timestamps != null && currentAttempt <= timestamps.length) {
                timestamps[currentAttempt - 1] = System.currentTimeMillis();
            }
            if (currentAttempt <= failCount) {
                throw new RuntimeException("Simulated failure #" + currentAttempt);
            }
            received.set(event);
            latch.countDown();
        }

        @MantoListener(topic = TOPIC_RETRY_TIMING, groupId = GROUP_PREFIX + "-retry-timing")
        public void handleRetryTiming(BasicEvent event) {
            currentAttempt++;
            attempts.incrementAndGet();
            if (timestamps != null && currentAttempt <= timestamps.length) {
                timestamps[currentAttempt - 1] = System.currentTimeMillis();
            }
            if (currentAttempt <= failCount) {
                throw new RuntimeException("Simulated failure #" + currentAttempt);
            }
            received.set(event);
            latch.countDown();
        }
    }

    static class NonRetryableHandler {
        private CountDownLatch latch;
        private AtomicInteger attempts;

        void reset(CountDownLatch latch, AtomicInteger attempts) {
            this.latch = latch;
            this.attempts = attempts;
        }

        @MantoListener(topic = TOPIC_NON_RETRYABLE, groupId = GROUP_PREFIX + "-non-retryable")
        public void handle(BasicEvent event) {
            attempts.incrementAndGet();
            latch.countDown();
            throw new IllegalArgumentException("Permanent failure - bad data");
        }
    }

    static class DltHandler {
        private CountDownLatch latch;
        private AtomicInteger attempts;

        void reset(CountDownLatch latch, AtomicInteger attempts) {
            this.latch = latch;
            this.attempts = attempts;
        }

        @MantoListener(topic = TOPIC_DLT, groupId = GROUP_PREFIX + "-dlt")
        public void handle(BasicEvent event) {
            attempts.incrementAndGet();
            latch.countDown();
            throw new RuntimeException("DLT test failure");
        }
    }

    static class IdempotencyHandler {
        private IdempotencyStore store;
        private CountDownLatch latch;
        private AtomicReference<BasicEvent> received;
        private AtomicInteger attempts;
        private final AtomicInteger skippedCount = new AtomicInteger(0);
        private final AtomicInteger totalAttempts = new AtomicInteger(0);

        void setStore(IdempotencyStore store) {
            this.store = store;
        }

        void reset(CountDownLatch latch, AtomicReference<BasicEvent> received, AtomicInteger attempts) {
            this.latch = latch;
            this.received = received;
            this.attempts = attempts;
        }

        AtomicInteger getSkippedCount() {
            return skippedCount;
        }

        AtomicInteger getTotalAttempts() {
            return totalAttempts;
        }

        @MantoListener(topic = TOPIC_IDEMPOTENCY, groupId = GROUP_PREFIX + "-idempotency")
        public void handle(BasicEvent event) {
            totalAttempts.incrementAndGet();
            String eventId = CorrelationIdContext.get();
            if (store != null && eventId != null && store.isProcessed(eventId)) {
                skippedCount.incrementAndGet();
                return;
            }
            attempts.incrementAndGet();
            received.set(event);
            if (store != null && eventId != null) {
                store.markProcessed(eventId);
            }
            latch.countDown();
        }
    }

    static class CorrelationHandler {
        private CountDownLatch latch;
        private AtomicReference<String> capturedCorrelationId;

        void reset(CountDownLatch latch, AtomicReference<String> capturedCorrelationId) {
            this.latch = latch;
            this.capturedCorrelationId = capturedCorrelationId;
        }

        @MantoListener(topic = TOPIC_CORRELATION, groupId = GROUP_PREFIX + "-correlation")
        public void handle(BasicEvent event) {
            capturedCorrelationId.set(CorrelationIdContext.get());
            latch.countDown();
        }
    }
}
