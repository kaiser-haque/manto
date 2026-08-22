package io.github.manto.kafka;

import io.github.manto.core.MantoHeaders;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MantoKafkaProducerTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    private record OrderCreatedEvent(String orderId, long amount) {
    }

    @Test
    void publishesTypedEventToTopic() {
        OrderCreatedEvent event = new OrderCreatedEvent("order-1", 42);
        when(kafkaTemplate.send(any(Message.class))).thenReturn(CompletableFuture.completedFuture(sendResult()));

        MantoKafkaProducer producer = new MantoKafkaProducer(kafkaTemplate, "test-source");
        producer.publish("order-events", event);

        ArgumentCaptor<Message<?>> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(kafkaTemplate).send(messageCaptor.capture());
        Message<?> message = messageCaptor.getValue();

        assertSame(event, message.getPayload());
        assertEquals("order-events", message.getHeaders().get("kafka_topic"));
        assertNotNull(message.getHeaders().get(MantoHeaders.EVENT_ID));
        assertEquals("OrderCreatedEvent", message.getHeaders().get(MantoHeaders.EVENT_TYPE));
        assertEquals("1.0", message.getHeaders().get(MantoHeaders.EVENT_VERSION));
        assertEquals(message.getHeaders().get(MantoHeaders.EVENT_ID), message.getHeaders().get(MantoHeaders.CORRELATION_ID));
        assertEquals("test-source", message.getHeaders().get(MantoHeaders.SOURCE));
    }

    @Test
    void publishesWithDefaultSourceWhenNotProvided() {
        OrderCreatedEvent event = new OrderCreatedEvent("order-1", 42);
        when(kafkaTemplate.send(any(Message.class))).thenReturn(CompletableFuture.completedFuture(sendResult()));

        MantoKafkaProducer producer = new MantoKafkaProducer(kafkaTemplate);
        producer.publish("order-events", event);

        ArgumentCaptor<Message<?>> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(kafkaTemplate).send(messageCaptor.capture());
        assertEquals("unknown", messageCaptor.getValue().getHeaders().get(MantoHeaders.SOURCE));
    }

    @Test
    void generatesUniqueEventIdPerPublish() {
        OrderCreatedEvent event = new OrderCreatedEvent("order-1", 42);
        when(kafkaTemplate.send(any(Message.class))).thenReturn(CompletableFuture.completedFuture(sendResult()));

        MantoKafkaProducer producer = new MantoKafkaProducer(kafkaTemplate, "test-source");
        producer.publish("order-events", event);
        producer.publish("order-events", event);

        ArgumentCaptor<Message<?>> messageCaptor = ArgumentCaptor.forClass(Message.class);
        verify(kafkaTemplate, org.mockito.Mockito.times(2)).send(messageCaptor.capture());
        var messages = messageCaptor.getAllValues();
        String id1 = (String) messages.get(0).getHeaders().get(MantoHeaders.EVENT_ID);
        String id2 = (String) messages.get(1).getHeaders().get(MantoHeaders.EVENT_ID);

        assertNotNull(id1);
        assertNotNull(id2);
        assertNotEquals(id1, id2);
    }

    @Test
    void rejectsNullTopic() {
        MantoKafkaProducer producer = new MantoKafkaProducer(kafkaTemplate);

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> producer.publish(null, new OrderCreatedEvent("order-1", 42)));
        assertEquals("topic must not be null or blank", e.getMessage());
    }

    @Test
    void rejectsBlankTopic() {
        MantoKafkaProducer producer = new MantoKafkaProducer(kafkaTemplate);

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> producer.publish("   ", new OrderCreatedEvent("order-1", 42)));
        assertEquals("topic must not be null or blank", e.getMessage());
    }

    @Test
    void rejectsNullEvent() {
        MantoKafkaProducer producer = new MantoKafkaProducer(kafkaTemplate);

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> producer.publish("order-events", null));
        assertEquals("event must not be null", e.getMessage());
    }

    @Test
    void wrapsSendFailure() {
        IllegalStateException cause = new IllegalStateException("broker unreachable");
        when(kafkaTemplate.send(any(Message.class)))
                .thenReturn(CompletableFuture.failedFuture(cause));

        MantoKafkaProducer producer = new MantoKafkaProducer(kafkaTemplate);

        MantoProducerException e = assertThrows(MantoProducerException.class,
                () -> producer.publish("order-events", new OrderCreatedEvent("order-1", 42)));
        assertEquals("Failed to publish event to topic 'order-events'", e.getMessage());
        assertSame(cause, e.getCause());
    }

    @Test
    void restoresInterruptFlagWhenInterrupted() {
        CompletableFuture<SendResult<String, Object>> future = new CompletableFuture<>();
        when(kafkaTemplate.send(any(Message.class))).thenReturn(future);

        MantoKafkaProducer producer = new MantoKafkaProducer(kafkaTemplate);
        Thread.currentThread().interrupt();

        try {
            MantoProducerException e = assertThrows(MantoProducerException.class,
                    () -> producer.publish("order-events", new OrderCreatedEvent("order-1", 42)));
            assertEquals("Interrupted while publishing event to topic 'order-events'", e.getMessage());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    private SendResult<String, Object> sendResult() {
        Headers headers = new RecordHeaders();
        ProducerRecord<String, Object> record = new ProducerRecord<>("order-events", null, "order-1", new OrderCreatedEvent("order-1", 42), headers);
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("order-events", 0), 0, 0, System.currentTimeMillis(), 0, 0);
        return new SendResult<>(record, metadata);
    }
}