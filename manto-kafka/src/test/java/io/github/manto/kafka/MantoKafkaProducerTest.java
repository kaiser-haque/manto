package io.github.manto.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
        when(kafkaTemplate.send(any(String.class), any())).thenReturn(CompletableFuture.completedFuture(sendResult()));

        MantoKafkaProducer producer = new MantoKafkaProducer(kafkaTemplate);
        producer.publish("order-events", event);

        ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate).send(eq("order-events"), eventCaptor.capture());
        assertSame(event, eventCaptor.getValue());
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
        when(kafkaTemplate.send(any(String.class), any()))
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
        when(kafkaTemplate.send(any(String.class), any())).thenReturn(future);

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
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("order-events", 0), 0, 0, System.currentTimeMillis(), 0, 0);
        return new SendResult<>(new ProducerRecord<>("order-events", "order-1"), metadata);
    }
}