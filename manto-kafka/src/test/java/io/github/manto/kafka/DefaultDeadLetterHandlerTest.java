package io.github.manto.kafka;

import io.github.manto.core.DeadLetterHandler;
import io.github.manto.core.MantoHeaders;
import io.github.manto.core.MantoRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DefaultDeadLetterHandlerTest {

    @Mock
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Captor
    private ArgumentCaptor<ProducerRecord<Object, Object>> recordCaptor;

    @Test
    void shouldPublishToDltTopicWithSuffix() {
        DeadLetterHandler handler = new DefaultDeadLetterHandler(kafkaTemplate, ".DLT");

        ConsumerRecord<String, String> kafkaRecord = new ConsumerRecord<>("orders", 0, 42, "key", "value");
        MantoRecord<String, String> record = new DefaultDeadLetterHandler.KafkaMantoRecord<>(kafkaRecord);

        when(kafkaTemplate.send(anyString(), any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        handler.handle(record, new RuntimeException("processing failed"), 3);

        verify(kafkaTemplate).send(anyString(), recordCaptor.capture());
        var sentRecord = recordCaptor.getValue();
        assertEquals("orders.DLT", sentRecord.topic());
        assertEquals("value", sentRecord.value());
    }

    @Test
    void shouldUseCustomTopicSuffix() {
        DeadLetterHandler handler = new DefaultDeadLetterHandler(kafkaTemplate, "-dead-letter");

        ConsumerRecord<String, String> kafkaRecord = new ConsumerRecord<>("orders", 0, 42, "key", "value");
        MantoRecord<String, String> record = new DefaultDeadLetterHandler.KafkaMantoRecord<>(kafkaRecord);

        when(kafkaTemplate.send(anyString(), any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        handler.handle(record, new RuntimeException("processing failed"), 3);

        verify(kafkaTemplate).send(anyString(), recordCaptor.capture());
        assertEquals("orders-dead-letter", recordCaptor.getValue().topic());
    }

    @Test
    void shouldIncludeOriginalMetadataInHeaders() {
        DeadLetterHandler handler = new DefaultDeadLetterHandler(kafkaTemplate, ".DLT");

        ConsumerRecord<String, String> kafkaRecord = new ConsumerRecord<>("orders", 1, 100, "key", "value");
        MantoRecord<String, String> record = new DefaultDeadLetterHandler.KafkaMantoRecord<>(kafkaRecord);

        when(kafkaTemplate.send(anyString(), any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        handler.handle(record, new RuntimeException("test error"), 2);

        verify(kafkaTemplate).send(anyString(), recordCaptor.capture());
        var sentRecord = recordCaptor.getValue();

        // Check DLT-specific headers
        assertEquals("orders", getHeader(sentRecord, MantoHeaders.DLT_ORIGINAL_TOPIC));
        assertEquals("1", getHeader(sentRecord, MantoHeaders.DLT_ORIGINAL_PARTITION));
        assertEquals("100", getHeader(sentRecord, MantoHeaders.DLT_ORIGINAL_OFFSET));

        // Check exception info
        assertEquals("java.lang.RuntimeException", getHeader(sentRecord, MantoHeaders.DLT_EXCEPTION_CLASS));
        assertEquals("test error", getHeader(sentRecord, MantoHeaders.DLT_EXCEPTION_MESSAGE));
        assertEquals("2", getHeader(sentRecord, MantoHeaders.DLT_RETRY_COUNT));

        // Check trace ID is generated
        assertNotNull(getHeader(sentRecord, MantoHeaders.DLT_TRACE_ID));
        assertNotNull(getHeader(sentRecord, MantoHeaders.DLT_FAILURE_TIMESTAMP));
    }

    @Test
    void shouldHandleNullException() {
        DeadLetterHandler handler = new DefaultDeadLetterHandler(kafkaTemplate, ".DLT");

        ConsumerRecord<String, String> kafkaRecord = new ConsumerRecord<>("orders", 0, 0, "key", "value");
        MantoRecord<String, String> record = new DefaultDeadLetterHandler.KafkaMantoRecord<>(kafkaRecord);

        when(kafkaTemplate.send(anyString(), any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        handler.handle(record, null, 0);

        verify(kafkaTemplate).send(anyString(), recordCaptor.capture());
        var sentRecord = recordCaptor.getValue();
        assertEquals("null", getHeader(sentRecord, MantoHeaders.DLT_EXCEPTION_CLASS));
        assertEquals("null", getHeader(sentRecord, MantoHeaders.DLT_EXCEPTION_MESSAGE));
    }

    @Test
    void shouldDefaultSuffixToDltWhenNull() {
        DeadLetterHandler handler = new DefaultDeadLetterHandler(kafkaTemplate, null);

        ConsumerRecord<String, String> kafkaRecord = new ConsumerRecord<>("orders", 0, 0, "key", "value");
        MantoRecord<String, String> record = new DefaultDeadLetterHandler.KafkaMantoRecord<>(kafkaRecord);

        when(kafkaTemplate.send(anyString(), any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        handler.handle(record, new RuntimeException("test"), 1);

        verify(kafkaTemplate).send(anyString(), recordCaptor.capture());
        assertEquals("orders.DLT", recordCaptor.getValue().topic());
    }

    @Test
    void shouldExposeKafkaTemplate() {
        DeadLetterHandler handler = new DefaultDeadLetterHandler(kafkaTemplate, ".DLT");

        assertSame(kafkaTemplate, ((DefaultDeadLetterHandler) handler).getKafkaTemplate());
    }

    @Test
    void shouldHandleKafkaRecordDirectly() {
        DefaultDeadLetterHandler handler = new DefaultDeadLetterHandler(kafkaTemplate, ".DLT");

        ConsumerRecord<String, String> record = new ConsumerRecord<>("orders", 0, 42, "key", "value");

        when(kafkaTemplate.send(anyString(), any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        handler.handleKafkaRecord(record, new RuntimeException("processing failed"), 3);

        verify(kafkaTemplate).send(anyString(), recordCaptor.capture());
        var sentRecord = recordCaptor.getValue();
        assertEquals("orders.DLT", sentRecord.topic());
    }

    private String getHeader(ProducerRecord<?, ?> record, String headerName) {
        var header = record.headers().lastHeader(headerName);
        return header != null ? new String(header.value(), StandardCharsets.UTF_8) : null;
    }
}