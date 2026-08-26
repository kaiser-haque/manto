package io.github.manto.kafka;

import io.github.manto.core.MantoHeaders;
import io.github.manto.core.RetryPolicy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;

import java.util.function.BiFunction;

/**
 * Spring Kafka recoverer that extends {@link DeadLetterPublishingRecoverer} to use
 * Manto's DLT topic naming convention and custom headers.
 */
public class MantoDeadLetterPublishingRecoverer extends DeadLetterPublishingRecoverer {

    private final DefaultDeadLetterHandler deadLetterHandler;
    private final DefaultExceptionClassifier exceptionClassifier;
    private final RetryPolicy retryPolicy;

    public MantoDeadLetterPublishingRecoverer(DefaultDeadLetterHandler deadLetterHandler,
                                               DefaultExceptionClassifier exceptionClassifier,
                                               RetryPolicy retryPolicy,
                                               KafkaTemplate<Object, Object> kafkaTemplate,
                                               String topicSuffix) {
        super(kafkaTemplate, createDestinationResolver(topicSuffix));
        this.deadLetterHandler = deadLetterHandler;
        this.exceptionClassifier = exceptionClassifier;
        this.retryPolicy = retryPolicy;
    }

    private static BiFunction<ConsumerRecord<?, ?>, Exception, org.apache.kafka.common.TopicPartition> createDestinationResolver(String topicSuffix) {
        return (record, exception) -> new org.apache.kafka.common.TopicPartition(record.topic() + topicSuffix, record.partition());
    }
}