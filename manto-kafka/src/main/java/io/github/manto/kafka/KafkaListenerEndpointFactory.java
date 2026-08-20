package io.github.manto.kafka;

import org.springframework.kafka.config.KafkaListenerEndpoint;

/**
 * Creates a Spring Kafka {@link KafkaListenerEndpoint} from a discovered
 * {@link MantoListenerDefinition}.
 */
public interface KafkaListenerEndpointFactory {

    /**
     * Creates the endpoint that registers the handler with Spring Kafka.
     *
     * @param definition the discovered listener definition
     * @return the Spring Kafka endpoint
     * @throws MantoListenerConfigurationException if the endpoint cannot be created
     */
    KafkaListenerEndpoint create(MantoListenerDefinition definition);
}