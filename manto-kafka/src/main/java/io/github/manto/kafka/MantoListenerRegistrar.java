package io.github.manto.kafka;

import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.kafka.annotation.KafkaListenerConfigurer;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;

import java.util.List;

/**
 * Registers {@link io.github.manto.core.MantoListener} handlers with Spring
 * Kafka.
 *
 * <p>Implements {@link KafkaListenerConfigurer}, so Spring Kafka invokes
 * {@link #configureKafkaListeners(KafkaListenerEndpointRegistrar)} during
 * listener bootstrap — after all singletons are instantiated. Discovery
 * therefore happens against a fully populated bean factory.</p>
 *
 * <p>This class is a configuration boundary: discovery or registration
 * failures are reported at startup (fail fast) and never touch runtime
 * message processing, which remains under Spring Kafka's control.</p>
 */
public class MantoListenerRegistrar implements KafkaListenerConfigurer {

    private final ListableBeanFactory beanFactory;
    private final MantoListenerDiscoverer discoverer;
    private final KafkaListenerEndpointFactory endpointFactory;

    public MantoListenerRegistrar(ListableBeanFactory beanFactory,
                                  MantoListenerDiscoverer discoverer,
                                  KafkaListenerEndpointFactory endpointFactory) {
        this.beanFactory = beanFactory;
        this.discoverer = discoverer;
        this.endpointFactory = endpointFactory;
    }

    @Override
    public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
        List<MantoListenerDefinition> definitions = discoverer.discover(beanFactory);
        for (MantoListenerDefinition definition : definitions) {
            KafkaListenerEndpoint endpoint = endpointFactory.create(definition);
            registrar.registerEndpoint(endpoint);
        }
    }
}