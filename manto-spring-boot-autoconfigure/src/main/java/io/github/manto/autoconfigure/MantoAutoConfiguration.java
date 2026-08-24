package io.github.manto.autoconfigure;

import io.github.manto.kafka.KafkaListenerEndpointFactory;
import io.github.manto.kafka.MantoListenerDiscoverer;
import io.github.manto.kafka.MantoListenerRegistrar;
import io.github.manto.kafka.MantoListenerValidator;
import io.github.manto.kafka.MethodKafkaListenerEndpointFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.KafkaListenerContainerFactory;

/**
 * Auto-configuration for Manto listener registration.
 *
 * <p>Declares the beans that discover {@link io.github.manto.core.MantoListener}
 * handlers and register them with Spring Kafka. Registration requires a
 * Spring Kafka listener container factory, which Spring Boot's
 * {@code KafkaAutoConfiguration} provides in a typical application.</p>
 */
@AutoConfiguration
@ConditionalOnClass(KafkaListenerContainerFactory.class)
@EnableConfigurationProperties(MantoProperties.class)
public class MantoAutoConfiguration {

    @Bean
    public MantoListenerValidator mantoListenerValidator() {
        return new MantoListenerValidator();
    }

    @Bean
    public MantoListenerDiscoverer mantoListenerDiscoverer(MantoListenerValidator validator) {
        return new MantoListenerDiscoverer(validator);
    }

    @Bean
    public KafkaListenerEndpointFactory kafkaListenerEndpointFactory() {
        return new MethodKafkaListenerEndpointFactory();
    }

    @Bean
    public MantoListenerRegistrar mantoListenerRegistrar(ListableBeanFactory beanFactory,
                                                         MantoListenerDiscoverer discoverer,
                                                         KafkaListenerEndpointFactory endpointFactory) {
        return new MantoListenerRegistrar(beanFactory, discoverer, endpointFactory);
    }
}