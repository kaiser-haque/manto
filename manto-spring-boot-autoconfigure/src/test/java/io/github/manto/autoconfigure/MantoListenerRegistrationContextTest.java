package io.github.manto.autoconfigure;

import io.github.manto.core.MantoListener;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MantoListenerRegistrationContextTest {

    static final String EXPECTED_ENDPOINT_ID =
            "order-events:payment-service:" + OrderHandler.class.getName() + ".handleOrder";

    @Test
    void registersMantoListenerEndpointInSpringKafkaRegistry() {
        try (AnnotationConfigApplicationContext context =
                     new AnnotationConfigApplicationContext(TestConfig.class)) {
            KafkaListenerEndpointRegistry registry = context.getBean(KafkaListenerEndpointRegistry.class);

            assertTrue(registry.getListenerContainerIds().contains(EXPECTED_ENDPOINT_ID));
        }
    }

    @Configuration
    @EnableKafka
    @Import(MantoAutoConfiguration.class)
    static class TestConfig {

        @Bean
        ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
                ConsumerFactory<String, Object> consumerFactory) {
            ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                    new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory);
            return factory;
        }

        @Bean
        ConsumerFactory<String, Object> consumerFactory() {
            ConsumerFactory<String, Object> factory = mock(ConsumerFactory.class);
            when(factory.createConsumer(any(), any(), any(), any()))
                    .thenReturn(new MockConsumer<>(OffsetResetStrategy.EARLIEST));
            return factory;
        }

        @Bean
        OrderHandler orderHandler() {
            return new OrderHandler();
        }
    }

    static class OrderHandler {

        @MantoListener(topic = "order-events", groupId = "payment-service")
        public void handleOrder(String event) {
        }
    }
}