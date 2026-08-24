package io.github.manto.autoconfigure;

import io.github.manto.core.MantoListener;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

import static org.junit.jupiter.api.Assertions.assertTrue;

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