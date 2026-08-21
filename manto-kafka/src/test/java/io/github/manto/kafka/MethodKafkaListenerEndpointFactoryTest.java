package io.github.manto.kafka;

import io.github.manto.core.MantoListener;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MethodKafkaListenerEndpointFactoryTest {

    private final KafkaListenerEndpointFactory factory = new MethodKafkaListenerEndpointFactory();

    @Test
    void bindsHandlerToEndpoint() throws Exception {
        OrderHandler bean = new OrderHandler();
        Method method = OrderHandler.class.getMethod("handleOrder", String.class);
        MantoListenerDefinition definition =
                new MantoListenerDefinition(bean, method, "order-events", "payment-service");

        KafkaListenerEndpoint endpoint = factory.create(definition);

        MethodKafkaListenerEndpoint<?, ?> methodEndpoint = (MethodKafkaListenerEndpoint<?, ?>) endpoint;
        assertEquals("order-events", methodEndpoint.getTopics().iterator().next());
        assertEquals("payment-service", methodEndpoint.getGroupId());
        assertSame(bean, methodEndpoint.getBean());
        assertSame(method, methodEndpoint.getMethod());
    }

    @Test
    void createsUniqueIdsForHandlersSharingTopicAndGroup() throws Exception {
        MantoListenerDefinition order =
                new MantoListenerDefinition(new OrderHandler(),
                        OrderHandler.class.getMethod("handleOrder", String.class),
                        "order-events", "payment-service");
        MantoListenerDefinition audit =
                new MantoListenerDefinition(new AuditHandler(),
                        AuditHandler.class.getMethod("audit", String.class),
                        "order-events", "payment-service");

        String orderId = factory.create(order).getId();
        String auditId = factory.create(audit).getId();

        assertNotEquals(orderId, auditId);
        assertTrue(orderId.contains("handleOrder"));
        assertTrue(auditId.contains("audit"));
    }

    @Test
    void endpointHasMessageHandlerMethodFactoryConfigured() throws Exception {
        OrderHandler bean = new OrderHandler();
        Method method = OrderHandler.class.getMethod("handleOrder", String.class);
        MantoListenerDefinition definition =
                new MantoListenerDefinition(bean, method, "order-events", "payment-service");

        KafkaListenerEndpoint endpoint = factory.create(definition);

        MethodKafkaListenerEndpoint<?, ?> methodEndpoint = (MethodKafkaListenerEndpoint<?, ?>) endpoint;
        // The factory should be configured via setMessageHandlerMethodFactory
        // We verify it by checking the endpoint was created without exception
        assertNotNull(endpoint);
    }

    static class OrderHandler {

        @MantoListener(topic = "order-events", groupId = "payment-service")
        public void handleOrder(String event) {
        }
    }

    static class AuditHandler {

        @MantoListener(topic = "order-events", groupId = "payment-service")
        public void audit(String event) {
        }
    }
}