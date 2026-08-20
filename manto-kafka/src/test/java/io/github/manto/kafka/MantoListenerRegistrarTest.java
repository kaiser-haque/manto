package io.github.manto.kafka;

import io.github.manto.core.MantoListener;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class MantoListenerRegistrarTest {

    @Test
    void registersOneEndpointPerDiscoveredHandler() {
        OrderHandler handler = new OrderHandler();
        ListableBeanFactory beanFactory = beanFactoryWith(handler);
        KafkaListenerEndpointRegistrar registrar = mock(KafkaListenerEndpointRegistrar.class);

        registrarWith(beanFactory).configureKafkaListeners(registrar);

        ArgumentCaptor<KafkaListenerEndpoint> captor = ArgumentCaptor.forClass(KafkaListenerEndpoint.class);
        verify(registrar).registerEndpoint(captor.capture());
        KafkaListenerEndpoint endpoint = captor.getValue();
        assertEquals("order-events", singleTopic(endpoint));
        assertEquals("payment-service", endpoint.getGroupId());
    }

    @Test
    void registersNothingWhenNoHandlersExist() {
        ListableBeanFactory beanFactory = beanFactoryWith(new Object());
        KafkaListenerEndpointRegistrar registrar = mock(KafkaListenerEndpointRegistrar.class);

        registrarWith(beanFactory).configureKafkaListeners(registrar);

        verifyNoInteractions(registrar);
    }

    private MantoListenerRegistrar registrarWith(ListableBeanFactory beanFactory) {
        return new MantoListenerRegistrar(beanFactory,
                new MantoListenerDiscoverer(new MantoListenerValidator()),
                new MethodKafkaListenerEndpointFactory());
    }

    private ListableBeanFactory beanFactoryWith(Object bean) {
        ListableBeanFactory beanFactory = mock(ListableBeanFactory.class);
        when(beanFactory.getBeanDefinitionNames()).thenReturn(new String[]{"handler"});
        when(beanFactory.getBean("handler")).thenReturn(bean);
        return beanFactory;
    }

    private String singleTopic(KafkaListenerEndpoint endpoint) {
        Iterator<String> topics = endpoint.getTopics().iterator();
        return topics.next();
    }

    static class OrderHandler {

        @MantoListener(topic = "order-events", groupId = "payment-service")
        public void handleOrder(String event) {
        }
    }
}