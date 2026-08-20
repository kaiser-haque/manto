package io.github.manto.kafka;

import io.github.manto.core.MantoListener;
import org.junit.jupiter.api.Test;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.beans.factory.ListableBeanFactory;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MantoListenerDiscovererTest {

    private final MantoListenerDiscoverer discoverer = new MantoListenerDiscoverer(new MantoListenerValidator());

    @Test
    void discoversAnnotatedMethodsInBeanOrder() {
        OrderHandler orderHandler = new OrderHandler();
        PaymentHandler paymentHandler = new PaymentHandler();

        ListableBeanFactory beanFactory = beanFactoryWith("orderHandler", orderHandler, "paymentHandler", paymentHandler);

        List<MantoListenerDefinition> definitions = discoverer.discover(beanFactory);

        assertEquals(2, definitions.size());
        assertEquals("order-events", definitions.get(0).topic());
        assertEquals("payment-service", definitions.get(0).groupId());
        assertSame(orderHandler, definitions.get(0).bean());
        assertEquals("handleOrder", definitions.get(0).method().getName());
        assertEquals("payment-events", definitions.get(1).topic());
        assertSame(paymentHandler, definitions.get(1).bean());
    }

    @Test
    void ignoresBeansWithoutAnnotatedMethods() {
        ListableBeanFactory beanFactory = beanFactoryWith("plain", new Object(), "service", new PlainService());

        List<MantoListenerDefinition> definitions = discoverer.discover(beanFactory);

        assertTrue(definitions.isEmpty());
    }

    @Test
    void unwrapsProxiedBeans() {
        OrderHandler target = new OrderHandler();
        ProxyFactory proxyFactory = new ProxyFactory();
        proxyFactory.setTarget(target);
        proxyFactory.setProxyTargetClass(true);
        Object proxy = proxyFactory.getProxy();

        ListableBeanFactory beanFactory = beanFactoryWith("orderHandler", proxy);

        List<MantoListenerDefinition> definitions = discoverer.discover(beanFactory);

        assertEquals(1, definitions.size());
        assertSame(proxy, definitions.get(0).bean());
        assertEquals("handleOrder", definitions.get(0).method().getName());
    }

    @Test
    void failsFastOnInvalidHandler() {
        ListableBeanFactory beanFactory = beanFactoryWith("invalidHandler", new InvalidHandler());

        MantoListenerConfigurationException e = assertThrows(MantoListenerConfigurationException.class,
                () -> discoverer.discover(beanFactory));

        assertTrue(e.getMessage().contains(InvalidHandler.class.getName() + "#handle"));
        assertTrue(e.getMessage().contains("topic"));
    }

    private ListableBeanFactory beanFactoryWith(Object... nameAndBeans) {
        ListableBeanFactory beanFactory = mock(ListableBeanFactory.class);
        String[] names = new String[nameAndBeans.length / 2];
        for (int i = 0; i < nameAndBeans.length; i += 2) {
            String beanName = (String) nameAndBeans[i];
            names[i / 2] = beanName;
            when(beanFactory.getBean(beanName)).thenReturn(nameAndBeans[i + 1]);
        }
        when(beanFactory.getBeanDefinitionNames()).thenReturn(names);
        return beanFactory;
    }

    static class OrderHandler {

        @MantoListener(topic = "order-events", groupId = "payment-service")
        public void handleOrder(String event) {
        }

        public void notAListener(String event) {
        }
    }

    static class PaymentHandler {

        @MantoListener(topic = "payment-events", groupId = "payment-service")
        public void handlePayment(String event) {
        }
    }

    static class PlainService {

        public void doWork() {
        }
    }

    static class InvalidHandler {

        @MantoListener(topic = "", groupId = "payment-service")
        public void handle(String event) {
        }
    }
}