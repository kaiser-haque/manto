package io.github.manto.kafka;

import org.springframework.aop.support.AopUtils;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.listener.adapter.KafkaMessageHandlerMethodFactory;
import org.springframework.messaging.converter.GenericMessageConverter;

import java.lang.reflect.Method;

/**
 * Default {@link KafkaListenerEndpointFactory} backed by Spring Kafka's
 * {@link MethodKafkaListenerEndpoint}.
 *
 * <p>The endpoint delegates message conversion and handler invocation to
 * Spring Kafka's messaging adapter. The listener container's record message
 * converter (typically {@code JsonMessageConverter}) converts the
 * {@code ConsumerRecord} to a {@code Message<?>}. The handler method factory
 * uses a {@link GenericMessageConverter} with a conversion service to convert
 * the message payload to the handler method's parameter type. Each endpoint
 * gets a unique id derived from the topic, group id, and handler method so
 * multiple handlers can share a topic or group.</p>
 */
public class MethodKafkaListenerEndpointFactory implements KafkaListenerEndpointFactory {

    @Override
    public KafkaListenerEndpoint create(MantoListenerDefinition definition) {
        Object bean = definition.bean();
        Method method = definition.method();

        MethodKafkaListenerEndpoint<String, ?> endpoint = new MethodKafkaListenerEndpoint<>();
        endpoint.setId(endpointId(definition));
        endpoint.setGroupId(definition.groupId());
        endpoint.setTopics(definition.topic());
        endpoint.setBean(bean);
        endpoint.setMethod(method);

        KafkaMessageHandlerMethodFactory handlerMethodFactory = new KafkaMessageHandlerMethodFactory();
        DefaultFormattingConversionService conversionService = new DefaultFormattingConversionService();
        handlerMethodFactory.setConversionService(conversionService);
        handlerMethodFactory.setMessageConverter(new GenericMessageConverter(conversionService));
        handlerMethodFactory.afterPropertiesSet();
        endpoint.setMessageHandlerMethodFactory(handlerMethodFactory);

        return endpoint;
    }

    private String endpointId(MantoListenerDefinition definition) {
        Class<?> beanType = AopUtils.getTargetClass(definition.bean());
        return definition.topic() + ":" + definition.groupId()
                + ":" + beanType.getName() + "." + definition.method().getName();
    }
}