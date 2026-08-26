package io.github.manto.autoconfigure;

import io.github.manto.core.MantoProducer;
import io.github.manto.core.RetryPolicy;
import io.github.manto.kafka.DefaultDeadLetterHandler;
import io.github.manto.kafka.DefaultExceptionClassifier;
import io.github.manto.kafka.DefaultRetryPolicy;
import io.github.manto.kafka.ExponentialBackoffStrategy;
import io.github.manto.kafka.KafkaListenerEndpointFactory;
import io.github.manto.kafka.MantoDeadLetterPublishingRecoverer;
import io.github.manto.kafka.MantoKafkaProducer;
import io.github.manto.kafka.MantoListenerDiscoverer;
import io.github.manto.kafka.MantoListenerRegistrar;
import io.github.manto.kafka.MantoListenerValidator;
import io.github.manto.kafka.MethodKafkaListenerEndpointFactory;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Auto-configuration for Manto producer, consumer, and listener registration.
 *
 * <p>Provides the following beans when Spring Kafka is on the classpath:
 * <ul>
 *   <li>{@link MantoProducer} - the producer abstraction, backed by {@link MantoKafkaProducer}</li>
 *   <li>{@link KafkaTemplate} - Spring Kafka template for direct use if needed</li>
 *   <li>{@link ConcurrentKafkaListenerContainerFactory} - consumer container factory using Spring's JSON deserializer with type headers</li>
 *   <li>Listener registration infrastructure (validator, discoverer, endpoint factory, registrar)</li>
 *   <li>Retry and DLT infrastructure abstractions (retry policy, backoff strategy, exception classifier, dead-letter handler)</li>
 * </ul>
 *
 * <p>Configuration is driven by {@link MantoProperties} with prefix {@code manto}.
 * Bootstrap servers default to {@code localhost:9092}.</p>
 */
@AutoConfiguration
@ConditionalOnClass(KafkaListenerContainerFactory.class)
@EnableConfigurationProperties(MantoProperties.class)
public class MantoAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public ProducerFactory<String, Object> mantoProducerFactory(MantoProperties properties) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getKafka().getBootstrapServers());
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(configs);
    }

    @Bean
    @ConditionalOnMissingBean
    public KafkaTemplate<String, Object> mantoKafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    @ConditionalOnMissingBean
    public MantoProducer mantoProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        return new MantoKafkaProducer(kafkaTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public ConsumerFactory<String, Object> mantoConsumerFactory(MantoProperties properties) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getKafka().getBootstrapServers());
        configs.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        configs.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configs.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        configs.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, true);
        return new DefaultKafkaConsumerFactory<>(configs);
    }

    @Bean
    @ConditionalOnMissingBean
    public ProducerFactory<Object, Object> mantoDltProducerFactory(MantoProperties properties) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getKafka().getBootstrapServers());
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(configs);
    }

    @Bean
    @ConditionalOnMissingBean
    public KafkaTemplate<Object, Object> mantoDltKafkaTemplate(ProducerFactory<Object, Object> dltProducerFactory) {
        return new KafkaTemplate<>(dltProducerFactory);
    }

    @Bean
    @ConditionalOnMissingBean
    public DefaultRetryPolicy mantoRetryPolicy(MantoProperties properties) {
        MantoProperties.Retry retryProps = properties.getRetry();
        return new DefaultRetryPolicy(retryProps.isEnabled(), retryProps.getMaxAttempts());
    }

    @Bean
    @ConditionalOnMissingBean
    public ExponentialBackoffStrategy mantoBackoffStrategy(MantoProperties properties) {
        MantoProperties.Retry.Backoff backoffProps = properties.getRetry().getBackoff();
        return new ExponentialBackoffStrategy(
                backoffProps.getInitialDelay(),
                backoffProps.getMultiplier(),
                backoffProps.getMaxDelay()
        );
    }

    @Bean
    @ConditionalOnMissingBean
    public DefaultExceptionClassifier mantoExceptionClassifier() {
        return new DefaultExceptionClassifier();
    }

    @Bean
    @ConditionalOnMissingBean
    public DefaultDeadLetterHandler mantoDeadLetterHandler(KafkaTemplate<Object, Object> dltKafkaTemplate,
                                                            MantoProperties properties) {
        return new DefaultDeadLetterHandler(dltKafkaTemplate, properties.getDlt().getTopicSuffix());
    }

    @Bean
    @ConditionalOnMissingBean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            RetryPolicy retryPolicy,
            ExponentialBackoffStrategy backoffStrategy,
            DefaultExceptionClassifier exceptionClassifier,
            DefaultDeadLetterHandler deadLetterHandler,
            KafkaTemplate<Object, Object> dltKafkaTemplate,
            MantoProperties properties) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        if (retryPolicy.isEnabled()) {
            ExponentialBackOff backOff = new ExponentialBackOff();
            backOff.setInitialInterval(backoffStrategy.getInitialDelay().toMillis());
            backOff.setMultiplier(backoffStrategy.getMultiplier());
            backOff.setMaxInterval(backoffStrategy.getMaxDelay().toMillis());
            backOff.setMaxAttempts(Math.max(0, retryPolicy.maxAttempts() - 1));

            DefaultErrorHandler errorHandler;
            if (properties.getDlt().isEnabled()) {
                MantoDeadLetterPublishingRecoverer recoverer =
                        new MantoDeadLetterPublishingRecoverer(deadLetterHandler, exceptionClassifier, retryPolicy, dltKafkaTemplate, properties.getDlt().getTopicSuffix());
                errorHandler = new DefaultErrorHandler(recoverer, backOff);
            } else {
                errorHandler = new DefaultErrorHandler(backOff);
            }
            configureExceptionClassifier(errorHandler, exceptionClassifier);
            factory.setCommonErrorHandler(errorHandler);
        }

        return factory;
    }

    private void configureExceptionClassifier(DefaultErrorHandler errorHandler,
                                              DefaultExceptionClassifier exceptionClassifier) {
        Set<Class<? extends Throwable>> nonRetryableTypes = exceptionClassifier.getNonRetryableTypes();
        for (Class<? extends Throwable> type : nonRetryableTypes) {
            if (Exception.class.isAssignableFrom(type)) {
                errorHandler.addNotRetryableExceptions(type.asSubclass(Exception.class));
            }
        }
    }

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