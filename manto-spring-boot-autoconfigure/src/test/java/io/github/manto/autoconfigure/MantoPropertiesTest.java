package io.github.manto.autoconfigure;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MantoPropertiesTest {

    private Validator validator;
    private Binder binder;

    @BeforeEach
    void setUp() {
        LocalValidatorFactoryBean factoryBean = new LocalValidatorFactoryBean();
        factoryBean.afterPropertiesSet();
        validator = factoryBean;

        StandardEnvironment environment = new StandardEnvironment();
        binder = Binder.get(environment);
    }

    @Test
    void defaultsAreSensible() {
        MantoProperties properties = new MantoProperties();

        assertEquals("localhost:9092", properties.getKafka().getBootstrapServers());

        assertTrue(properties.getRetry().isEnabled());
        assertEquals(3, properties.getRetry().getMaxAttempts());
        assertEquals(Duration.ofMillis(1000), properties.getRetry().getBackoff().getInitialDelay());
        assertEquals(2.0, properties.getRetry().getBackoff().getMultiplier());
        assertEquals(Duration.ofMillis(30000), properties.getRetry().getBackoff().getMaxDelay());

        assertFalse(properties.getDlt().isEnabled());
        assertEquals(".DLT", properties.getDlt().getTopicSuffix());

        assertTrue(properties.getIdempotency().isEnabled());

        assertTrue(properties.getObservability().isEnabled());
    }

    @Test
    void bindsKafkaBootstrapServersFromYaml() {
        Map<String, Object> source = Map.of("manto.kafka.bootstrap-servers", "kafka1:9092,kafka2:9092");
        MantoProperties properties = bind(source);

        assertEquals("kafka1:9092,kafka2:9092", properties.getKafka().getBootstrapServers());
    }

    @Test
    void bindsRetryConfiguration() {
        Map<String, Object> source = Map.of(
                "manto.retry.enabled", false,
                "manto.retry.max-attempts", 5,
                "manto.retry.backoff.initial-delay", "2000",
                "manto.retry.backoff.multiplier", 1.5,
                "manto.retry.backoff.max-delay", "60000"
        );
        MantoProperties properties = bind(source);

        assertFalse(properties.getRetry().isEnabled());
        assertEquals(5, properties.getRetry().getMaxAttempts());
        assertEquals(Duration.ofMillis(2000), properties.getRetry().getBackoff().getInitialDelay());
        assertEquals(1.5, properties.getRetry().getBackoff().getMultiplier());
        assertEquals(Duration.ofMillis(60000), properties.getRetry().getBackoff().getMaxDelay());
    }

    @Test
    void bindsDltConfiguration() {
        Map<String, Object> source = Map.of(
                "manto.dlt.enabled", false,
                "manto.dlt.topic-suffix", "-dead-letter"
        );
        MantoProperties properties = bind(source);

        assertFalse(properties.getDlt().isEnabled());
        assertEquals("-dead-letter", properties.getDlt().getTopicSuffix());
    }

    @Test
    void bindsIdempotencyConfiguration() {
        Map<String, Object> source = Map.of("manto.idempotency.enabled", false);
        MantoProperties properties = bind(source);

        assertFalse(properties.getIdempotency().isEnabled());
    }

    @Test
    void bindsObservabilityConfiguration() {
        Map<String, Object> source = Map.of("manto.observability.enabled", false);
        MantoProperties properties = bind(source);

        assertFalse(properties.getObservability().isEnabled());
    }

    @Test
    void validatesKafkaBootstrapServersNotBlank() {
        MantoProperties properties = new MantoProperties();
        properties.getKafka().setBootstrapServers("");

        var violations = validator.validate(properties);

        assertFalse(violations.isEmpty());
        assertTrue(violations.stream().anyMatch(v -> v.getPropertyPath().toString().contains("bootstrapServers")));
    }

    @Test
    void validatesRetryMaxAttemptsMin() {
        MantoProperties properties = new MantoProperties();
        properties.getRetry().setMaxAttempts(0);

        var violations = validator.validate(properties);

        assertFalse(violations.isEmpty());
        assertTrue(violations.stream().anyMatch(v -> v.getPropertyPath().toString().contains("maxAttempts")));
    }

    @Test
    void validatesRetryMaxAttemptsMax() {
        MantoProperties properties = new MantoProperties();
        properties.getRetry().setMaxAttempts(101);

        var violations = validator.validate(properties);

        assertFalse(violations.isEmpty());
        assertTrue(violations.stream().anyMatch(v -> v.getPropertyPath().toString().contains("maxAttempts")));
    }

    @Test
    void validatesRetryBackoffMultiplierMin() {
        MantoProperties properties = new MantoProperties();
        properties.getRetry().getBackoff().setMultiplier(0.5);

        var violations = validator.validate(properties);

        assertFalse(violations.isEmpty());
        assertTrue(violations.stream().anyMatch(v -> v.getPropertyPath().toString().contains("multiplier")));
    }

    @Test
    void validatesRetryBackoffMultiplierMax() {
        MantoProperties properties = new MantoProperties();
        properties.getRetry().getBackoff().setMultiplier(11.0);

        var violations = validator.validate(properties);

        assertFalse(violations.isEmpty());
        assertTrue(violations.stream().anyMatch(v -> v.getPropertyPath().toString().contains("multiplier")));
    }

    private MantoProperties bind(Map<String, Object> source) {
        StandardEnvironment environment = new StandardEnvironment();
        MutablePropertySources propertySources = environment.getPropertySources();
        propertySources.addLast(new MapPropertySource("test", source));
        Binder testBinder = Binder.get(environment);
        return testBinder.bind("manto", MantoProperties.class).orElseThrow(() ->
                new IllegalStateException("Failed to bind properties"));
    }
}