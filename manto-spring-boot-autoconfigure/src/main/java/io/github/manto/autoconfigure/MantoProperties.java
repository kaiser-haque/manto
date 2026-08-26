package io.github.manto.autoconfigure;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Configuration properties for Manto framework.
 *
 * <p>Prefix: {@code manto}</p>
 */
@ConfigurationProperties(prefix = "manto")
@Validated
public class MantoProperties {

    @Valid
    private Kafka kafka = new Kafka();

    @Valid
    private Retry retry = new Retry();

    @Valid
    private Dlt dlt = new Dlt();

    @Valid
    private Idempotency idempotency = new Idempotency();

    @Valid
    private Observability observability = new Observability();

    public Kafka getKafka() {
        return kafka;
    }

    public void setKafka(Kafka kafka) {
        this.kafka = kafka;
    }

    public Retry getRetry() {
        return retry;
    }

    public void setRetry(Retry retry) {
        this.retry = retry;
    }

    public Dlt getDlt() {
        return dlt;
    }

    public void setDlt(Dlt dlt) {
        this.dlt = dlt;
    }

    public Idempotency getIdempotency() {
        return idempotency;
    }

    public void setIdempotency(Idempotency idempotency) {
        this.idempotency = idempotency;
    }

    public Observability getObservability() {
        return observability;
    }

    public void setObservability(Observability observability) {
        this.observability = observability;
    }

    /**
     * Kafka-specific configuration.
     */
    public static class Kafka {

        @NotBlank
        private String bootstrapServers = "localhost:9092";

        public String getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }
    }

    /**
     * Retry configuration (structure only; behavior implemented on a future day).
     */
    public static class Retry {

        private boolean enabled = true;

        @Min(1)
        @Max(100)
        private int maxAttempts = 3;

        @Valid
        private Backoff backoff = new Backoff();

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getMaxAttempts() {
            return maxAttempts;
        }

        public void setMaxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        public Backoff getBackoff() {
            return backoff;
        }

        public void setBackoff(Backoff backoff) {
            this.backoff = backoff;
        }

        /**
         * Backoff configuration for retries.
         */
        public static class Backoff {

            @NotNull
            private Duration initialDelay = Duration.ofMillis(1000);

            @DecimalMin("1.0")
            @DecimalMax("10.0")
            private double multiplier = 2.0;

            @NotNull
            private Duration maxDelay = Duration.ofMillis(30000);

            public Duration getInitialDelay() {
                return initialDelay;
            }

            public void setInitialDelay(Duration initialDelay) {
                this.initialDelay = initialDelay;
            }

            public double getMultiplier() {
                return multiplier;
            }

            public void setMultiplier(double multiplier) {
                this.multiplier = multiplier;
            }

            public Duration getMaxDelay() {
                return maxDelay;
            }

            public void setMaxDelay(Duration maxDelay) {
                this.maxDelay = maxDelay;
            }
        }
    }

    /**
     * Dead Letter Topic configuration.
     */
    public static class Dlt {

        private boolean enabled = false;

        private String topicSuffix = ".DLT";

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getTopicSuffix() {
            return topicSuffix;
        }

        public void setTopicSuffix(String topicSuffix) {
            this.topicSuffix = topicSuffix;
        }
    }

    /**
     * Idempotency configuration.
     */
    public static class Idempotency {

        private boolean enabled = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    /**
     * Observability configuration.
     */
    public static class Observability {

        private boolean enabled = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }
}