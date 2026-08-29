package io.github.manto.kafka;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class CorrelationIdContextTest {

    @AfterEach
    void cleanup() {
        CorrelationIdContext.clear();
    }

    @Test
    void returnsNullWhenNotSet() {
        assertNull(CorrelationIdContext.get());
    }

    @Test
    void storesAndRetrievesCorrelationId() {
        CorrelationIdContext.set("test-correlation-id");
        assertEquals("test-correlation-id", CorrelationIdContext.get());
    }

    @Test
    void clearRemovesCorrelationId() {
        CorrelationIdContext.set("test-correlation-id");
        CorrelationIdContext.clear();
        assertNull(CorrelationIdContext.get());
    }

    @Test
    void setReplacesExistingValue() {
        CorrelationIdContext.set("first-id");
        CorrelationIdContext.set("second-id");
        assertEquals("second-id", CorrelationIdContext.get());
    }

    @Test
    void isThreadLocalIsolated() throws Exception {
        String mainThreadId = UUID.randomUUID().toString();
        CorrelationIdContext.set(mainThreadId);

        AtomicReference<String> otherThreadValue = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        Thread otherThread = new Thread(() -> {
            otherThreadValue.set(CorrelationIdContext.get());
            CorrelationIdContext.set("other-thread-id");
            latch.countDown();
        });
        otherThread.start();
        latch.await(5, TimeUnit.SECONDS);

        assertEquals(mainThreadId, CorrelationIdContext.get());
        assertNull(otherThreadValue.get());

        CorrelationIdContext.clear();
    }

    @Test
    void clearOnOneThreadDoesNotAffectAnother() throws Exception {
        CorrelationIdContext.set("main-thread-id");

        CountDownLatch setLatch = new CountDownLatch(1);
        CountDownLatch clearLatch = new CountDownLatch(1);

        Thread otherThread = new Thread(() -> {
            CorrelationIdContext.set("other-thread-id");
            setLatch.countDown();
            try {
                clearLatch.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        otherThread.start();
        setLatch.await(5, TimeUnit.SECONDS);

        CorrelationIdContext.clear();
        assertNull(CorrelationIdContext.get());

        clearLatch.countDown();
        otherThread.join(5000);
    }
}
