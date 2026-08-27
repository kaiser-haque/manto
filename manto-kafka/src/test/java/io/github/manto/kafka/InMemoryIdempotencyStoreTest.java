package io.github.manto.kafka;

import io.github.manto.core.IdempotencyStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Execution(ExecutionMode.CONCURRENT)
class InMemoryIdempotencyStoreTest {

    @Test
    void shouldReturnFalseForUnprocessedEvent() {
        IdempotencyStore store = new InMemoryIdempotencyStore();
        assertFalse(store.isProcessed("event-1"));
    }

    @Test
    void shouldReturnTrueAfterMarkingProcessed() {
        IdempotencyStore store = new InMemoryIdempotencyStore();
        store.markProcessed("event-1");
        assertTrue(store.isProcessed("event-1"));
    }

    @Test
    void shouldNotAffectOtherEvents() {
        IdempotencyStore store = new InMemoryIdempotencyStore();
        store.markProcessed("event-1");
        assertFalse(store.isProcessed("event-2"));
    }

    @Test
    void shouldHandleMultipleEvents() {
        IdempotencyStore store = new InMemoryIdempotencyStore();
        store.markProcessed("event-1");
        store.markProcessed("event-2");
        store.markProcessed("event-3");
        assertTrue(store.isProcessed("event-1"));
        assertTrue(store.isProcessed("event-2"));
        assertTrue(store.isProcessed("event-3"));
        assertFalse(store.isProcessed("event-4"));
    }

    @Test
    void shouldBeIdempotentOnMarkProcessed() {
        IdempotencyStore store = new InMemoryIdempotencyStore();
        store.markProcessed("event-1");
        store.markProcessed("event-1");
        assertTrue(store.isProcessed("event-1"));
    }

    @Test
    void shouldHandleEmptyEventId() {
        IdempotencyStore store = new InMemoryIdempotencyStore();
        assertFalse(store.isProcessed(""));
        store.markProcessed("");
        assertTrue(store.isProcessed(""));
    }

    @RepeatedTest(10)
    void shouldHandleConcurrentAccess() throws InterruptedException {
        IdempotencyStore store = new InMemoryIdempotencyStore();
        int threadCount = 10;
        int eventsPerThread = 100;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        AtomicInteger processedCount = new AtomicInteger(0);

        try (ExecutorService executor = Executors.newFixedThreadPool(threadCount)) {
            for (int t = 0; t < threadCount; t++) {
                final int threadIndex = t;
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        for (int i = 0; i < eventsPerThread; i++) {
                            String eventId = "event-" + threadIndex + "-" + i;
                            if (!store.isProcessed(eventId)) {
                                store.markProcessed(eventId);
                                processedCount.incrementAndGet();
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        endLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            endLatch.await();
        }

        assertEquals(threadCount * eventsPerThread, processedCount.get());
    }

    @Test
    void shouldHandleConcurrentMarkAndCheck() throws InterruptedException {
        IdempotencyStore store = new InMemoryIdempotencyStore();
        String eventId = "concurrent-event";
        int threadCount = 50;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        AtomicInteger checkCount = new AtomicInteger(0);
        AtomicInteger markCount = new AtomicInteger(0);

        try (ExecutorService executor = Executors.newFixedThreadPool(threadCount)) {
            for (int i = 0; i < threadCount; i++) {
                final boolean shouldMark = i % 2 == 0;
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        if (shouldMark) {
                            store.markProcessed(eventId);
                            markCount.incrementAndGet();
                        } else {
                            if (store.isProcessed(eventId)) {
                                checkCount.incrementAndGet();
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        endLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            endLatch.await();
        }

        assertTrue(store.isProcessed(eventId));
        assertEquals(threadCount / 2, markCount.get());
    }
}