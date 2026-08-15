package io.github.manto.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MantoHeadersTest {

    @Test
    void headerNamesMatchTheDocumentedStandard() {
        assertEquals("Manto-Event-Id", MantoHeaders.EVENT_ID);
        assertEquals("Manto-Event-Type", MantoHeaders.EVENT_TYPE);
        assertEquals("Manto-Event-Version", MantoHeaders.EVENT_VERSION);
        assertEquals("Manto-Correlation-Id", MantoHeaders.CORRELATION_ID);
        assertEquals("Manto-Source", MantoHeaders.SOURCE);
    }
}