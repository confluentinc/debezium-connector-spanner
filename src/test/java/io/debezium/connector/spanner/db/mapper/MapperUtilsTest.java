/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.db.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.node.MissingNode;

import io.debezium.connector.spanner.db.mapper.parser.ParseException;

class MapperUtilsTest {

    @Test
    void testGetJsonNode() {
        assertThrows(ParseException.class, () -> MapperUtils.getJsonNode("Json"));
        assertEquals(0, MapperUtils.getJsonNode(null).size());
        assertEquals("42", MapperUtils.getJsonNode("42").toPrettyString());
        assertTrue(MapperUtils.getJsonNode("") instanceof MissingNode);
        assertThrows(ParseException.class, () -> MapperUtils.getJsonNode("42Json"));
        assertEquals("4242", MapperUtils.getJsonNode("4242").toPrettyString());
    }

    @Test
    void getJsonNodeDoesNotLeakRawJsonWhenParseFails() {
        // Synthetic stand-in for a customer column value; the raw JSON here is change-record data
        // (keys / old_values / new_values) and must never surface in the thrown exception, which
        // propagates uncaught to the forwarded SpannerChangeStream / SpannerErrorHandler loggers.
        final String canary = "SENSITIVE_CANARY_9f3a2b7c";
        final String malformedJson = "{" + canary + "}";

        ParseException ex = assertThrows(ParseException.class, () -> MapperUtils.getJsonNode(malformedJson));

        // Walk the whole cause chain: the raw JSON must not appear in any message.
        for (Throwable t = ex; t != null; t = t.getCause()) {
            assertFalse(t.getMessage() != null && t.getMessage().contains(canary),
                    "raw JSON leaked via " + t.getClass().getName() + ": " + t.getMessage());
        }
        // Fixed, non-sensitive message; the value-bearing Jackson cause is dropped.
        assertEquals("Error parse string: change record value", ex.getMessage());
        assertNull(ex.getCause());
    }
}
