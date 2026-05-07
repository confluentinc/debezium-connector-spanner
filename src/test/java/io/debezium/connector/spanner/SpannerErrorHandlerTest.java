/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;

import com.google.cloud.spanner.SpannerException;

import io.debezium.connector.spanner.db.model.Partition;
import io.debezium.connector.spanner.db.stream.exception.OutOfRangeChangeStreamException;

class SpannerErrorHandlerTest {

    @Test
    void testNullIsRetriable() {
        SpannerErrorHandler handler = new SpannerErrorHandler(null, null);
        assertTrue(handler.isRetriable(null));
    }

    @Test
    void testGenericExceptionIsRetriable() {
        SpannerErrorHandler handler = new SpannerErrorHandler(null, null);
        assertTrue(handler.isRetriable(new RuntimeException("transient error")));
    }

    @Test
    void testOutOfRangeExceptionIsNotRetriable() {
        SpannerErrorHandler handler = new SpannerErrorHandler(null, null);
        OutOfRangeChangeStreamException ex = new OutOfRangeChangeStreamException(
                mock(Partition.class), mock(SpannerException.class));
        assertFalse(handler.isRetriable(ex));
    }

    @Test
    void testWrappedOutOfRangeExceptionIsNotRetriable() {
        SpannerErrorHandler handler = new SpannerErrorHandler(null, null);
        OutOfRangeChangeStreamException cause = new OutOfRangeChangeStreamException(
                mock(Partition.class), mock(SpannerException.class));
        RuntimeException wrapped = new RuntimeException("wrapper", cause);
        assertFalse(handler.isRetriable(wrapped));
    }
}
