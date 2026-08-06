/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.context.offset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.google.cloud.Timestamp;

import io.debezium.connector.spanner.db.model.StreamEventMetadata;

class PartitionOffsetHolderTest {

    private static Stream<Arguments> offsetProvider() {
        return Stream.of(
                Arguments.of(Timestamp.ofTimeMicroseconds(1L), 3,
                        Timestamp.ofTimeMicroseconds(1L).toString()),
                Arguments.of(Timestamp.ofTimeMicroseconds(0L), 3,
                        Timestamp.ofTimeMicroseconds(0L).toSqlTimestamp().toInstant().toString()));
    }

    @ParameterizedTest
    @MethodSource("offsetProvider")
    void testGetOffset(Timestamp timestamp, int expectedSize, String expectedOffset) {
        PartitionOffset partitionOffsetHolder = new PartitionOffset(timestamp, StreamEventMetadata.newBuilder().withPartitionStartTimestamp(Timestamp.now()).build());
        Map<String, String> actualOffset = partitionOffsetHolder.getOffset();

        assertEquals(expectedSize, actualOffset.size());
        assertEquals(expectedOffset, actualOffset.get("offset"));
    }

    @Test
    void testGetOffsetIsNullForRemovedPartitionTombstone() {
        // Finished-partition removal uses the no-arg constructor (null offset). getOffset()
        // must return null (not an empty map) so Kafka Connect writes a tombstone and removes
        // the partition token from the offset store, instead of leaving it behind as "{}" and
        // growing the offset store without bound.
        assertNull(new PartitionOffset().getOffset());
    }

    @Test
    void testUpdatePartitionOffset() {
        PartitionOffset partitionOffsetHolder = new PartitionOffset(Timestamp.ofTimeMicroseconds(1L),
                StreamEventMetadata.newBuilder().withPartitionStartTimestamp(Timestamp.now()).build());

        assertEquals(3, partitionOffsetHolder.getOffset().size());
    }

    @Test
    void testExtractOffset() {
        assertNull(PartitionOffset.extractOffset(new HashMap<>()));

        HashMap<String, String> stringStringMap = new HashMap<>();
        Timestamp timestamp = Timestamp.ofTimeMicroseconds(1L);
        stringStringMap.put("offset", timestamp.toSqlTimestamp().toInstant().toString());
        assertEquals(timestamp, PartitionOffset.extractOffset(stringStringMap));
    }
}
