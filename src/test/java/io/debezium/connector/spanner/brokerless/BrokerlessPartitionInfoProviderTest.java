/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.brokerless;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;

class BrokerlessPartitionInfoProviderTest {

    private final BrokerlessPartitionInfoProvider provider = new BrokerlessPartitionInfoProvider();

    @Test
    void returnsRequestedNumberOfPartitions() {
        Collection<Integer> partitions = provider.getPartitions("some-topic", Optional.of(3));

        assertEquals(Set.of(0, 1, 2), partitions);
    }

    @Test
    void defaultsToSinglePartitionWhenNumPartitionsAbsent() {
        Collection<Integer> partitions = provider.getPartitions("some-topic", Optional.empty());

        assertEquals(Set.of(0), partitions);
    }

    @Test
    void returnsEmptyCollectionWhenZeroPartitionsRequested() {
        Collection<Integer> partitions = provider.getPartitions("some-topic", Optional.of(0));

        assertEquals(Set.of(), partitions);
    }
}
