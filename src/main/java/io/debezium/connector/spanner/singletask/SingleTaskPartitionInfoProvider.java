/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.singletask;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.debezium.connector.spanner.coordination.PartitionInfoProvider;

/**
 * Partition info provider for single-task implementation. There is no Kafka broker to describe
 * topics against, so the requested number of partitions (default 1) is returned directly.
 */
public class SingleTaskPartitionInfoProvider implements PartitionInfoProvider {

    @Override
    public Collection<Integer> getPartitions(String topicName, Optional<Integer> numPartitions) {
        return IntStream.range(0, numPartitions.orElse(1))
                .boxed()
                .collect(Collectors.toSet());
    }
}