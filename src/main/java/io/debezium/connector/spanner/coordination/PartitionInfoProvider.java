/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * SPI for resolving the partitions of an output topic, used when fanning out
 * low watermark stamp records.
 */
public interface PartitionInfoProvider {

    Collection<Integer> getPartitions(String topicName, Optional<Integer> numPartitions) throws ExecutionException, InterruptedException;
}
