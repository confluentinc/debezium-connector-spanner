/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination;

import io.debezium.connector.spanner.kafka.internal.model.RebalanceEventMetadata;
import io.debezium.function.BlockingConsumer;

/**
 * SPI for determining leadership and reacting to membership rebalances. Implementations invoke a
 * registered callback with the outcome of each rebalance the task participates in.
 */
public interface LeaderElector {

    /**
     * Registers the callback invoked with each rebalance's outcome which includes
     * this task's member id, the rebalance generation, and whether this task is leader.
     */
    void listen(BlockingConsumer<RebalanceEventMetadata> action);

    /**
     * Stops listening for rebalances and releases held resources.
     */
    void shutdown();
}
