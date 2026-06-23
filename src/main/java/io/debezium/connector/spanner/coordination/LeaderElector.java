/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination;

import io.debezium.connector.spanner.kafka.internal.model.RebalanceEventMetadata;
import io.debezium.function.BlockingConsumer;

/**
 * Elects a leader among the connector's tasks and notifies on membership changes.
 *
 * <p>The default implementation ({@code RebalancingEventListener}) uses the Kafka consumer-group
 * rebalance protocol (the task assigned partition 0 of the rebalancing topic is the leader). A
 * single-task implementation elects itself once at startup by emitting a single
 * {@code RebalanceEventMetadata(consumerId, generationId=0, leader=true)} event.
 */
public interface LeaderElector {

    /**
     * Starts election and invokes {@code action} on each (re)election / membership change.
     */
    void listen(BlockingConsumer<RebalanceEventMetadata> action);

    /**
     * Stops election and releases resources.
     */
    void shutdown();
}
