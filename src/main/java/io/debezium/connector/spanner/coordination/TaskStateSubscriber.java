/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination;

import io.debezium.connector.spanner.coordination.kafka.internal.model.SyncEventMetadata;
import io.debezium.connector.spanner.coordination.kafka.internal.model.TaskSyncEvent;
import io.debezium.connector.spanner.function.BlockingBiConsumer;

/**
 * SPI for receiving partition-ownership state published by peers.
 */
public interface TaskStateSubscriber {

    /**
     * Registers a callback to be invoked with each state event learned from peers, along with
     * metadata about that delivery.
     */
    void subscribe(BlockingBiConsumer<TaskSyncEvent, SyncEventMetadata> eventConsumer);

    /**
     * Begins delivering known peer's state to registered subscribers, signaling once enough
     * history has been consumed to safely begin rebalancing.
     */
    void start() throws InterruptedException;

    /**
     * Stops delivering state events and releases resources.
     */
    void shutdown();
}
