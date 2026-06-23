/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination;

import io.debezium.connector.spanner.function.BlockingBiConsumer;
import io.debezium.connector.spanner.kafka.internal.model.SyncEventMetadata;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;

/**
 * Receives partition-ownership state published by tasks (including this one).
 *
 * <p>This is the consuming half of the connector's implicit partition-coordination contract. The
 * default implementation ({@code TaskSyncEventListener}) reads from a Kafka sync topic; a
 * single-task implementation has no external state to consume and instead drives initialization
 * directly on {@link #start()} by delivering a {@code (null, canInitiateRebalancing=true)} signal.
 */
public interface TaskStateSubscriber {

    /**
     * Registers a consumer to receive {@code (event, metadata)} updates.
     */
    void subscribe(BlockingBiConsumer<TaskSyncEvent, SyncEventMetadata> eventConsumer);

    /**
     * Begins delivering state updates. Must drive the connector past initialization
     * (i.e. emit the {@code canInitiateRebalancing} signal) before returning or shortly after.
     */
    void start() throws InterruptedException;

    /**
     * Stops delivering state updates and releases resources.
     */
    void shutdown();
}
