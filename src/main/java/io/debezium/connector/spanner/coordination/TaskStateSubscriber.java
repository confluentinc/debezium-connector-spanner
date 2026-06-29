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
 * SPI for receiving task's partition-ownership state
 */
public interface TaskStateSubscriber {

    void subscribe(BlockingBiConsumer<TaskSyncEvent, SyncEventMetadata> eventConsumer);

    void start() throws InterruptedException;

    void shutdown();
}
