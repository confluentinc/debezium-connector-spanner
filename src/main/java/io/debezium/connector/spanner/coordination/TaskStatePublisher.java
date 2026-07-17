/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination;

import java.time.Instant;

import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;

/**
 * SPI for publishing this task's partition-ownership state so peers can observe it.
 */
public interface TaskStatePublisher {

    /**
     * Publishes this task's current partition-ownership state.
     */
    void send(TaskSyncEvent taskSyncEvent);

    /**
     * Releases any resources held for publishing.
     */
    void close();

    /**
     * Returns the timestamp of the most recent successful publish.
     */
    Instant getLastTime();
}
