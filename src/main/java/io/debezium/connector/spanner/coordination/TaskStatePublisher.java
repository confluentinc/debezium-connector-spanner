/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination;

import java.time.Instant;

import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;

/**
 * Publishes this task's partition-ownership state so other tasks can observe it.
 *
 * <p>This is one half of the connector's implicit partition-coordination contract. The default
 * implementation ({@code TaskSyncPublisher}) writes to a Kafka sync topic; a single-task
 * implementation needs no broker because there are no other tasks to inform.
 */
public interface TaskStatePublisher {

    /**
     * Publishes a task-state snapshot / control message.
     */
    void send(TaskSyncEvent taskSyncEvent);

    /**
     * Releases any resources held by the publisher.
     */
    void close();

    /**
     * @return the time of the last successful publish, or {@code null} if nothing published yet.
     */
    Instant getLastTime();
}
