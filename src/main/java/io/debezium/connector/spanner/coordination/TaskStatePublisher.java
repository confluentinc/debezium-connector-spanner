/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination;

import java.time.Instant;

import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;

/**
 * SPI for publishing task's partition-ownership state
 */
public interface TaskStatePublisher {

    void send(TaskSyncEvent taskSyncEvent);

    void close();

    Instant getLastTime();
}
