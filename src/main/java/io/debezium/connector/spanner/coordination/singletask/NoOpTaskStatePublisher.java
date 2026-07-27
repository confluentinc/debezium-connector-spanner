/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination.singletask;

import java.time.Instant;

import io.debezium.connector.spanner.coordination.TaskStatePublisher;
import io.debezium.connector.spanner.coordination.kafka.internal.model.TaskSyncEvent;

/**
 * Task state publisher for single-task implementation. There are no peer tasks to publish
 * partition-ownership state to, so {@link #send} only records a timestamp which is kept so
 * {@link #getLastTime()} still satisfies the SPI's liveness-signal contract, even though nothing
 * currently reads it in single-task mode.
 */
public class NoOpTaskStatePublisher implements TaskStatePublisher {
    private volatile Instant lastTime = Instant.now();

    @Override
    public void send(TaskSyncEvent taskSyncEvent) {
        lastTime = Instant.now();
    }

    @Override
    public void close() {
        // nothing to release
    }

    @Override
    public Instant getLastTime() {
        return lastTime;
    }
}
