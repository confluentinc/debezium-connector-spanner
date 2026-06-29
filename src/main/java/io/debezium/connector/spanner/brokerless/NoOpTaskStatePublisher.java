/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.brokerless;

import java.time.Instant;

import io.debezium.connector.spanner.coordination.TaskStatePublisher;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;

/**
 * Task state publisher for broker-less implementation
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
