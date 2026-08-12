/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination.singletask;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.coordination.TaskStatePublisher;
import io.debezium.connector.spanner.coordination.kafka.internal.model.TaskSyncEvent;
import io.debezium.connector.spanner.coordination.kafka.internal.proto.SyncEventToProtoMapper;

/**
 * Task state publisher for single-task implementation. There are no peer tasks to publish
 * partition-ownership state to, so {@link #send} always records a timestamp (kept so
 * {@link #getLastTime()} still satisfies the SPI's liveness-signal contract) and, when
 * {@code stateFile} is configured, it persists the event to that local file,
 * so {@link SingleTaskStateRestorer} can restore it after a restart.
 */
public class SingleTaskStatePublisher implements TaskStatePublisher {
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleTaskStatePublisher.class);

    private volatile Instant lastTime = Instant.now();
    private final Path stateFile;

    public SingleTaskStatePublisher(String stateFile) {
        this.stateFile = stateFile == null || stateFile.isBlank() ? null : Paths.get(stateFile);
    }

    @Override
    public void send(TaskSyncEvent taskSyncEvent) {
        lastTime = Instant.now();
        if (stateFile != null) {
            persist(taskSyncEvent);
        }
    }

    private void persist(TaskSyncEvent taskSyncEvent) {
        Path tmpFile = stateFile.resolveSibling(stateFile.getFileName() + ".tmp");
        try {
            try (OutputStream out = Files.newOutputStream(tmpFile)) {
                SyncEventToProtoMapper.mapToProto(taskSyncEvent).writeTo(out);
            }
            // Atomic move so a concurrent/crashed write can never leave a half-written state file
            // for SingleTaskStateRestorer to read back on the next restart.
            Files.move(tmpFile, stateFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            LOGGER.info("Single-task coordination: persisted task sync state to {}", stateFile);
        }
        catch (IOException e) {
            LOGGER.warn("Single-task coordination: failed to persist task sync state to {}", stateFile, e);
        }
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
