/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination.singletask;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.coordination.kafka.internal.model.PartitionState;
import io.debezium.connector.spanner.coordination.kafka.internal.model.PartitionStateEnum;
import io.debezium.connector.spanner.coordination.kafka.internal.model.TaskState;
import io.debezium.connector.spanner.coordination.kafka.internal.model.TaskSyncEvent;
import io.debezium.connector.spanner.coordination.kafka.internal.proto.SyncEventFromProtoMapper;
import io.debezium.connector.spanner.kafka.event.proto.SyncEventProtos;

/**
 * Restores this task's own prior {@link TaskState} — which partitions exist and their lifecycle
 * state, not their change-stream offsets.
 *
 * <p>Single-task mode's file only ever holds one task's state (there's only ever one task), so the
 * persisted entry is taken regardless of which taskUid it was written under.
 */
public class SingleTaskStateRestorer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleTaskStateRestorer.class);

    private SingleTaskStateRestorer() {
    }

    public static TaskState restore(String stateFile) {
        if (stateFile == null || stateFile.isBlank()) {
            return null;
        }
        Path path = Paths.get(stateFile);
        if (!Files.isRegularFile(path)) {
            return null;
        }
        try (InputStream in = Files.newInputStream(path)) {
            TaskSyncEvent restored = SyncEventFromProtoMapper.mapFromProto(SyncEventProtos.SyncEvent.parseFrom(in));
            TaskState taskState = restored.getTaskStates().values().stream().findFirst().orElse(null);
            if (taskState == null) {
                LOGGER.info("Single-task coordination: state file {} has no persisted task state, starting fresh", stateFile);
                return null;
            }
            LOGGER.info("Single-task coordination: restored {} partition(s) from {}", taskState.getPartitions().size(), stateFile);
            return resumeInFlightPartitions(taskState);
        }
        catch (IOException e) {
            LOGGER.warn("Single-task coordination: failed to restore task state from {}, starting fresh instead", stateFile, e);
            return null;
        }
    }

    /**
     * Partitions persisted mid-stream ({@code SCHEDULED}/{@code RUNNING}) reflect a live thread
     * that owned them in the previous process.
     */
    private static TaskState resumeInFlightPartitions(TaskState taskState) {
        List<PartitionState> partitions = taskState.getPartitions().stream()
                .map(partitionState -> {
                    if (partitionState.getState() == PartitionStateEnum.SCHEDULED
                            || partitionState.getState() == PartitionStateEnum.RUNNING) {
                        return partitionState.toBuilder().state(PartitionStateEnum.READY_FOR_STREAMING).build();
                    }
                    return partitionState;
                })
                .collect(Collectors.toList());
        return taskState.toBuilder().partitions(partitions).build();
    }
}
