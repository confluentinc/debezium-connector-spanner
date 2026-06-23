/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination;

import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.SpannerConnectorTask;
import io.debezium.connector.spanner.coordination.kafka.KafkaTaskCoordinator;
import io.debezium.connector.spanner.coordination.singletask.NoOpCoordinationProvisioner;
import io.debezium.connector.spanner.coordination.singletask.SingleTaskCoordinator;
import io.debezium.connector.spanner.kafka.KafkaAdminClientFactory;
import io.debezium.connector.spanner.kafka.internal.KafkaInternalTopicAdminService;
import io.debezium.connector.spanner.task.TaskSyncContextHolder;

/**
 * Selects the partition-coordination implementation based on
 * {@code connector.spanner.coordination.mode}: the default Kafka-backed coordinator, or the
 * broker-less single-task coordinator.
 */
public final class TaskCoordinatorFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskCoordinatorFactory.class);

    private TaskCoordinatorFactory() {
    }

    /**
     * Builds the task-scoped coordination components (state publish/subscribe, leader election,
     * membership).
     *
     * @param adminClientFactory the Kafka admin client factory; used only in Kafka mode and may be
     *            {@code null} in single-task mode.
     */
    public static TaskCoordinator create(SpannerConnectorConfig config,
                                         SpannerConnectorTask task,
                                         KafkaAdminClientFactory adminClientFactory,
                                         TaskSyncContextHolder taskSyncContextHolder,
                                         Consumer<RuntimeException> errorHandler) {
        if (config.isSingleTaskCoordination()) {
            LOGGER.info("Task {} - using single-task (broker-less) partition coordination", task.getTaskUid());
            return new SingleTaskCoordinator(task.getTaskUid());
        }
        LOGGER.info("Task {} - using Kafka partition coordination", task.getTaskUid());
        return new KafkaTaskCoordinator(config, task, adminClientFactory, taskSyncContextHolder, errorHandler);
    }

    /**
     * Builds the connector-scoped coordination provisioner (run once at startup).
     */
    public static CoordinationProvisioner createProvisioner(SpannerConnectorConfig config) {
        if (config.isSingleTaskCoordination()) {
            return new NoOpCoordinationProvisioner();
        }
        return new KafkaInternalTopicAdminService(config);
    }
}
