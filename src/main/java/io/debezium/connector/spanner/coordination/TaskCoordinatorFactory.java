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
import io.debezium.connector.spanner.kafka.KafkaAdminClientFactory;
import io.debezium.connector.spanner.kafka.KafkaPartitionInfoProvider;
import io.debezium.connector.spanner.kafka.KafkaTaskCoordinator;
import io.debezium.connector.spanner.kafka.internal.KafkaInternalTopicAdminService;
import io.debezium.connector.spanner.singletask.NoOpCoordinationProvisioner;
import io.debezium.connector.spanner.singletask.SingleTaskCoordinator;
import io.debezium.connector.spanner.singletask.SingleTaskPartitionInfoProvider;
import io.debezium.connector.spanner.task.TaskSyncContextHolder;

/**
 * Selects partition coordination implementation based on config.
 */
public class TaskCoordinatorFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskCoordinatorFactory.class);

    private TaskCoordinatorFactory() {
    }

    public static TaskCoordinator create(SpannerConnectorConfig config,
                                         SpannerConnectorTask task,
                                         KafkaAdminClientFactory adminClientFactory,
                                         TaskSyncContextHolder taskSyncContextHolder,
                                         Consumer<RuntimeException> errorHandler) {
        if (config.isSingleTaskCoordination()) {
            LOGGER.info("Task {} - single-task partition coordination", task.getTaskUid());
            return new SingleTaskCoordinator(task.getTaskUid());
        }
        LOGGER.info("Task {} - using Kafka partition coordination", task.getTaskUid());
        return new KafkaTaskCoordinator(config, task, adminClientFactory, taskSyncContextHolder, errorHandler);
    }

    public static CoordinationProvisioner createProvisioner(SpannerConnectorConfig config) {
        if (config.isSingleTaskCoordination()) {
            return new NoOpCoordinationProvisioner();
        }
        return new KafkaInternalTopicAdminService(config);
    }

    public static PartitionInfoProvider createPartitionInfoProvider(SpannerConnectorConfig config,
                                                                    KafkaAdminClientFactory adminClientFactory) {
        if (config.isSingleTaskCoordination()) {
            return new SingleTaskPartitionInfoProvider();
        }
        return new KafkaPartitionInfoProvider(adminClientFactory.getAdminClient());
    }
}
