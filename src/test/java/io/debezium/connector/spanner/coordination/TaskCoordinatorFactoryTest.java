/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.SpannerConnectorTask;
import io.debezium.connector.spanner.kafka.KafkaAdminClientFactory;
import io.debezium.connector.spanner.kafka.KafkaPartitionInfoProvider;
import io.debezium.connector.spanner.singletask.NoOpCoordinationProvisioner;
import io.debezium.connector.spanner.singletask.SingleTaskCoordinator;
import io.debezium.connector.spanner.singletask.SingleTaskPartitionInfoProvider;

class TaskCoordinatorFactoryTest {

    @Test
    void createReturnsSingleTaskTaskCoordinatorWhenSingleTaskCoordinationEnabled() {
        SpannerConnectorConfig config = mock(SpannerConnectorConfig.class);
        when(config.isSingleTaskCoordination()).thenReturn(true);
        SpannerConnectorTask task = mock(SpannerConnectorTask.class);
        when(task.getTaskUid()).thenReturn("task-1");

        TaskCoordinator coordinator = TaskCoordinatorFactory.create(config, task, null, null, ex -> {
        });

        assertInstanceOf(SingleTaskCoordinator.class, coordinator);
    }

    @Test
    void createProvisionerReturnsNoOpWhenSingleTaskCoordinationEnabled() {
        SpannerConnectorConfig config = mock(SpannerConnectorConfig.class);
        when(config.isSingleTaskCoordination()).thenReturn(true);

        CoordinationProvisioner provisioner = TaskCoordinatorFactory.createProvisioner(config);

        assertInstanceOf(NoOpCoordinationProvisioner.class, provisioner);
    }

    @Test
    void createPartitionInfoProviderReturnsSingleTaskImplWhenSingleTaskCoordinationEnabled() {
        SpannerConnectorConfig config = mock(SpannerConnectorConfig.class);
        when(config.isSingleTaskCoordination()).thenReturn(true);

        PartitionInfoProvider provider = TaskCoordinatorFactory.createPartitionInfoProvider(config, null);

        assertInstanceOf(SingleTaskPartitionInfoProvider.class, provider);
    }

    @Test
    void createPartitionInfoProviderReturnsKafkaImplWhenSingleTaskCoordinationDisabled() {
        SpannerConnectorConfig config = mock(SpannerConnectorConfig.class);
        when(config.isSingleTaskCoordination()).thenReturn(false);
        KafkaAdminClientFactory adminClientFactory = mock(KafkaAdminClientFactory.class);

        PartitionInfoProvider provider = TaskCoordinatorFactory.createPartitionInfoProvider(config, adminClientFactory);

        assertInstanceOf(KafkaPartitionInfoProvider.class, provider);
    }
}
