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
import io.debezium.connector.spanner.brokerless.BrokerlessPartitionInfoProvider;
import io.debezium.connector.spanner.brokerless.BrokerlessTaskCoordinator;
import io.debezium.connector.spanner.brokerless.NoOpCoordinationProvisioner;
import io.debezium.connector.spanner.kafka.KafkaAdminClientFactory;
import io.debezium.connector.spanner.kafka.KafkaPartitionInfoProvider;

class TaskCoordinatorFactoryTest {

    @Test
    void createReturnsBrokerlessTaskCoordinatorWhenBrokerlessCoordinationEnabled() {
        SpannerConnectorConfig config = mock(SpannerConnectorConfig.class);
        when(config.isBrokerlessCoordination()).thenReturn(true);
        SpannerConnectorTask task = mock(SpannerConnectorTask.class);
        when(task.getTaskUid()).thenReturn("task-1");

        TaskCoordinator coordinator = TaskCoordinatorFactory.create(config, task, null, null, ex -> {
        });

        assertInstanceOf(BrokerlessTaskCoordinator.class, coordinator);
    }

    @Test
    void createProvisionerReturnsNoOpWhenBrokerlessCoordinationEnabled() {
        SpannerConnectorConfig config = mock(SpannerConnectorConfig.class);
        when(config.isBrokerlessCoordination()).thenReturn(true);

        CoordinationProvisioner provisioner = TaskCoordinatorFactory.createProvisioner(config);

        assertInstanceOf(NoOpCoordinationProvisioner.class, provisioner);
    }

    @Test
    void createPartitionInfoProviderReturnsBrokerlessImplWhenBrokerlessCoordinationEnabled() {
        SpannerConnectorConfig config = mock(SpannerConnectorConfig.class);
        when(config.isBrokerlessCoordination()).thenReturn(true);

        PartitionInfoProvider provider = TaskCoordinatorFactory.createPartitionInfoProvider(config, null);

        assertInstanceOf(BrokerlessPartitionInfoProvider.class, provider);
    }

    @Test
    void createPartitionInfoProviderReturnsKafkaImplWhenBrokerlessCoordinationDisabled() {
        SpannerConnectorConfig config = mock(SpannerConnectorConfig.class);
        when(config.isBrokerlessCoordination()).thenReturn(false);
        KafkaAdminClientFactory adminClientFactory = mock(KafkaAdminClientFactory.class);

        PartitionInfoProvider provider = TaskCoordinatorFactory.createPartitionInfoProvider(config, adminClientFactory);

        assertInstanceOf(KafkaPartitionInfoProvider.class, provider);
    }
}
