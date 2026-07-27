/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination.kafka;

import java.util.function.Consumer;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.SpannerConnectorTask;
import io.debezium.connector.spanner.coordination.LeaderElector;
import io.debezium.connector.spanner.coordination.MembershipProvider;
import io.debezium.connector.spanner.coordination.TaskCoordinator;
import io.debezium.connector.spanner.coordination.TaskStatePublisher;
import io.debezium.connector.spanner.coordination.TaskStateSubscriber;
import io.debezium.connector.spanner.coordination.kafka.internal.KafkaConsumerAdminService;
import io.debezium.connector.spanner.coordination.kafka.internal.ProducerFactory;
import io.debezium.connector.spanner.coordination.kafka.internal.RebalancingConsumerFactory;
import io.debezium.connector.spanner.coordination.kafka.internal.RebalancingEventListener;
import io.debezium.connector.spanner.coordination.kafka.internal.SyncEventConsumerFactory;
import io.debezium.connector.spanner.coordination.kafka.internal.TaskSyncEventListener;
import io.debezium.connector.spanner.coordination.kafka.internal.TaskSyncPublisher;
import io.debezium.connector.spanner.task.TaskSyncContextHolder;

/**
 * Existing Kafka-backed implementation of the partition coordination SPI
 */
public class KafkaTaskCoordinator implements TaskCoordinator {
    private final TaskStatePublisher statePublisher;
    private final TaskStateSubscriber stateSubscriber;
    private final LeaderElector leaderElector;
    private final MembershipProvider membershipProvider;
    private final KafkaAdminClientFactory adminClientFactory;

    public KafkaTaskCoordinator(SpannerConnectorConfig config,
                                SpannerConnectorTask task,
                                TaskSyncContextHolder taskSyncContextHolder,
                                Consumer<RuntimeException> errorHandler) {

        final String taskSyncTopic = config.taskSyncTopic();

        ProducerFactory<String, byte[]> producerFactory = new ProducerFactory<>(config);
        this.statePublisher = new TaskSyncPublisher(task.getTaskUid(), taskSyncTopic,
                config.syncEventPublisherWaitingTimeout(), producerFactory, taskSyncContextHolder, errorHandler);

        SyncEventConsumerFactory<String, byte[]> syncEventConsumerFactory = new SyncEventConsumerFactory<>(config, false);
        this.stateSubscriber = new TaskSyncEventListener(task.getTaskUid(), taskSyncTopic, syncEventConsumerFactory, true, errorHandler);

        RebalancingConsumerFactory<?, ?> rebalancingConsumerFactory = new RebalancingConsumerFactory<>(config);
        this.leaderElector = new RebalancingEventListener(task, config.getConnectorName(), config.rebalancingTopic(),
                config.rebalancingTaskWaitingTimeout(), rebalancingConsumerFactory, errorHandler);

        this.adminClientFactory = new KafkaAdminClientFactory(config);
        this.membershipProvider = new KafkaConsumerAdminService(adminClientFactory.getAdminClient(), config.getConnectorName());
    }

    @Override
    public TaskStatePublisher statePublisher() {
        return statePublisher;
    }

    @Override
    public TaskStateSubscriber stateSubscriber() {
        return stateSubscriber;
    }

    @Override
    public LeaderElector leaderElector() {
        return leaderElector;
    }

    @Override
    public MembershipProvider membershipProvider() {
        return membershipProvider;
    }

    @Override
    public void close() {
        adminClientFactory.close();
    }
}
