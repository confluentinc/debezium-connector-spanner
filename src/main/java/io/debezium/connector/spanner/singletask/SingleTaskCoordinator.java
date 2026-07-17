/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.singletask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.coordination.LeaderElector;
import io.debezium.connector.spanner.coordination.MembershipProvider;
import io.debezium.connector.spanner.coordination.TaskCoordinator;
import io.debezium.connector.spanner.coordination.TaskStatePublisher;
import io.debezium.connector.spanner.coordination.TaskStateSubscriber;

/**
 * Single-task implementation of the partition-coordination SPI, the sole task is always leader,
 * membership is always just itself, and there are no peers to publish state to or subscribe from.
 * Bundles the no-op/self-contained single-task components instead of the Kafka-topic-backed
 * equivalents {@link io.debezium.connector.spanner.kafka.KafkaTaskCoordinator} bundles.
 */
public class SingleTaskCoordinator implements TaskCoordinator {
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleTaskCoordinator.class);

    private final TaskStatePublisher statePublisher = new NoOpTaskStatePublisher();
    private final TaskStateSubscriber stateSubscriber = new SingleTaskStateSubscriber();
    private final LeaderElector leaderElector;
    private final MembershipProvider membershipProvider;

    public SingleTaskCoordinator(String taskUid) {
        final String consumerId = "single-task-" + taskUid;
        this.leaderElector = new SingleTaskLeaderElector(consumerId);
        this.membershipProvider = new SingleTaskMembershipProvider(consumerId);
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
}
