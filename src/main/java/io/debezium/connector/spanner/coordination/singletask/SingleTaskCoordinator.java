/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination.singletask;

import io.debezium.connector.spanner.coordination.LeaderElector;
import io.debezium.connector.spanner.coordination.MembershipProvider;
import io.debezium.connector.spanner.coordination.TaskCoordinator;
import io.debezium.connector.spanner.coordination.TaskStatePublisher;
import io.debezium.connector.spanner.coordination.TaskStateSubscriber;

/**
 * Single-task implementation of the partition-coordination SPI, the sole task is always leader,
 * membership is always just itself, and there are no peers to publish state to or subscribe from.
 * Bundles the self-contained single-task components.
 */
public class SingleTaskCoordinator implements TaskCoordinator {
    private final TaskStatePublisher statePublisher;
    private final TaskStateSubscriber stateSubscriber = new SingleTaskStateSubscriber();
    private final LeaderElector leaderElector;
    private final MembershipProvider membershipProvider;

    public SingleTaskCoordinator(String taskUid, String stateFile) {
        final String consumerId = "single-task-" + taskUid;
        this.leaderElector = new SingleTaskLeaderElector(consumerId);
        this.membershipProvider = new SingleTaskMembershipProvider(consumerId);
        this.statePublisher = new SingleTaskStatePublisher(stateFile);
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
        // no resources to release
    }
}
