/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.brokerless;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.coordination.LeaderElector;
import io.debezium.connector.spanner.coordination.MembershipProvider;
import io.debezium.connector.spanner.coordination.TaskCoordinator;
import io.debezium.connector.spanner.coordination.TaskStatePublisher;
import io.debezium.connector.spanner.coordination.TaskStateSubscriber;

/**
 * Broker-less implementation of the partition-coordination SPI
 */
public class BrokerlessTaskCoordinator implements TaskCoordinator {
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerlessTaskCoordinator.class);

    private final TaskStatePublisher statePublisher = new NoOpTaskStatePublisher();
    private final TaskStateSubscriber stateSubscriber = new BrokerlessTaskStateSubscriber();
    private final LeaderElector leaderElector;
    private final MembershipProvider membershipProvider;

    public BrokerlessTaskCoordinator(String taskUid) {
        final String consumerId = "broker-less-" + taskUid;
        this.leaderElector = new SelfLeaderElector(consumerId);
        this.membershipProvider = new SoleMemberProvider(consumerId);
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
