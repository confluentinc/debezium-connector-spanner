/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination;

/**
 * Bundles the task-scoped components of the partition coordination SPI
 */
public interface TaskCoordinator {

    TaskStatePublisher statePublisher();

    TaskStateSubscriber stateSubscriber();

    LeaderElector leaderElector();

    MembershipProvider membershipProvider();
}
