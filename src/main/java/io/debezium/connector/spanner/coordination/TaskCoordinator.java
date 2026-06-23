/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination;

/**
 * Bundles the task-scoped components of the partition-coordination SPI: state publish/subscribe,
 * leader election, and membership. Selected per connector config (Kafka vs. single-task) by
 * {@link TaskCoordinatorFactory}.
 *
 * <p>This makes explicit the contract the Spanner connector previously satisfied implicitly against
 * Kafka, so the same connector can run broker-less (single task) or against alternative backends
 * without changing the partition-lifecycle code, the leader logic, or the change-stream readers.
 */
public interface TaskCoordinator {

    TaskStatePublisher statePublisher();

    TaskStateSubscriber stateSubscriber();

    LeaderElector leaderElector();

    MembershipProvider membershipProvider();
}
