/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination;

/**
 * Bundles one mutually consistent set of the task-scoped partition coordination SPI components for a
 * given coordination mode. All four accessors return the same instances for the lifetime of the
 * coordinator.
 */
public interface TaskCoordinator {

    /**
     * Component used to publish this task's partition-ownership state so peers can observe it.
     */
    TaskStatePublisher statePublisher();

    /**
     * Component used to receive partition-ownership state published by peers.
     */
    TaskStateSubscriber stateSubscriber();

    /**
     * Component used to determine leadership and react to rebalances.
     */
    LeaderElector leaderElector();

    /**
     * Component used to determine the currently active set of tasks.
     */
    MembershipProvider membershipProvider();
}
