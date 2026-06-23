/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination;

import java.util.Set;

/**
 * Provides the set of currently active task members, used by the leader when computing a new epoch.
 *
 * <p>The default implementation ({@code KafkaConsumerAdminService}) reads the Kafka consumer-group
 * members. A single-task implementation returns a singleton containing this task's own consumer id.
 *
 * <p>Contract note: the value returned here must be consistent with the {@code consumerId} carried
 * by the {@link LeaderElector}'s events, otherwise the leader will wait indefinitely for a phantom
 * member's rebalance answer.
 */
public interface MembershipProvider {

    /**
     * @return the consumer ids of the currently active task members.
     */
    Set<String> getActiveMembers();
}
