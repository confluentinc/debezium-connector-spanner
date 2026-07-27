/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination.singletask;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.coordination.MembershipProvider;

/**
 * Membership provider for single-task implementation. Kafka mode asks the consumer-group
 * coordinator who else is alive whereas single-task mode has no group, so this always returns a
 * mutable set containing only this task's own id.
 */
public class SingleTaskMembershipProvider implements MembershipProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleTaskMembershipProvider.class);

    private final String consumerId;

    SingleTaskMembershipProvider(String consumerId) {
        this.consumerId = consumerId;
    }

    @Override
    public Set<String> getActiveMembers() {
        LOGGER.info("Single-task coordination: returning ({}) as the only active member", consumerId);
        return new HashSet<>(Set.of(consumerId));
    }
}
