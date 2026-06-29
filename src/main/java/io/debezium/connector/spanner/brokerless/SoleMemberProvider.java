/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.brokerless;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.coordination.MembershipProvider;

/**
 * Membership provider for broker-less implementation
 */
public class SoleMemberProvider implements MembershipProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(SoleMemberProvider.class);

    private final String consumerId;

    SoleMemberProvider(String consumerId) {
        this.consumerId = consumerId;
    }

    @Override
    public Set<String> getActiveMembers() {
        return new HashSet<>(Set.of(consumerId));
    }
}
