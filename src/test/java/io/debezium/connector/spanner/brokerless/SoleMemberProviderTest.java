/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.brokerless;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;

import org.junit.jupiter.api.Test;

class SoleMemberProviderTest {

    @Test
    void activeMembersIsOnlyItself() {
        SoleMemberProvider provider = new SoleMemberProvider("consumer-1");

        assertEquals(Set.of("consumer-1"), provider.getActiveMembers());
    }
}
