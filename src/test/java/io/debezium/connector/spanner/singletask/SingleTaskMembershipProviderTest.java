/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.singletask;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;

import org.junit.jupiter.api.Test;

class SingleTaskMembershipProviderTest {

    @Test
    void activeMembersIsOnlyItself() {
        SingleTaskMembershipProvider provider = new SingleTaskMembershipProvider("consumer-1");

        assertEquals(Set.of("consumer-1"), provider.getActiveMembers());
    }
}
