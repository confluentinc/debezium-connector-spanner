/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination;

import java.util.Set;

/**
 * SPI for determining which tasks are currently part of the active coordination group.
 */
public interface MembershipProvider {

    /**
     * Returns the identifiers of all tasks currently considered active, used by the leader to
     * distinguish live tasks from ones that have left.
     */
    Set<String> getActiveMembers();
}
