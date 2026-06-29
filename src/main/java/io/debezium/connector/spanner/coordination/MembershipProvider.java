/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination;

import java.util.Set;

/**
 * SPI for providing the set of currently active task members
 */
public interface MembershipProvider {

    Set<String> getActiveMembers();
}
