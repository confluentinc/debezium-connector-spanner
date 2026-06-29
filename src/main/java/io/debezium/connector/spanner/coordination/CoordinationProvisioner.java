/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination;

/**
 * Provides shared infrastructure for the coordination implementation, once at startup
 */
public interface CoordinationProvisioner {
    void ensureReady();
}
