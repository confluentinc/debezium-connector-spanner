/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination;

/**
 * SPI for provisioning shared infrastructure the coordination implementation depends on.
 */
public interface CoordinationProvisioner {

    /**
     * Creates or validates required infrastructure once at startup, before any other coordination
     * component is used.
     */
    void ensureReady();
}
