/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination;

/**
 * Provisions whatever shared infrastructure the coordination implementation needs, once, at
 * connector startup.
 *
 * <p>The default implementation ({@code KafkaInternalTopicAdminService}) creates the internal sync
 * and rebalancing topics. A single-task implementation ({@code NoOpCoordinationProvisioner}) is a
 * no-op (no broker, nothing to create).
 */
public interface CoordinationProvisioner {

    /**
     * Ensures the coordination backend is ready for tasks to start. Idempotent.
     */
    void ensureReady();
}
