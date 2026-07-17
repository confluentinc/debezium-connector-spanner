/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.singletask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.coordination.CoordinationProvisioner;

/**
 * NoOp coordination provisioner for single-task implementation. Kafka mode's provisioner creates
 * and verifies the rebalance and {@code _connector_sync} topics before anything else starts whereas
 * single-task mode has no such shared infrastructure to provision, so this is a genuine no-op.
 */
public class NoOpCoordinationProvisioner implements CoordinationProvisioner {
    private static final Logger LOGGER = LoggerFactory.getLogger(NoOpCoordinationProvisioner.class);

    @Override
    public void ensureReady() {
        LOGGER.info("Single-task coordination: no infrastructure to provision");
    }
}
