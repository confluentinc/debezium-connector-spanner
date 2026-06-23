/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination.singletask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.coordination.CoordinationProvisioner;

/**
 * Broker-less coordination provisioner. With a single task there is no Kafka broker and no internal
 * sync/rebalancing topics to create, so {@link #ensureReady()} does nothing.
 */
public class NoOpCoordinationProvisioner implements CoordinationProvisioner {

    private static final Logger LOGGER = LoggerFactory.getLogger(NoOpCoordinationProvisioner.class);

    @Override
    public void ensureReady() {
        LOGGER.info("Single-task (broker-less) coordination: no infrastructure to provision");
    }
}
