/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.brokerless;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.coordination.CoordinationProvisioner;

public class NoOpCoordinationProvisioner implements CoordinationProvisioner {
    private static final Logger LOGGER = LoggerFactory.getLogger(NoOpCoordinationProvisioner.class);

    @Override
    public void ensureReady() {
        LOGGER.info("Broker-less coordination: no infrastructure to provision");
    }
}
