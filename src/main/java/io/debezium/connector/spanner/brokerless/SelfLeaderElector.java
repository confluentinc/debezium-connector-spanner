/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.brokerless;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.coordination.LeaderElector;
import io.debezium.connector.spanner.kafka.internal.model.RebalanceEventMetadata;
import io.debezium.function.BlockingConsumer;

/**
 * Leader elector for broker-less implementation
 */
public class SelfLeaderElector implements LeaderElector {
    private static final Logger LOGGER = LoggerFactory.getLogger(SelfLeaderElector.class);
    private final String consumerId;

    SelfLeaderElector(String consumerId) {
        this.consumerId = consumerId;
    }

    @Override
    public void listen(BlockingConsumer<RebalanceEventMetadata> action) {
        LOGGER.info("Broker-less coordination: electing self ({}) as leader, generation 0", consumerId);
        try {
            action.accept(new RebalanceEventMetadata(consumerId, 0L, true));
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void shutdown() {
        // no background work to stop
    }
}
