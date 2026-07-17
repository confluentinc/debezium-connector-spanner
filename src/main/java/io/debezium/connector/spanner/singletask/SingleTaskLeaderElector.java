/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.singletask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.coordination.LeaderElector;
import io.debezium.connector.spanner.kafka.internal.model.RebalanceEventMetadata;
import io.debezium.function.BlockingConsumer;

/**
 * Leader elector for single-task implementation. Kafka mode's {@code RebalancingEventListener}
 * derives leadership and generation from an actual consumer-group rebalance and only invokes
 * {@code action} once that settles but there is no consumer group here, so {@link #listen} delivers a
 * single, synchronous result instead. So this task is always the leader, and the generation is
 * hardcoded to {@code 0} since single-task mode never has more than one rebalance epoch to
 * distinguish. No background thread is needed since there's nothing to listen for, which is also
 * why {@link #shutdown()} is a no-op.
 */
public class SingleTaskLeaderElector implements LeaderElector {
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleTaskLeaderElector.class);
    private final String consumerId;

    SingleTaskLeaderElector(String consumerId) {
        this.consumerId = consumerId;
    }

    @Override
    public void listen(BlockingConsumer<RebalanceEventMetadata> action) {
        LOGGER.info("Single-task coordination: electing self ({}) as leader, generation 0", consumerId);
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
