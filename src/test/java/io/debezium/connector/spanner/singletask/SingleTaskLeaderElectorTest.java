/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.singletask;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.debezium.connector.spanner.kafka.internal.model.RebalanceEventMetadata;

class SingleTaskLeaderElectorTest {

    @Test
    void alwaysElectsSelfAsLeaderAtGenerationZero() throws InterruptedException {
        SingleTaskLeaderElector elector = new SingleTaskLeaderElector("consumer-1");
        AtomicReference<RebalanceEventMetadata> received = new AtomicReference<>();

        elector.listen(received::set);

        RebalanceEventMetadata metadata = received.get();
        assertEquals("consumer-1", metadata.getConsumerId());
        assertEquals(0L, metadata.getRebalanceGenerationId());
        assertTrue(metadata.isLeader());
    }

    @Test
    void shutdownDoesNotThrow() {
        SingleTaskLeaderElector elector = new SingleTaskLeaderElector("consumer-1");

        elector.shutdown();
    }
}
