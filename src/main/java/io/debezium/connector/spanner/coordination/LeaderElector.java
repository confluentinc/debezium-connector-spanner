/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination;

import io.debezium.connector.spanner.kafka.internal.model.RebalanceEventMetadata;
import io.debezium.function.BlockingConsumer;

/**
 * SPI for leader election
 */
public interface LeaderElector {

    void listen(BlockingConsumer<RebalanceEventMetadata> action);

    void shutdown();
}
