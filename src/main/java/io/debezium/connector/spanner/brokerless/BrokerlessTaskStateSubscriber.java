/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.brokerless;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.coordination.TaskStateSubscriber;
import io.debezium.connector.spanner.function.BlockingBiConsumer;
import io.debezium.connector.spanner.kafka.internal.model.SyncEventMetadata;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;

/**
 * Task state subscriber for broker-less implementation
 */
public class BrokerlessTaskStateSubscriber implements TaskStateSubscriber {
    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerlessTaskStateSubscriber.class);
    private final List<BlockingBiConsumer<TaskSyncEvent, SyncEventMetadata>> consumers = new CopyOnWriteArrayList<>();

    @Override
    public void subscribe(BlockingBiConsumer<TaskSyncEvent, SyncEventMetadata> eventConsumer) {
        consumers.add(eventConsumer);
    }

    @Override
    public void start() throws InterruptedException {
        SyncEventMetadata metadata = SyncEventMetadata.builder().canInitiateRebalancing(true).build();
        for (BlockingBiConsumer<TaskSyncEvent, SyncEventMetadata> consumer : consumers) {
            consumer.accept(null, metadata);
        }
        LOGGER.info("Broker-less coordination: delivered initialization signal to {} consumer(s)", consumers.size());
    }

    @Override
    public void shutdown() {
        consumers.clear();
    }
}
