/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination.singletask;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.coordination.TaskStateSubscriber;
import io.debezium.connector.spanner.coordination.kafka.internal.model.SyncEventMetadata;
import io.debezium.connector.spanner.coordination.kafka.internal.model.TaskSyncEvent;
import io.debezium.connector.spanner.function.BlockingBiConsumer;

/**
 * Task state subscriber for single-task implementation. Kafka mode's {@code TaskSyncEventListener}
 * drains the {@code _connector_sync} topic backlog before signaling
 * {@code canInitiateRebalancing} whereas single-task mode has no topic and no peer state to drain, so
 * {@link #start()} delivers a {@code null} event with {@code canInitiateRebalancing=true} to every
 * subscriber synchronously and immediately. Without this, {@code SyncEventHandler
 * .processPreviousStates()} would never flip to {@code INITIAL_INCREMENTED_STATE_COMPLETED} and the
 * connector would deadlock at startup.
 */
public class SingleTaskStateSubscriber implements TaskStateSubscriber {
    private static final Logger LOGGER = LoggerFactory.getLogger(SingleTaskStateSubscriber.class);
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
        LOGGER.info("Single-task coordination: delivered initialization signal to {} consumer(s)", consumers.size());
    }

    @Override
    public void shutdown() {
        consumers.clear();
    }
}
