/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.coordination.singletask;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.spanner.coordination.LeaderElector;
import io.debezium.connector.spanner.coordination.MembershipProvider;
import io.debezium.connector.spanner.coordination.TaskCoordinator;
import io.debezium.connector.spanner.coordination.TaskStatePublisher;
import io.debezium.connector.spanner.coordination.TaskStateSubscriber;
import io.debezium.connector.spanner.function.BlockingBiConsumer;
import io.debezium.connector.spanner.kafka.internal.model.RebalanceEventMetadata;
import io.debezium.connector.spanner.kafka.internal.model.SyncEventMetadata;
import io.debezium.connector.spanner.kafka.internal.model.TaskSyncEvent;
import io.debezium.function.BlockingConsumer;

/**
 * Broker-less, single-task implementation of the partition-coordination SPI.
 *
 * <p>With exactly one task there is no cross-task coordination to perform, so:
 * <ul>
 *   <li>the {@link TaskStatePublisher} is a no-op (no other task to inform);</li>
 *   <li>the {@link TaskStateSubscriber} drives initialization directly on {@code start()} by
 *       emitting a {@code (null, canInitiateRebalancing=true)} signal — equivalent to the Kafka
 *       path's "sync topic is empty" case;</li>
 *   <li>the {@link LeaderElector} elects this task once with generation 0; the existing
 *       {@code LeaderAction} then transitions the local context to {@code NEW_EPOCH_STARTED} and
 *       assigns the whole change stream to this task (no round-trip through a broker required);</li>
 *   <li>the {@link MembershipProvider} reports this task as the only member.</li>
 * </ul>
 *
 * <p>The leader's consumer id is shared between the elector and the membership provider so that
 * {@code LeaderAction.newEpoch()} sees an empty set of peers after removing itself.
 */
public class SingleTaskCoordinator implements TaskCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleTaskCoordinator.class);

    private final TaskStatePublisher statePublisher = new NoOpTaskStatePublisher();
    private final TaskStateSubscriber stateSubscriber = new SingleTaskStateSubscriber();
    private final LeaderElector leaderElector;
    private final MembershipProvider membershipProvider;

    public SingleTaskCoordinator(String taskUid) {
        final String consumerId = "single-task-" + taskUid;
        this.leaderElector = new SelfLeaderElector(consumerId);
        this.membershipProvider = new SoleMemberProvider(consumerId);
    }

    @Override
    public TaskStatePublisher statePublisher() {
        return statePublisher;
    }

    @Override
    public TaskStateSubscriber stateSubscriber() {
        return stateSubscriber;
    }

    @Override
    public LeaderElector leaderElector() {
        return leaderElector;
    }

    @Override
    public MembershipProvider membershipProvider() {
        return membershipProvider;
    }

    /** No other task consumes published state, so publishing is a no-op. */
    static final class NoOpTaskStatePublisher implements TaskStatePublisher {
        private volatile Instant lastTime = Instant.now();

        @Override
        public void send(TaskSyncEvent taskSyncEvent) {
            lastTime = Instant.now();
        }

        @Override
        public void close() {
            // nothing to release
        }

        @Override
        public Instant getLastTime() {
            return lastTime;
        }
    }

    /**
     * Has no external state to consume. On {@code start()} it delivers a single
     * {@code (null, canInitiateRebalancing=true)} signal to each registered consumer, which moves
     * the task out of {@code START_INITIAL_SYNC} and satisfies {@code awaitInitialization()}.
     */
    static final class SingleTaskStateSubscriber implements TaskStateSubscriber {
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

    /** Elects this (only) task as leader exactly once, with rebalance generation 0. */
    static final class SelfLeaderElector implements LeaderElector {
        private final String consumerId;

        SelfLeaderElector(String consumerId) {
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

    /** Reports this (only) task as the sole active member. */
    static final class SoleMemberProvider implements MembershipProvider {
        private final String consumerId;

        SoleMemberProvider(String consumerId) {
            this.consumerId = consumerId;
        }

        @Override
        public Set<String> getActiveMembers() {
            // Return a fresh mutable set each call: LeaderAction.newEpoch() removes the leader's own
            // consumerId in place (matching the Kafka impl, which returns a mutable HashSet). With one
            // task, that leaves an empty peer set — no phantom peer to wait on.
            return new HashSet<>(Set.of(consumerId));
        }
    }
}
