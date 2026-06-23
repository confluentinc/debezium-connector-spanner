/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner.task;

import static org.slf4j.LoggerFactory.getLogger;

import java.time.Duration;

import org.slf4j.Logger;

import io.debezium.connector.spanner.SpannerConnectorConfig;
import io.debezium.connector.spanner.SpannerConnectorTask;
import io.debezium.connector.spanner.coordination.LeaderElector;
import io.debezium.connector.spanner.coordination.MembershipProvider;
import io.debezium.connector.spanner.coordination.TaskCoordinator;
import io.debezium.connector.spanner.coordination.TaskCoordinatorFactory;
import io.debezium.connector.spanner.coordination.TaskStatePublisher;
import io.debezium.connector.spanner.coordination.TaskStateSubscriber;
import io.debezium.connector.spanner.db.metadata.SchemaRegistry;
import io.debezium.connector.spanner.db.stream.ChangeStream;
import io.debezium.connector.spanner.kafka.KafkaAdminClientFactory;
import io.debezium.connector.spanner.metrics.MetricsEventPublisher;
import io.debezium.connector.spanner.processor.SpannerEventDispatcher;
import io.debezium.connector.spanner.task.leader.LeaderAction;
import io.debezium.connector.spanner.task.leader.LeaderService;
import io.debezium.connector.spanner.task.leader.LowWatermarkStampPublisher;
import io.debezium.connector.spanner.task.leader.rebalancer.LeaderRebalanceStrategy;
import io.debezium.connector.spanner.task.leader.rebalancer.TaskPartitionEqualSharingRebalancer;
import io.debezium.connector.spanner.task.leader.rebalancer.TaskPartitionGreedyLeaderRebalancer;
import io.debezium.connector.spanner.task.leader.rebalancer.TaskPartitionRebalancer;
import io.debezium.connector.spanner.task.state.TaskStateChangeEvent;
import io.debezium.pipeline.ErrorHandler;

/**
 * This class coordinates between the connector producers and consumers:
 * The RebalancingEventListener producer produces events that are consumed by the RebalanceHandler.
 * The TaskSyncEventListener produces events that are consumed by the SyncEventHandler.
 * The SynchronizedPartitionManager produces events to the queue, which are then consumed from
 * by the TaskStateChangeEventHandler.
 */
public class SynchronizationTaskContext {
    private static final Logger LOGGER = getLogger(SynchronizationTaskContext.class);

    private final LeaderRebalanceStrategy leaderRebalanceStrategy = LeaderRebalanceStrategy.EQUAL_SHARING;
    private final LeaderAction leaderAction;

    private final LeaderElector leaderElector;
    private final TaskStateSubscriber taskStateSubscriber;
    private final TaskStatePublisher taskSyncPublisher;

    private final TaskSyncContextHolder taskSyncContextHolder;

    private final TaskStateChangeEventHandler taskStateChangeEventHandler;

    private final ErrorHandler errorHandler;

    private final PartitionFactory partitionFactory;

    private final LowWatermarkStampPublisher lowWatermarkStampPublisher;

    private final Runnable finishingHandler;

    private final TaskStateChangeEventProcessor taskStateChangeEventProcessor;

    private final SyncEventHandler syncEventHandler;

    private final RebalanceHandler rebalanceHandler;

    private final LowWatermarkCalculationJob lowWatermarkCalculationJob;

    private final SchemaRegistry schemaRegistry;

    private final SpannerConnectorTask task;

    private final SpannerConnectorConfig connectorConfig;

    public SynchronizationTaskContext(SpannerConnectorTask task,
                                      SpannerConnectorConfig connectorConfig,
                                      ErrorHandler errorHandler,
                                      PartitionOffsetProvider partitionOffsetProvider,
                                      ChangeStream changeStream,
                                      SpannerEventDispatcher spannerEventDispatcher,
                                      KafkaAdminClientFactory adminClientFactory,
                                      SchemaRegistry schemaRegistry,
                                      Runnable finishingHandler,
                                      MetricsEventPublisher metricsEventPublisher,
                                      LowWatermarkHolder lowWatermarkHolder) {
        this.task = task;

        this.connectorConfig = connectorConfig;

        this.errorHandler = errorHandler;

        this.finishingHandler = finishingHandler;

        this.schemaRegistry = schemaRegistry;

        this.taskSyncContextHolder = new TaskSyncContextHolder(metricsEventPublisher);

        TaskCoordinator coordination = TaskCoordinatorFactory.create(
                connectorConfig, task, adminClientFactory, taskSyncContextHolder, this::onError);
        this.taskSyncPublisher = coordination.statePublisher();
        this.taskStateSubscriber = coordination.stateSubscriber();
        this.leaderElector = coordination.leaderElector();
        final MembershipProvider membershipProvider = coordination.membershipProvider();

        this.partitionFactory = new PartitionFactory(partitionOffsetProvider, metricsEventPublisher);

        final LeaderService leaderService = new LeaderService(taskSyncContextHolder,
                connectorConfig,
                this::publishEvent,
                errorHandler,
                partitionFactory,
                metricsEventPublisher);

        this.lowWatermarkStampPublisher = new LowWatermarkStampPublisher(connectorConfig,
                spannerEventDispatcher, this::onError, taskSyncContextHolder);

        TaskPartitionRebalancer taskPartitionRebalancer = leaderRebalanceStrategy.equals(LeaderRebalanceStrategy.EQUAL_SHARING)
                ? new TaskPartitionEqualSharingRebalancer()
                : new TaskPartitionGreedyLeaderRebalancer();

        this.leaderAction = new LeaderAction(taskSyncContextHolder, membershipProvider, leaderService,
                taskPartitionRebalancer, taskSyncPublisher, this::onError);

        this.taskStateChangeEventHandler = new TaskStateChangeEventHandler(taskSyncContextHolder, taskSyncPublisher,
                changeStream, partitionFactory, spannerEventDispatcher, this::onFinish, connectorConfig, this::onError);

        this.rebalanceHandler = new RebalanceHandler(taskSyncContextHolder, taskSyncPublisher,
                leaderAction, lowWatermarkStampPublisher);

        this.syncEventHandler = new SyncEventHandler(taskSyncContextHolder,
                taskSyncPublisher, this::publishEvent);

        final LowWatermarkCalculator lowWatermarkCalculator = new LowWatermarkCalculator(connectorConfig, taskSyncContextHolder, partitionOffsetProvider);

        this.lowWatermarkCalculationJob = new LowWatermarkCalculationJob(connectorConfig, this::onError, lowWatermarkCalculator,
                lowWatermarkHolder, task.getTaskUid());

        this.taskStateChangeEventProcessor = new TaskStateChangeEventProcessor(connectorConfig.taskStateChangeEventQueueCapacity(),
                taskSyncContextHolder, taskStateChangeEventHandler, this::onError, metricsEventPublisher);

    }

    public synchronized void init() {
        try {

            this.taskSyncContextHolder.init(TaskSyncContext.getInitialContext(this.task.getTaskUid(), connectorConfig));

            this.rebalanceHandler.init();

            this.taskStateSubscriber.subscribe(syncEventHandler::updateCurrentOffset);

            this.taskStateSubscriber.subscribe(syncEventHandler::process);

            this.taskStateSubscriber.subscribe(syncEventHandler::processPreviousStates);

            this.taskStateSubscriber.start();

            final Duration awaitTimeout = connectorConfig.awaitInitializationTimeout();

            this.taskSyncContextHolder.awaitInitialization(awaitTimeout);

            LOGGER.info("{}, connecting to the rebalance topic", task.getTaskUid());
            this.leaderElector
                    .listen(metadata -> rebalanceHandler.process(metadata.isLeader(), metadata.getConsumerId(), metadata.getRebalanceGenerationId()));

            LOGGER.info("{}, Start Low Watermark Calculation Job", task.getTaskUid());
            this.lowWatermarkCalculationJob.start();

            LOGGER.info("{}, Init Schema Registry", task.getTaskUid());
            try {
                this.schemaRegistry.init(this.task.getTaskUid());
            }
            catch (Exception e) {
                LOGGER.error("{}, Init Schema Registry failure", task.getTaskUid());
                throw e;
            }

            LOGGER.info("{}, Start Processing Task State Change Event Processor", task.getTaskUid());
            this.taskStateChangeEventProcessor.startProcessing();

            LOGGER.info("{}, TaskSyncContextHolder update initialized", task.getTaskUid());
            this.taskSyncContextHolder.update(context -> context.toBuilder().initialized(true).build());

            LOGGER.info("{}, Finished updating TaskSyncContextHolder", task.getTaskUid());

        }
        catch (InterruptedException ex) {
            LOGGER.error("Interrupted exception during SynchronizationTaskContext starting", ex);
            this.onError(ex);

        }
        catch (Exception ex) {
            LOGGER.error("Exception during SynchronizationTaskContext starting", ex);
            this.onError(ex);
        }
    }

    public void destroy() {

        try {
            try {
                this.leaderElector.shutdown();
            }
            catch (Exception e) {
                LOGGER.error("Task {}, exception during rebalancing event listener shutdown", e);
                throw e;
            }
            LOGGER.info("Task {}, Shut down rebalancingEventListener", this.taskSyncContextHolder.get().getTaskUid());

            this.taskStateSubscriber.shutdown();
            LOGGER.info("Task {}, Shut down TaskSyncEventListener", this.taskSyncContextHolder.get().getTaskUid());

            this.taskSyncPublisher.close();
            LOGGER.info("Task {}, Shut down TaskSyncPublisher", this.taskSyncContextHolder.get().getTaskUid());

            this.taskStateChangeEventProcessor.stopProcessing();
            LOGGER.info("Task {}, Shut down TaskStateChangeEventProcessor", this.taskSyncContextHolder.get().getTaskUid());

            this.lowWatermarkCalculationJob.stop();
            LOGGER.info("Task {}, Shut down LowWatermarkCalculationJob", this.taskSyncContextHolder.get().getTaskUid());

            this.rebalanceHandler.destroy();
            LOGGER.info("Task {}, Shut down rebalance handler", this.taskSyncContextHolder.get().getTaskUid());

        }
        catch (Exception ex) {
            LOGGER.warn("Task {}, Exception during sync context destroying", this.taskSyncContextHolder.get().getTaskUid(), ex);
        }
        finally {
            LOGGER.info("Task {}, SynchronizationTaskContext end", this.taskSyncContextHolder.get().getTaskUid());
        }

    }

    public void publishEvent(TaskStateChangeEvent event) throws InterruptedException {
        LoggerUtils.debug(LOGGER, "publishEvent: type: {}, event: {}", event.getClass().getSimpleName(), event);

        this.taskStateChangeEventProcessor.processEvent(event);
    }

    private void onError(Throwable throwable) {
        LOGGER.info("Task {}, enqueueing error in task", this.taskSyncContextHolder.get().getTaskUid(), throwable);
        this.errorHandler.setProducerThrowable(throwable);
    }

    private void onFinish() {
        this.finishingHandler.run();
    }
}