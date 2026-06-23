# Claude Code Instructions — Debezium Spanner Connector (broker-less PoC)

## What this PoC adds

By default the Debezium Spanner connector requires a Kafka broker to coordinate work across
tasks (an internal **sync topic** for partition-ownership state and a **rebalancing topic** for
leader election / membership) and, optionally, to publish low watermarks.

This PoC makes the connector able to run **without a Kafka broker** by introducing a single-task
coordination mode. With exactly one task there is nothing to coordinate: the task elects itself
leader, owns every change-stream partition, and streams them with one thread per partition. The
Kafka-coupled coordination logic is pulled behind an explicit SPI; the existing Kafka behaviour
becomes one implementation, and a broker-less single-task implementation is added alongside.
The mode is selected by config — **the default Kafka behaviour is unchanged.**

## How to enable it

```properties
connector.spanner.coordination.mode=single-task   # default: kafka
tasks.max=1
gcp.spanner.low-watermark.enabled=false
# no bootstrap.servers required
```

`SpannerConnectorConfig` exposes `coordinationMode()` / `isSingleTaskCoordination()` for this.

## Code map

The coordination SPI lives in `io.debezium.connector.spanner.coordination`:

| Type | Role |
|------|------|
| `TaskStatePublisher` | publishes this task's partition-ownership state |
| `TaskStateSubscriber` | receives partition-ownership state and drives initialization |
| `LeaderElector` | elects a leader and notifies on membership changes |
| `MembershipProvider` | reports the set of active task members |
| `CoordinationProvisioner` | provisions shared infrastructure once at startup |
| `TaskCoordinator` | bundles the four task-scoped components above |
| `TaskCoordinatorFactory` | selects the implementation from config (`create` + `createProvisioner`) |

**Kafka implementation** (default) — the existing classes now implement the interfaces:
`TaskSyncPublisher`, `TaskSyncEventListener`, `RebalancingEventListener`,
`KafkaConsumerAdminService`, and `KafkaInternalTopicAdminService` (now also the
`CoordinationProvisioner`). `coordination/kafka/KafkaTaskCoordinator` wires them together.

**Broker-less implementation** (single-task) — `coordination/singletask/`:
`SingleTaskCoordinator` with `NoOpTaskStatePublisher`, `SingleTaskStateSubscriber`,
`SelfLeaderElector`, `SoleMemberProvider`, plus `NoOpCoordinationProvisioner`. Each satisfies its
interface as a no-op or self-answer, because a single task has no peer to coordinate with.

The wiring is in `SynchronizationTaskContext` (obtains coordination components from the factory as
interfaces), `LeaderAction` and the task handlers (depend only on the interfaces), `SpannerConnector`
(provisions via the factory), and `SpannerConnectorTask` (skips the Kafka admin client in
single-task mode).

## Build

This project builds against Debezium `3.6.0-SNAPSHOT`. Its parent POM and dependencies are not on
Maven Central, so point Maven at Debezium's public snapshot repository via a settings file:

```bash
./mvnw -s /tmp/spanner-build-settings.xml -DskipTests test-compile
```

The settings file defines two repositories (releases from Maven Central, snapshots from
`https://central.sonatype.com/repository/maven-snapshots/`) and no other mirror.

Import ordering / formatting is enforced by the `impsort` and `formatter` plugins (not spotless):

```bash
./mvnw -s /tmp/spanner-build-settings.xml net.revelc.code:impsort-maven-plugin:check
```

## Run the broker-less integration test

`BrokerlessSanityCheckIT` proves the connector streams create/update/delete (+ tombstone) change
events through the embedded engine with **no Kafka broker**.

Prerequisites:
1. Spanner emulator on `localhost:9010/9020`:
   ```bash
   docker run -d --name spanner-emulator -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator
   ```
2. A throwaway service-account JSON at `/tmp/emulator-sa.json` (must parse; the emulator ignores it).

Run the failsafe goal directly (not the `verify` lifecycle) so the docker-maven-plugin doesn't try
to start a second emulator on `9010`:

```bash
./mvnw -s /tmp/spanner-build-settings.xml failsafe:integration-test failsafe:verify \
  -Dit.test=BrokerlessSanityCheckIT -DskipITs=false
```

A green run logs `Broker-less run produced N record(s) with ops [c, u, d]` and asserts the ops and a
tombstone, with no broker involved.

## Prerequisites

- **JDK 21** (build and run).
- **Docker** (for the Spanner emulator used by the integration test).
