/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.spanner.util.Connection;
import io.debezium.connector.spanner.util.Database;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing;

/**
 * Proves the Spanner connector runs with {@code connector.spanner.coordination.mode=single-task} and
 * <strong>no Kafka broker at all</strong>: it deliberately does NOT extend
 * {@link AbstractSpannerConnectorIT} (which boots a Kafka container) and sets no
 * {@code bootstrap.servers}. The connector is driven by the Debezium embedded engine against the
 * local Spanner emulator, with offsets in {@link org.apache.kafka.connect.storage.MemoryOffsetBackingStore}.
 *
 * <p>Mirrors {@code BasicSanityCheckIT#shouldStreamUpdatesToKafka} (the proven Kafka-mode flow) so a
 * green run here is a direct, behaviour-matched broker-less comparison.
 *
 * <p>Prerequisites: Spanner emulator on localhost:9010/9020 and a throwaway service-account JSON at
 * {@code /tmp/emulator-sa.json} (older runtimes look up ADC unconditionally; the emulator ignores the
 * key but the file must parse).
 */
public class BrokerlessSanityCheckIT extends AbstractAsyncEngineConnectorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerlessSanityCheckIT.class);

    private static final Database database = Database.TEST_DATABASE;
    private static final Connection databaseConnection = database.getConnection();

    private static final String tableName = "brokerless_sanity_table";
    private static final String changeStreamName = "brokerlessSanityChangeStream";

    private static final Configuration baseConfig = Configuration.create()
            .with("gcp.spanner.instance.id", database.getInstanceId())
            .with("gcp.spanner.project.id", database.getProjectId())
            .with("gcp.spanner.database.id", database.getDatabaseId())
            .with("gcp.spanner.emulator.host", "http://localhost:9010")
            // Older runtimes resolve ADC unconditionally; emulator ignores the key, file must parse.
            .with("gcp.spanner.credentials.path", "/tmp/emulator-sa.json")
            .with("offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
            // The whole point: broker-less coordination. No bootstrap.servers anywhere.
            .with("connector.spanner.coordination.mode", "single-task")
            .with("tasks.max", 1)
            .with("gcp.spanner.low-watermark.enabled", false)
            .with("heartbeat.interval.ms", "300000")
            .build();

    @BeforeAll
    static void setup() throws InterruptedException, ExecutionException {
        Testing.Print.enable();
        databaseConnection.createTable(tableName + "(id int64, name string(100)) primary key(id)");
        databaseConnection.createChangeStream(changeStreamName, tableName);
        Testing.print("BrokerlessSanityCheckIT is ready (no Kafka broker)...");
    }

    @AfterAll
    static void clear() throws InterruptedException {
        databaseConnection.dropChangeStream(changeStreamName);
        databaseConnection.dropTable(tableName);
    }

    @Test
    public void shouldStreamUpdatesWithoutKafkaBroker() throws InterruptedException {
        final Configuration config = Configuration.copy(baseConfig)
                .with("gcp.spanner.change.stream", changeStreamName)
                .with("name", tableName + "_brokerless_test")
                .with("gcp.spanner.start.time", DateTimeFormatter.ISO_INSTANT.format(Instant.now()))
                .build();

        initializeConnectorTestFramework();
        start(SpannerConnector.class, config);
        assertConnectorIsRunning();

        databaseConnection.executeUpdate("insert into " + tableName + "(id, name) values (1, 'some name')");
        databaseConnection.executeUpdate("update " + tableName + " set name = 'test' where id = 1");
        databaseConnection.executeUpdate("delete from " + tableName + " where id = 1");

        waitForAvailableRecords(30, TimeUnit.SECONDS);
        SourceRecords sourceRecords = consumeRecordsByTopic(10, false);
        List<SourceRecord> records = sourceRecords.allRecordsInOrder();

        // Keep only the data-change records (those whose value carries an "op"), ignoring heartbeat
        // / tombstone (null value) / any other framework records. In order they must be c, u, d.
        List<String> ops = records.stream()
                .filter(r -> r.value() instanceof Struct)
                .filter(r -> ((Struct) r.value()).schema().field("op") != null)
                .map(r -> (String) ((Struct) r.value()).get("op"))
                .collect(Collectors.toList());

        LOGGER.info("Broker-less run produced {} record(s) with ops {}", records.size(), ops);

        // Log every record so the full stream is visible: the 3 data changes (c/u/d) plus the
        // delete's tombstone and the Spanner change-stream heartbeats that interleave them.
        int idx = 0;
        for (SourceRecord record : records) {
            String op = "<none>";
            String valueType = record.value() == null ? "null (tombstone)" : record.value().getClass().getSimpleName();
            if (record.value() instanceof Struct && ((Struct) record.value()).schema().field("op") != null) {
                op = (String) ((Struct) record.value()).get("op");
            }
            LOGGER.info("  record[{}] topic={} value={} op={}", idx++, record.topic(), valueType, op);
        }

        assertThat(ops).containsExactly("c", "u", "d");
        // The delete is followed by a tombstone (null-valued) record.
        assertThat(records.stream().anyMatch(r -> r.value() == null)).isTrue();

        stopConnector();
        assertConnectorNotRunning();
    }
}
