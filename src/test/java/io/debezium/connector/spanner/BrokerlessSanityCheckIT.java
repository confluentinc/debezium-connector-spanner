/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.spanner;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
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
            .with("offset.storage", "org.apache.kafka.connect.storage.MemoryOffsetBackingStore")
            // The whole point: broker-less coordination. No bootstrap.servers anywhere.
            .with("connector.spanner.coordination.mode", "brokerless")
            .with("tasks.max", 1)
            .with("gcp.spanner.low-watermark.enabled", true)
            // Short interval so a watermark stamp record is emitted within the test's wait window.
            .with("gcp.spanner.low-watermark.stamp.interval", 2000)
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
    public void shouldStreamUpdatesWithLowWatermarkEnabled() throws InterruptedException {
        Instant testStart = Instant.now();
        final Configuration config = Configuration.copy(baseConfig)
                .with("gcp.spanner.change.stream", changeStreamName)
                .with("name", tableName + "_brokerless_test")
                .with("gcp.spanner.start.time", DateTimeFormatter.ISO_INSTANT.format(testStart))
                .build();

        List<SourceRecord> records = runAndCollectRecords(config);
        List<String> ops = dataChangeAndStampOps(records);

        // The data-change ops must arrive in order c, u, d; watermark stamp records ("m") may be
        // interleaved and are asserted separately below.
        List<String> dataChangeOps = ops.stream()
                .filter(op -> !"m".equals(op))
                .collect(Collectors.toList());
        assertThat(dataChangeOps).containsExactly("c", "u", "d");
        // The delete is followed by a tombstone (null-valued) record.
        assertThat(records.stream().anyMatch(r -> r.value() == null)).isTrue();

        // Proves the watermark stamp path (LowWatermarkStampPublisher -> SpannerEventDispatcher ->
        // BrokerlessPartitionInfoProvider) runs to completion without the null-AdminClient NPE that
        // used to crash the connector in broker-less mode.
        assertThat(ops).contains("m");

        // Every value-bearing record (data change or stamp) carries a populated, advancing
        // low watermark on its source struct.
        List<Long> lowWatermarks = lowWatermarksOf(records);
        assertThat(lowWatermarks).isNotEmpty();
        assertThat(Collections.max(lowWatermarks)).isGreaterThan(testStart.toEpochMilli());
    }

    @Test
    public void shouldStreamUpdatesWithLowWatermarkDisabled() throws InterruptedException {
        Instant testStart = Instant.now();
        final Configuration config = Configuration.copy(baseConfig)
                .with("gcp.spanner.change.stream", changeStreamName)
                .with("name", tableName + "_brokerless_test_watermark_disabled")
                .with("gcp.spanner.start.time", DateTimeFormatter.ISO_INSTANT.format(testStart))
                .with("gcp.spanner.low-watermark.enabled", false)
                .build();

        List<SourceRecord> records = runAndCollectRecords(config);
        List<String> ops = dataChangeAndStampOps(records);

        assertThat(ops).containsExactly("c", "u", "d");
        // The delete is followed by a tombstone (null-valued) record.
        assertThat(records.stream().anyMatch(r -> r.value() == null)).isTrue();

        // With the feature disabled, the stamp publisher must never run (no "m" records), and no
        // record carries a low watermark value.
        assertThat(ops).doesNotContain("m");
        assertThat(lowWatermarksOf(records)).isEmpty();
    }

    /**
     * Starts the connector, runs an insert/update/delete against {@link #tableName}, waits for and
     * consumes the resulting records, then stops the connector. Logs every consumed record for
     * diagnostics.
     */
    private List<SourceRecord> runAndCollectRecords(Configuration config) throws InterruptedException {
        initializeConnectorTestFramework();
        start(SpannerConnector.class, config);
        assertConnectorIsRunning();

        databaseConnection.executeUpdate("insert into " + tableName + "(id, name) values (1, 'some name')");
        databaseConnection.executeUpdate("update " + tableName + " set name = 'test' where id = 1");
        databaseConnection.executeUpdate("delete from " + tableName + " where id = 1");

        waitForAvailableRecords(30, TimeUnit.SECONDS);
        SourceRecords sourceRecords = consumeRecordsByTopic(20, false);
        List<SourceRecord> records = sourceRecords.allRecordsInOrder();

        List<String> ops = dataChangeAndStampOps(records);
        LOGGER.info("Broker-less run produced {} record(s) with ops {}", records.size(), ops);
        int idx = 0;
        for (SourceRecord record : records) {
            String op = "<none>";
            String valueType = record.value() == null ? "null (tombstone)" : record.value().getClass().getSimpleName();
            if (record.value() instanceof Struct && ((Struct) record.value()).schema().field("op") != null) {
                op = (String) ((Struct) record.value()).get("op");
            }
            LOGGER.info("  record[{}] topic={} value={} op={}", idx++, record.topic(), valueType, op);
        }

        stopConnector();
        assertConnectorNotRunning();
        return records;
    }

    // Every value-bearing record ("op" c/u/d for data changes, "m" for watermark stamp records),
    // ignoring heartbeat/tombstone (null value) records.
    private static List<String> dataChangeAndStampOps(List<SourceRecord> records) {
        return records.stream()
                .filter(r -> r.value() instanceof Struct)
                .filter(r -> ((Struct) r.value()).schema().field("op") != null)
                .map(r -> (String) ((Struct) r.value()).get("op"))
                .collect(Collectors.toList());
    }

    // Low watermark values off every value-bearing record's source struct. Heartbeat records use a
    // different schema with no "source" field, so they're excluded rather than assumed absent.
    private static List<Long> lowWatermarksOf(List<SourceRecord> records) {
        return records.stream()
                .filter(r -> r.value() instanceof Struct)
                .filter(r -> ((Struct) r.value()).schema().field("source") != null)
                .map(r -> (Struct) ((Struct) r.value()).get("source"))
                .filter(source -> source != null)
                .map(source -> (Long) source.get("low_watermark"))
                .filter(watermark -> watermark != null)
                .collect(Collectors.toList());
    }
}
