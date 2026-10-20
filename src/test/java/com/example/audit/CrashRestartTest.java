package com.example.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Crash and restart evidence for the Kafka to Flink to Iceberg job, against a real broker.
 *
 * Needs Kafka at KAFKA_BOOTSTRAP (environment variable or system property, default
 * localhost:9092; `make up` starts one). Without a reachable broker both tests are
 * skipped, unless REQUIRE_KAFKA=1, in which case they fail.
 *
 * The job reads an unbounded stream, as in production, so each run is started with
 * executeAsync and stopped by the test: run 1 by the injected failure, every other run by
 * cancellation once the Iceberg table holds the expected rows and stays there.
 *
 * Case 1, recovery: 5,000 distinct events go to a fresh topic. Run 1 of the job
 * checkpoints every 200 ms with retained checkpoints on disk and a map operator that
 * throws once after at least one checkpoint has completed and 2,000 records have passed.
 * The restart strategy is "none", so the job dies. Run 2 is a new submission of the same
 * pipeline restored from the last completed checkpoint of run 1. The Iceberg table must
 * then hold exactly one row per input event, with no repeated (customer_id,
 * event_sequence). Records that passed the failing operator after the last checkpoint
 * were written to data files that were never committed; Kafka replays them from the
 * checkpointed offsets, and they are committed once.
 *
 * Case 2, duplicate in the topic: the same events plus one of them sent a second time.
 * The job runs without failure. The duplicate is a separate Kafka record, so it lands as
 * a separate Iceberg row: 5,001 rows, 5,000 distinct (customer_id, event_sequence) pairs,
 * and sql/dedup.sql returns 5,000. That is the boundary of exactly-once: Flink's recovery
 * path, not the topic's content.
 *
 * Both tests write what they observed to results/crash_restart_case{1,2}.md.
 */
class CrashRestartTest {

    static final int CUSTOMERS = 50;
    static final int EVENTS_PER_CUSTOMER = 100;
    static final int PARTITIONS = 3;
    static final long CHECKPOINT_INTERVAL_MS = 200;
    static final int FAIL_AFTER_RECORDS = 2_000;
    static final long WAIT_FOR_ROWS_SECONDS = 180;
    static final ObjectMapper JSON = new ObjectMapper();

    static String bootstrap() {
        String v = System.getProperty("KAFKA_BOOTSTRAP", System.getenv("KAFKA_BOOTSTRAP"));
        return v == null || v.isEmpty() ? "localhost:9092" : v;
    }

    @BeforeAll
    static void requireKafka() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap());
        props.put("request.timeout.ms", "5000");
        props.put("default.api.timeout.ms", "5000");
        boolean reachable;
        try (AdminClient admin = AdminClient.create(props)) {
            admin.describeCluster().nodes().get(10, TimeUnit.SECONDS);
            reachable = true;
        } catch (Exception e) {
            reachable = false;
        }
        boolean required = "1".equals(System.getProperty("REQUIRE_KAFKA", System.getenv("REQUIRE_KAFKA")));
        if (required) {
            assertTrue(reachable, "REQUIRE_KAFKA=1 but no Kafka broker at " + bootstrap());
        }
        Assumptions.assumeTrue(reachable, "no Kafka broker at " + bootstrap() + "; run `make up`");
    }

    // ------------------------------------------------------------------ case 1

    @Test
    @Timeout(600)
    void recoveryFromLastCheckpoint_commitsEachInputEventOnce(@TempDir Path tmp) throws Exception {
        String topic = "crash-restart-" + UUID.randomUUID();
        List<CustomerProfileChangeEvent> events = events();
        long recordsInTopic = produce(topic, events, null);
        assertEquals(events.size(), recordsInTopic, "every produced record must be in the topic");
        String warehouse = "file://" + tmp.resolve("warehouse");
        Path checkpoints = tmp.resolve("checkpoints");

        // Run 1: dies on the injected failure after at least one completed checkpoint.
        FailOnce.reset();
        JobClient run1 = startJob(topic, warehouse, checkpoints, null, true);
        Throwable failure = null;
        try {
            run1.getJobExecutionResult().get(WAIT_FOR_ROWS_SECONDS, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            failure = e.getCause();
        }
        assertNotNull(failure, "run 1 must end with the injected failure");
        assertTrue(FailOnce.failed.get(), "the failing operator must have thrown");
        assertTrue(FailOnce.checkpointsBeforeFailure >= 1,
                "at least one checkpoint must have completed before the failure");
        long recordsBeforeFailure = FailOnce.recordsBeforeFailure;
        TableSnapshot afterRun1 = readTable(warehouse);
        assertTrue(afterRun1.rows.size() < events.size(),
                "run 1 stopped before every event was committed");

        // Run 2: a new job restored from the last completed checkpoint of run 1. It is
        // cancelled once the table holds every event and the count has stopped moving.
        Path restoreFrom = latestCompletedCheckpoint(checkpoints);
        JobClient run2 = startJob(topic, warehouse, checkpoints, restoreFrom.toUri().toString(), false);
        TableSnapshot afterRun2 = waitForRows(warehouse, events.size());
        run2.cancel().get(60, TimeUnit.SECONDS);
        Set<String> distinct = distinctPairs(afterRun2.rows);
        long duplicateRows = afterRun2.rows.size() - distinct.size();

        String evidence = String.join("\n",
            "# Crash and restart, case 1: recovery from the last checkpoint",
            "",
            "Observed by CrashRestartTest against Kafka at " + bootstrap() + ".",
            "",
            "| Measure | Value |",
            "|---|---|",
            "| Distinct input events produced | " + events.size() + " |",
            "| Kafka records in the topic (sum of partition end offsets) | " + recordsInTopic + " |",
            "| Records that passed the failing operator before it threw | " + recordsBeforeFailure + " |",
            "| Checkpoints completed before the failure | " + FailOnce.checkpointsBeforeFailure + " |",
            "| Iceberg rows committed when run 1 died | " + afterRun1.rows.size() + " (" + afterRun1.snapshots + " snapshots) |",
            "| Records between the last commit and the failure, replayed from Kafka | " + (recordsBeforeFailure - afterRun1.rows.size()) + " |",
            "| Restored from | " + restoreFrom + " |",
            "| Iceberg rows after run 2 | " + afterRun2.rows.size() + " (" + afterRun2.snapshots + " snapshots) |",
            "| Distinct (customer_id, event_sequence) after run 2 | " + distinct.size() + " |",
            "| Rows added by recovery beyond the input | " + duplicateRows + " |",
            "");
        writeEvidence("results/crash_restart_case1.md", evidence);
        System.out.println(evidence);

        assertEquals(events.size(), afterRun2.rows.size(),
                "Iceberg row count must equal the number of distinct input events");
        assertEquals(events.size(), distinct.size(),
                "no repeated (customer_id, event_sequence) may come from recovery");
        assertTrue(recordsBeforeFailure > afterRun1.rows.size(),
                "some records had passed the operator but were not yet committed, so recovery had work to redo");
    }

    // ------------------------------------------------------------------ case 2

    @Test
    @Timeout(600)
    void duplicateAlreadyInTheTopic_landsTwice_andDedupReturnsItOnce(@TempDir Path tmp) throws Exception {
        String topic = "topic-duplicate-" + UUID.randomUUID();
        List<CustomerProfileChangeEvent> events = events();
        CustomerProfileChangeEvent resent = events.get(events.size() / 2);
        long recordsInTopic = produce(topic, events, resent);
        assertEquals(events.size() + 1, recordsInTopic, "every produced record must be in the topic");
        String warehouse = "file://" + tmp.resolve("warehouse");

        FailOnce.reset();
        JobClient job = startJob(topic, warehouse, tmp.resolve("checkpoints"), null, false);
        TableSnapshot after = waitForRows(warehouse, events.size() + 1);
        job.cancel().get(60, TimeUnit.SECONDS);
        Set<String> distinct = distinctPairs(after.rows);
        long dedupRows = dedupRowCount(after.rows);

        String evidence = String.join("\n",
            "# Crash and restart, case 2: a duplicate already in the topic",
            "",
            "Observed by CrashRestartTest against Kafka at " + bootstrap() + ".",
            "",
            "| Measure | Value |",
            "|---|---|",
            "| Distinct events produced | " + events.size() + " |",
            "| Event sent a second time | customer_id=" + resent.getCustomerId() + ", event_sequence=" + resent.getEventSequence() + " |",
            "| Kafka records produced | " + (events.size() + 1) + " |",
            "| Kafka records in the topic (sum of partition end offsets) | " + recordsInTopic + " |",
            "| Iceberg rows after the job | " + after.rows.size() + " |",
            "| Distinct (customer_id, event_sequence) | " + distinct.size() + " |",
            "| Rows returned by sql/dedup.sql | " + dedupRows + " |",
            "");
        writeEvidence("results/crash_restart_case2.md", evidence);
        System.out.println(evidence);

        assertEquals(events.size() + 1, after.rows.size(),
                "a duplicate Kafka record is a separate row; exactly-once does not remove it");
        assertEquals(events.size(), distinct.size());
        assertEquals(events.size(), dedupRows, "sql/dedup.sql returns the repeated event once");
    }

    // ---------------------------------------------------------------- pipeline

    /** The job's own source and Iceberg sink, with the test's checkpoint settings and an
     *  optional failure, started asynchronously on an embedded cluster. The failing
     *  operator is present in both runs (disabled in run 2), so the topology, and therefore
     *  the operator state mapping, is the same. */
    static JobClient startJob(String topic, String warehouse, Path checkpointDir, String restoreFrom,
                              boolean injectFailure) throws Exception {
        Configuration conf = new Configuration();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        if (restoreFrom != null) {
            conf.set(StateRecoveryOptions.SAVEPOINT_PATH, restoreFrom);
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        env.enableCheckpointing(CHECKPOINT_INTERVAL_MS, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage(checkpointDir.toUri().toString());
        env.getCheckpointConfig().setExternalizedCheckpointRetention(
                ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        DataStream<CustomerProfileChangeEvent> events = env
            .fromSource(CustomerProfileAuditJob.kafkaSource(bootstrap(), topic, "crash-restart-test"),
                        WatermarkStrategy.noWatermarks(), "Kafka customer profile events")
            .uid("kafka-customer-profile-events")
            .map(new FailOnce(injectFailure))
            .name("fail-once")
            .uid("fail-once");
        CustomerProfileAuditJob.appendToIceberg(events, warehouse, "");
        return env.executeAsync(injectFailure ? "crash-restart run 1" : "crash-restart run 2");
    }

    /** Poll the Iceberg table until it holds at least {@code expected} rows and the count has
     *  not moved for two seconds (ten checkpoint intervals), then return what it holds. */
    static TableSnapshot waitForRows(String warehouse, int expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(WAIT_FOR_ROWS_SECONDS);
        TableSnapshot last = readTable(warehouse);
        while (System.nanoTime() < deadline) {
            if (last.rows.size() >= expected) {
                Thread.sleep(2_000);
                TableSnapshot again = readTable(warehouse);
                if (again.rows.size() == last.rows.size()) {
                    return again;
                }
                last = again;
                continue;
            }
            Thread.sleep(500);
            last = readTable(warehouse);
        }
        throw new AssertionError("expected " + expected + " rows within " + WAIT_FOR_ROWS_SECONDS
                + "s, the table holds " + last.rows.size());
    }

    /** Throws once, after at least one checkpoint has completed and FAIL_AFTER_RECORDS
     *  records have passed. Sleeps 1 ms per record while armed so that checkpoints complete
     *  before the bounded input runs out. State is static because the embedded Flink
     *  cluster runs in this JVM. */
    static class FailOnce extends RichMapFunction<CustomerProfileChangeEvent, CustomerProfileChangeEvent>
            implements CheckpointListener {
        static final AtomicBoolean failed = new AtomicBoolean();
        static final AtomicInteger completedCheckpoints = new AtomicInteger();
        static final AtomicLong recordsSeen = new AtomicLong();
        static volatile int checkpointsBeforeFailure;
        static volatile long recordsBeforeFailure;

        private final boolean armed;

        FailOnce(boolean armed) {
            this.armed = armed;
        }

        static void reset() {
            failed.set(false);
            completedCheckpoints.set(0);
            recordsSeen.set(0);
            checkpointsBeforeFailure = 0;
            recordsBeforeFailure = 0;
        }

        @Override
        public CustomerProfileChangeEvent map(CustomerProfileChangeEvent event) throws Exception {
            long n = recordsSeen.incrementAndGet();
            if (armed && !failed.get()) {
                Thread.sleep(1);
                if (completedCheckpoints.get() >= 1 && n >= FAIL_AFTER_RECORDS
                        && failed.compareAndSet(false, true)) {
                    checkpointsBeforeFailure = completedCheckpoints.get();
                    recordsBeforeFailure = n;
                    throw new RuntimeException("injected failure after " + n + " records and "
                            + checkpointsBeforeFailure + " completed checkpoints");
                }
            }
            return event;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            completedCheckpoints.incrementAndGet();
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {
        }
    }

    /** The chk-N directory with the highest N that holds a _metadata file. */
    static Path latestCompletedCheckpoint(Path checkpointDir) throws IOException {
        try (Stream<Path> walk = Files.walk(checkpointDir)) {
            List<Path> completed = walk
                .filter(p -> p.getFileName().toString().equals("_metadata"))
                .map(Path::getParent)
                .filter(p -> p.getFileName().toString().startsWith("chk-"))
                .collect(Collectors.toList());
            assertTrue(!completed.isEmpty(), "run 1 left no completed checkpoint under " + checkpointDir);
            completed.sort((a, b) -> Long.compare(chkNumber(b), chkNumber(a)));
            return completed.get(0);
        }
    }

    static long chkNumber(Path chk) {
        return Long.parseLong(chk.getFileName().toString().substring("chk-".length()));
    }

    // ------------------------------------------------------------------- kafka

    static List<CustomerProfileChangeEvent> events() {
        List<CustomerProfileChangeEvent> out = new ArrayList<>();
        for (int c = 0; c < CUSTOMERS; c++) {
            String customerId = String.format("%05d", 10_000 + c);
            for (int i = 1; i <= EVENTS_PER_CUSTOMER; i++) {
                out.add(new CustomerProfileChangeEvent(
                        customerId, 1_700_000_000_000L + i * 1000L, i,
                        "user" + c + "." + i + "@example.com",
                        i % 7 == 0 ? "555-" + String.format("%04d", i) : null,
                        i % 11 == 0 ? i + " Main St" : null));
            }
        }
        return out;
    }

    /** Create the topic, wait until every partition has a leader, produce the events keyed
     *  by customer_id, check every send, and return the record count the broker reports
     *  (the sum of the partition end offsets), so the input size is measured, not assumed. */
    static long produce(String topic, List<CustomerProfileChangeEvent> events,
                        CustomerProfileChangeEvent resend) throws Exception {
        Properties admin = new Properties();
        admin.put("bootstrap.servers", bootstrap());
        try (AdminClient client = AdminClient.create(admin)) {
            client.createTopics(Collections.singletonList(new NewTopic(topic, PARTITIONS, (short) 1)))
                  .all().get(30, TimeUnit.SECONDS);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (true) {
                TopicDescription d = client.describeTopics(Collections.singletonList(topic))
                        .allTopicNames().get(10, TimeUnit.SECONDS).get(topic);
                boolean ready = d.partitions().size() == PARTITIONS;
                for (TopicPartitionInfo p : d.partitions()) {
                    ready &= p.leader() != null;
                }
                if (ready) break;
                assertTrue(System.nanoTime() < deadline, "partitions of " + topic + " never got a leader");
                Thread.sleep(200);
            }

            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap());
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            // One request in flight: on a just-created partition the first batch can fail
            // transiently while later batches land, after which the idempotent producer's
            // retry of the first batch is rejected as out of order until it expires.
            props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            List<Future<RecordMetadata>> sent = new ArrayList<>();
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                for (CustomerProfileChangeEvent e : events) {
                    sent.add(producer.send(new ProducerRecord<>(topic, e.getCustomerId(), JSON.writeValueAsString(e))));
                }
                if (resend != null) {
                    // The same event again, as a producer retry would send it.
                    sent.add(producer.send(new ProducerRecord<>(topic, resend.getCustomerId(), JSON.writeValueAsString(resend))));
                }
                producer.flush();
                for (Future<RecordMetadata> f : sent) {
                    f.get(60, TimeUnit.SECONDS);   // throws if the broker rejected the record
                }
            }

            Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
            for (int p = 0; p < PARTITIONS; p++) {
                latest.put(new TopicPartition(topic, p), OffsetSpec.latest());
            }
            long total = 0;
            for (ListOffsetsResult.ListOffsetsResultInfo info
                    : client.listOffsets(latest).all().get(10, TimeUnit.SECONDS).values()) {
                total += info.offset();
            }
            return total;
        }
    }

    // ----------------------------------------------------------------- iceberg

    static class TableSnapshot {
        final List<Object[]> rows;
        final int snapshots;

        TableSnapshot(List<Object[]> rows, int snapshots) {
            this.rows = rows;
            this.snapshots = snapshots;
        }
    }

    static TableSnapshot readTable(String warehouse) throws IOException {
        List<Object[]> rows = new ArrayList<>();
        TableLoader loader = CustomerProfileAuditJob.buildTableLoader(warehouse, "");
        loader.open();
        Table table = loader.loadTable();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record r : records) {
                LocalDateTime ts = (LocalDateTime) r.getField("event_timestamp");
                rows.add(new Object[]{
                        r.getField("customer_id"),
                        ts.toInstant(ZoneOffset.UTC).toEpochMilli(),
                        r.getField("event_sequence"),
                        r.getField("email"),
                        r.getField("phone"),
                        r.getField("address")});
            }
        }
        int snapshots = 0;
        for (Snapshot ignored : table.snapshots()) {
            snapshots++;
        }
        loader.close();
        return new TableSnapshot(rows, snapshots);
    }

    static Set<String> distinctPairs(List<Object[]> rows) {
        Set<String> pairs = new HashSet<>();
        for (Object[] r : rows) {
            pairs.add(r[0] + ":" + r[2]);
        }
        return pairs;
    }

    /** Load the Iceberg rows into DuckDB and count what sql/dedup.sql returns. */
    static long dedupRowCount(List<Object[]> rows) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE customer_profile_changes ("
                    + "customer_id VARCHAR, event_timestamp BIGINT, event_sequence BIGINT,"
                    + "email VARCHAR, phone VARCHAR, address VARCHAR)");
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO customer_profile_changes VALUES (?,?,?,?,?,?)")) {
                for (Object[] row : rows) {
                    for (int i = 0; i < 6; i++) ps.setObject(i + 1, row[i]);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            String sql = com.example.audit.demo.DemoRunner.readQuery("sql/dedup.sql");
            long count = 0;
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) count++;
            }
            return count;
        }
    }

    static void writeEvidence(String relPath, String text) throws IOException {
        Path path = Path.of(relPath);
        Files.createDirectories(path.getParent());
        Files.writeString(path, text);
    }
}
