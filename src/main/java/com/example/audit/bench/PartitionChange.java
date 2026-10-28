package com.example.audit.bench;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.AppInfoParser;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Partition changes under a running job: the benchmark's events go to Kafka keyed by
 * customer_id while the Flink job lands them in Iceberg, and partway through, the way
 * records are assigned to partitions changes.
 *
 * Scenario "partition-increase": the topic starts with --partitions-before partitions. The
 * first half of the events, in the file's event-time order, is sent with the producer's
 * default partitioner (murmur2 of the key, modulo the partition count). The topic is then
 * raised to --partitions-after partitions and a new producer, again with the default
 * partitioner, sends the rest.
 *
 * Scenario "partitioner-change": the topic keeps --partitions-before partitions. The first
 * half is sent with the default partitioner, the rest by a producer configured with
 * RoundRobinPartitioner, which ignores the key.
 *
 * The job is PipelineBenchmark's topology (KafkaSource from the earliest offsets, the Iceberg
 * FlinkSink into a table partitioned by day(event_timestamp)), with partition discovery every
 * --discovery-interval-ms so that it reads partitions added while it runs. It starts before
 * the first event and is cancelled once Iceberg holds every event.
 *
 * Writes into --work: records.csv (phase, customer_id, event_sequence, partition and offset of
 * every record, from the producer's acknowledgements), files.txt (the data files of the
 * table's current snapshot) and run.json (topic layout, end offsets, rows and settings).
 * bench/partition_change.py runs the LAG checks over these files.
 */
public class PartitionChange {

    private static final ObjectMapper JSON = new ObjectMapper();
    static final String DEFAULT_PARTITIONER = "default (murmur2 of the key modulo the partition count)";

    public static void main(String[] args) throws Exception {
        Map<String, String> o = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            o.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        }
        String scenario = o.get("scenario");
        if (!"partition-increase".equals(scenario) && !"partitioner-change".equals(scenario)) {
            throw new IllegalArgumentException("--scenario partition-increase|partitioner-change");
        }
        String bootstrap = o.getOrDefault("bootstrap", "localhost:9092");
        int before = Integer.parseInt(o.getOrDefault("partitions-before", "3"));
        int after = "partition-increase".equals(scenario) ? Integer.parseInt(o.getOrDefault("partitions-after", "6")) : before;
        int parallelism = Integer.parseInt(o.getOrDefault("parallelism", "3"));
        long checkpointMs = Long.parseLong(o.getOrDefault("checkpoint-interval-ms", "1000"));
        long discoveryMs = Long.parseLong(o.getOrDefault("discovery-interval-ms", "1000"));
        Path work = Path.of(o.get("work"));
        Files.createDirectories(work);

        PipelineBenchmark.Events ev = PipelineBenchmark.Events.read(Path.of(o.get("events")));
        int n = ev.values.size();
        int half = n / 2;
        String topic = "partition-change-" + scenario + "-" + UUID.randomUUID().toString().substring(0, 8);

        Properties ap = new Properties();
        ap.put("bootstrap.servers", bootstrap);
        try (AdminClient admin = AdminClient.create(ap)) {
            PipelineBenchmark.createTopic(admin, topic, before, (short) 1);

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(parallelism);
            env.enableCheckpointing(checkpointMs, CheckpointingMode.EXACTLY_ONCE);
            KafkaSource<RowData> source = KafkaSource.<RowData>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId("partition-change-" + UUID.randomUUID())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setProperty("partition.discovery.interval.ms", String.valueOf(discoveryMs))
                .setValueOnlyDeserializer(new PipelineBenchmark.AvroRowDataDeserializer(ev.avro, ev.rowType))
                .build();
            DataStream<RowData> rows = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka events")
                .uid("kafka-events");
            String warehouse = "file://" + work.resolve("warehouse").toAbsolutePath();
            TableLoader loader = PipelineBenchmark.createTable(warehouse, "events", ev.iceberg);
            FlinkSink.forRowData(rows).tableLoader(loader).uidPrefix("iceberg-audit-trail").overwrite(false).append();
            JobClient job = env.executeAsync("partition-change " + topic);

            int[] partition = new int[n];
            long[] offset = new long[n];
            Map<String, Object> run = new LinkedHashMap<>();
            try {
                PipelineBenchmark.waitFor(() -> job.getJobStatus().get() == JobStatus.RUNNING ? Boolean.TRUE : null,
                    120_000, 100);
                produce(bootstrap, topic, ev, 0, half, null, partition, offset);
                if (after != before) {
                    admin.createPartitions(Map.of(topic, NewPartitions.increaseTo(after))).all().get(60, TimeUnit.SECONDS);
                    PipelineBenchmark.waitFor(() -> {
                        TopicDescription d = PipelineBenchmark.describe(admin, topic);
                        if (d == null || d.partitions().size() != after) return null;
                        for (TopicPartitionInfo p : d.partitions()) {
                            if (p.leader() == null) return null;
                        }
                        return Boolean.TRUE;
                    }, 60_000, 200);
                }
                String phase2 = "partitioner-change".equals(scenario) ? RoundRobinPartitioner.class.getName() : null;
                produce(bootstrap, topic, ev, half, n, phase2, partition, offset);

                Map<String, Long> endOffsets = endOffsets(admin, topic, after);
                long inTopic = endOffsets.values().stream().mapToLong(Long::longValue).sum();
                if (inTopic != n) throw new IllegalStateException("topic holds " + inTopic + " records, expected " + n);

                Table table = PipelineBenchmark.load(loader);
                PipelineBenchmark.waitFor(() -> PipelineBenchmark.totalRecords(table) >= n ? Boolean.TRUE : null,
                    600_000, 200);
                long icebergRows = PipelineBenchmark.totalRecords(table);
                if (icebergRows != n) throw new IllegalStateException("Iceberg holds " + icebergRows + " rows, expected " + n);
                writeFiles(table, work.resolve("files.txt"));
                int snapshots = 0;
                for (Snapshot ignored : table.snapshots()) snapshots++;

                run.put("scenario", scenario);
                run.put("events", n);
                run.put("phase1_events", half);
                run.put("phase2_events", n - half);
                run.put("partitions_before", before);
                run.put("partitions_after", after);
                run.put("phase1_partitioner", DEFAULT_PARTITIONER);
                run.put("phase2_partitioner", phase2 == null ? DEFAULT_PARTITIONER : phase2);
                run.put("end_offsets", endOffsets);
                run.put("records_in_topic", inTopic);
                run.put("iceberg_rows", icebergRows);
                run.put("iceberg_snapshots", snapshots);
                run.put("flink_parallelism", parallelism);
                run.put("checkpoint_interval_ms", checkpointMs);
                run.put("partition_discovery_interval_ms", discoveryMs);
                run.put("kafka_client", AppInfoParser.getVersion());
                run.put("flink", EnvironmentInformation.getVersion());
                run.put("iceberg", IcebergBuild.version());
            } finally {
                job.cancel().get(60, TimeUnit.SECONDS);
                try {
                    job.getJobExecutionResult().get(60, TimeUnit.SECONDS);
                } catch (Exception expected) {
                    // cancelled
                }
                admin.deleteTopics(Collections.singletonList(topic)).all().get(60, TimeUnit.SECONDS);
            }

            try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(work.resolve("records.csv"), StandardCharsets.UTF_8))) {
                w.println("phase,customer_id,event_sequence,partition,offset");
                for (int i = 0; i < n; i++) {
                    w.println((i < half ? 1 : 2) + "," + new String(ev.keys.get(i), StandardCharsets.UTF_8) + ","
                        + ev.sequences.get(i) + "," + partition[i] + "," + offset[i]);
                }
            }
            JSON.writerWithDefaultPrettyPrinter().writeValue(work.resolve("run.json").toFile(), run);
            System.out.printf("%s: %d events, partitions %d -> %d, %d Iceberg rows%n", scenario, n, before, after,
                run.get("iceberg_rows"));
        }
    }

    /** Sends events [from, to) keyed by customer_id with acks=all, one request in flight, and
     *  the given partitioner class (null for the default); records each record's partition and
     *  offset from its acknowledgement. A new producer per phase fetches fresh metadata, so it
     *  sees the partition count as it is when the phase starts. */
    static void produce(String bootstrap, String topic, PipelineBenchmark.Events ev, int from, int to,
                        String partitioner, int[] partition, long[] offset) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        if (partitioner != null) props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitioner);
        AtomicReference<Exception> err = new AtomicReference<>();
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            for (int i = from; i < to; i++) {
                final int idx = i;
                producer.send(new ProducerRecord<>(topic, ev.keys.get(i), ev.values.get(i)), (md, ex) -> {
                    if (ex != null) {
                        err.compareAndSet(null, ex);
                        return;
                    }
                    partition[idx] = md.partition();
                    offset[idx] = md.offset();
                });
            }
            producer.flush();
        }
        if (err.get() != null) throw err.get();
    }

    static Map<String, Long> endOffsets(AdminClient admin, String topic, int partitions) throws Exception {
        Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
        for (int p = 0; p < partitions; p++) latest.put(new TopicPartition(topic, p), OffsetSpec.latest());
        Map<String, Long> out = new LinkedHashMap<>();
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> res = admin.listOffsets(latest).all().get(30, TimeUnit.SECONDS);
        for (int p = 0; p < partitions; p++) out.put(String.valueOf(p), res.get(new TopicPartition(topic, p)).offset());
        return out;
    }

    static void writeFiles(Table table, Path out) throws IOException {
        table.refresh();
        List<String> files = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask t : tasks) files.add(Path.of(URI.create(t.file().location())).toString());
        }
        Files.write(out, files, StandardCharsets.UTF_8);
    }
}
