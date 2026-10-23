package com.example.audit.aws;

import org.apache.avro.Schema;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFilesActionResult;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.actions.Actions;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.AppInfoParser;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Runner-side measurements for the AWS scale run. Each mode appends one CSV row (or one per
 * snapshot) to --out. The Kafka modes follow PipelineBenchmark's kafka mode: the same topic
 * settings, the same producer settings and send-to-acknowledgement timing, and the same
 * settled describeLogDirs reading. The Iceberg modes follow its iceberg mode: the same
 * expiration (retainLast 1) and the same compaction (Actions.rewriteDataFiles defaults),
 * with file counts and bytes listed from the table location.
 *
 * Modes: topic-create, produce, log-bytes, topic-delete, tables-create, load-c, snapshots,
 * stats, expire, compact, drop-table. Arguments are --key value pairs.
 */
public class RunnerTools {

    public static void main(String[] args) throws Exception {
        if (args.length == 0) throw new IllegalArgumentException("usage: RunnerTools <mode> --key value ...");
        Map<String, String> o = new HashMap<>();
        for (int i = 1; i + 1 < args.length; i += 2) o.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        switch (args[0]) {
            case "topic-create": topicCreate(o); break;
            case "produce": produce(o); break;
            case "log-bytes": logBytes(o); break;
            case "topic-delete": topicDelete(o); break;
            case "tables-create": tablesCreate(o); break;
            case "load-c": loadC(o); break;
            case "snapshots": snapshots(o); break;
            case "stats": stats(o); break;
            case "expire": expire(o); break;
            case "compact": compact(o); break;
            case "drop-table": dropTable(o); break;
            default: throw new IllegalArgumentException("unknown mode " + args[0]);
        }
    }

    static String req(Map<String, String> o, String k) {
        String v = o.get(k);
        if (v == null) throw new IllegalArgumentException("missing --" + k);
        return v;
    }

    static int intArg(Map<String, String> o, String k, int d) {
        return o.containsKey(k) ? Integer.parseInt(o.get(k)) : d;
    }

    static AdminClient admin(Map<String, String> o) {
        Properties ap = new Properties();
        ap.put("bootstrap.servers", req(o, "bootstrap"));
        return AdminClient.create(ap);
    }

    // ------------------------------------------------------------------- kafka

    static void topicCreate(Map<String, String> o) throws Exception {
        String topic = req(o, "topic");
        int partitions = intArg(o, "partitions", 12);
        short rf = (short) intArg(o, "replication-factor", 3);
        try (AdminClient admin = admin(o)) {
            NewTopic nt = new NewTopic(topic, partitions, rf)
                .configs(Map.of("min.insync.replicas", String.valueOf(Math.min(2, rf))));
            admin.createTopics(Collections.singletonList(nt)).all().get(60, TimeUnit.SECONDS);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
            while (true) {
                TopicDescription d = describe(admin, topic);
                boolean ready = d != null && d.partitions().size() == partitions;
                if (ready) {
                    for (TopicPartitionInfo p : d.partitions()) ready &= p.leader() != null && p.isr().size() == rf;
                }
                if (ready) break;
                if (System.nanoTime() > deadline) throw new IllegalStateException("topic " + topic + " never became ready");
                Thread.sleep(200);
            }
            System.out.println("created " + topic + " partitions=" + partitions + " rf=" + rf);
        }
    }

    static TopicDescription describe(AdminClient admin, String topic) throws Exception {
        try {
            return admin.describeTopics(Collections.singletonList(topic)).allTopicNames().get(10, TimeUnit.SECONDS).get(topic);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException) return null;
            throw e;
        }
    }

    /** Sends the events of steps 1..H in order, the first --entities events of each step, with
     *  acks=all, timing each send() call to its acknowledgement callback. */
    static void produce(Map<String, String> o) throws Exception {
        String topic = req(o, "topic");
        String compression = req(o, "compression");
        Path dir = Path.of(req(o, "steps-dir"));
        int steps = Integer.parseInt(req(o, "steps"));
        int entities = Integer.parseInt(req(o, "entities"));
        long total = (long) steps * entities;
        if (total > Integer.MAX_VALUE) throw new IllegalArgumentException("too many events for one run: " + total);
        int n = (int) total;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, req(o, "bootstrap"));
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        long[] lat = new long[n];
        AtomicReference<Exception> err = new AtomicReference<>();
        long valueBytes = 0;
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props);
        producer.partitionsFor(topic); // metadata fetched before the clock starts
        long startMs = System.currentTimeMillis();
        long start = System.nanoTime();
        int i = 0;
        for (int s = 1; s <= steps; s++) {
            try (StepFiles.Reader r = new StepFiles.Reader(StepFiles.step(dir, s))) {
                for (int e = 0; e < entities; e++) {
                    byte[] v = r.next();
                    if (v == null) throw new IllegalStateException("step " + s + " has only " + e + " events");
                    valueBytes += v.length;
                    final int idx = i++;
                    final long t0 = System.nanoTime();
                    producer.send(new ProducerRecord<>(topic, StepFiles.customerId(v), v), (md, ex) -> {
                        lat[idx] = System.nanoTime() - t0;
                        if (ex != null) err.compareAndSet(null, ex);
                    });
                }
            }
        }
        producer.flush();
        long wall = System.nanoTime() - start;
        long endMs = System.currentTimeMillis();
        producer.close();
        if (err.get() != null) throw err.get();
        long[] sorted = lat.clone();
        Arrays.sort(sorted);

        Map<String, Object> row = new LinkedHashMap<>();
        row.put("label", req(o, "label"));
        row.put("design", req(o, "design"));
        row.put("compression", compression);
        row.put("topic", topic);
        row.put("events", n);
        row.put("value_bytes_sent", valueBytes);
        row.put("acks", "all");
        row.put("kafka_client_version", AppInfoParser.getVersion());
        row.put("producer_latency_p50_ns", nearestRank(sorted, 50));
        row.put("producer_latency_p95_ns", nearestRank(sorted, 95));
        row.put("producer_latency_p99_ns", nearestRank(sorted, 99));
        row.put("producer_latency_max_ns", sorted[n - 1]);
        row.put("produce_wall_ns", wall);
        row.put("produce_start_epoch_ms", startMs);
        row.put("produce_end_epoch_ms", endMs);
        appendCsv(Path.of(req(o, "out")), row);
        System.out.println("produced " + row);
    }

    static long nearestRank(long[] sorted, int pct) {
        int k = (int) Math.ceil(pct / 100.0 * sorted.length);
        return sorted[Math.max(1, k) - 1];
    }

    /** The topic's log bytes per replica from describeLogDirs, once every replica of every
     *  partition reports the same size as its leader, and the topic's end offsets. */
    static void logBytes(Map<String, String> o) throws Exception {
        String topic = req(o, "topic");
        short rf = (short) intArg(o, "replication-factor", 3);
        try (AdminClient admin = admin(o)) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(300);
            while (true) {
                TopicDescription d = describe(admin, topic);
                List<Integer> brokers = admin.describeCluster().nodes().get(10, TimeUnit.SECONDS).stream()
                    .map(Node::id).collect(Collectors.toList());
                Map<Integer, Map<String, LogDirDescription>> dirs =
                    admin.describeLogDirs(brokers).allDescriptions().get(60, TimeUnit.SECONDS);
                Map<TopicPartition, Map<Integer, Long>> sizes = new HashMap<>();
                Map<Integer, Long> perBroker = new TreeMap<>();
                for (Map.Entry<Integer, Map<String, LogDirDescription>> b : dirs.entrySet()) {
                    for (LogDirDescription ld : b.getValue().values()) {
                        for (Map.Entry<TopicPartition, ReplicaInfo> r : ld.replicaInfos().entrySet()) {
                            if (r.getKey().topic().equals(topic)) {
                                sizes.computeIfAbsent(r.getKey(), k -> new HashMap<>()).put(b.getKey(), r.getValue().size());
                                perBroker.merge(b.getKey(), r.getValue().size(), Long::sum);
                            }
                        }
                    }
                }
                long oneCopy = 0, all = 0;
                boolean settled = d != null && sizes.size() == d.partitions().size();
                if (d != null) {
                    for (TopicPartitionInfo p : d.partitions()) {
                        Map<Integer, Long> s = sizes.getOrDefault(new TopicPartition(topic, p.partition()), Map.of());
                        Long leader = s.get(p.leader().id());
                        settled &= leader != null && s.size() == rf && s.values().stream().allMatch(leader::equals);
                        if (leader != null) oneCopy += leader;
                        all += s.values().stream().mapToLong(Long::longValue).sum();
                    }
                }
                if (settled) {
                    Map<TopicPartition, OffsetSpec> req = new HashMap<>();
                    for (TopicPartitionInfo p : d.partitions()) req.put(new TopicPartition(topic, p.partition()), OffsetSpec.latest());
                    long records = 0;
                    for (ListOffsetsResult.ListOffsetsResultInfo info : admin.listOffsets(req).all().get(30, TimeUnit.SECONDS).values()) {
                        records += info.offset();
                    }
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("label", req(o, "label"));
                    row.put("topic", topic);
                    row.put("partitions", d.partitions().size());
                    row.put("replication_factor", rf);
                    row.put("min_insync_replicas", Math.min(2, rf));
                    row.put("records", records);
                    row.put("topic_log_bytes_one_copy", oneCopy);
                    row.put("topic_log_bytes_all_replicas", all);
                    row.put("topic_log_bytes_per_broker", perBroker.entrySet().stream()
                        .map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(";")));
                    row.put("measured_epoch_ms", System.currentTimeMillis());
                    appendCsv(Path.of(req(o, "out")), row);
                    System.out.println("log bytes " + row);
                    return;
                }
                if (System.nanoTime() > deadline) throw new IllegalStateException("replica sizes never settled for " + topic);
                Thread.sleep(1000);
            }
        }
    }

    static void topicDelete(Map<String, String> o) throws Exception {
        try (AdminClient admin = admin(o)) {
            admin.deleteTopics(Collections.singletonList(req(o, "topic"))).all().get(120, TimeUnit.SECONDS);
            System.out.println("deleted " + req(o, "topic"));
        }
    }

    // ----------------------------------------------------------------- iceberg

    static CatalogLoader catalog(Map<String, String> o) {
        return Catalogs.loader(o.getOrDefault("catalog", "glue"), req(o, "warehouse"));
    }

    /** --tables a=A,b=B,c=C : table name = design schema. */
    static void tablesCreate(Map<String, String> o) throws Exception {
        CatalogLoader loader = catalog(o);
        for (String spec : req(o, "tables").split(",")) {
            String[] kv = spec.split("=");
            Schema avro = AvroRows.designSchema(kv[1]);
            boolean created = Catalogs.createTable(loader, req(o, "database"), kv[0], AvroRows.icebergSchema(avro));
            System.out.println((created ? "created " : "exists ") + req(o, "database") + "." + kv[0] + " schema " + kv[1]);
        }
    }

    /** Rows for the bounded load; read by the job running in this JVM. */
    static volatile List<RowData> rowsForLoad = Collections.emptyList();

    static final class FromHolder implements MapFunction<Long, RowData> {
        @Override
        public RowData map(Long i) {
            return rowsForLoad.get(i.intValue());
        }
    }

    /** Appends the generator's current-state rows (one per entity) to table C in one bounded
     *  Flink job through the same Iceberg sink, so C has one commit. */
    static void loadC(Map<String, String> o) throws Exception {
        Schema avro = AvroRows.designSchema("C");
        AvroRows.Decoder dec = new AvroRows.Decoder(avro);
        List<RowData> rows = new ArrayList<>();
        try (StepFiles.Reader r = new StepFiles.Reader(Path.of(req(o, "file")))) {
            for (byte[] v = r.next(); v != null; v = r.next()) rows.add(dec.row(v));
        }
        int expected = Integer.parseInt(req(o, "entities"));
        if (rows.size() != expected) throw new IllegalStateException("C file holds " + rows.size() + " rows, expected " + expected);
        rowsForLoad = rows;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(intArg(o, "parallelism", 4));
        DataStream<RowData> stream = env.fromSequence(0, rows.size() - 1)
            .map(new FromHolder()).returns(InternalTypeInfo.of(AvroRows.rowType(avro)));
        FlinkSink.forRowData(stream).tableLoader(Catalogs.tableLoader(catalog(o), req(o, "database"), req(o, "table")))
            .uidPrefix("iceberg-audit-trail").overwrite(false).append();
        env.execute("load current-state table " + req(o, "table"));
        rowsForLoad = Collections.emptyList();
        Table t = Catalogs.load(catalog(o), req(o, "database"), req(o, "table"));
        System.out.println("loaded " + rows.size() + " rows; table total-records "
            + t.currentSnapshot().summary().get("total-records"));
    }

    /** One CSV row per snapshot: commit time and the record and file counts of its summary. */
    static void snapshots(Map<String, String> o) throws Exception {
        Table t = Catalogs.load(catalog(o), req(o, "database"), req(o, "table"));
        for (Snapshot s : t.snapshots()) {
            Map<String, String> sum = s.summary();
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("label", req(o, "label"));
            row.put("table", req(o, "table"));
            row.put("snapshot_id", s.snapshotId());
            row.put("parent_id", s.parentId());
            row.put("sequence_number", s.sequenceNumber());
            row.put("timestamp_ms", s.timestampMillis());
            row.put("operation", s.operation());
            for (String k : List.of("added-records", "total-records", "added-data-files", "total-data-files",
                "added-files-size", "total-files-size", "flink.max-committed-checkpoint-id", "flink.job-id")) {
                row.put(k.replace('-', '_').replace('.', '_'), sum.getOrDefault(k, ""));
            }
            appendCsv(Path.of(req(o, "out")), row);
        }
        System.out.println("snapshots of " + req(o, "table") + " written");
    }

    /** Snapshot, manifest and data-file counts, and the bytes of every file under the table's
     *  metadata/ and data/ prefixes, classified as PipelineBenchmark.record classifies them. */
    static void stats(Map<String, String> o) throws Exception {
        Table t = Catalogs.load(catalog(o), req(o, "database"), req(o, "table"));
        if (!(t.io() instanceof SupportsPrefixOperations)) throw new IllegalStateException("FileIO cannot list prefixes");
        SupportsPrefixOperations io = (SupportsPrefixOperations) t.io();
        long[] json = new long[2], lists = new long[2], manifests = new long[2], other = new long[2], data = new long[2], dataOther = new long[2];
        for (FileInfo f : io.listPrefix(t.location() + "/metadata/")) {
            String name = f.location().substring(f.location().lastIndexOf('/') + 1);
            long[] bucket = name.endsWith(".metadata.json") ? json
                : name.startsWith("snap-") && name.endsWith(".avro") ? lists
                : name.endsWith(".avro") ? manifests : other;
            bucket[0]++;
            bucket[1] += f.size();
        }
        for (FileInfo f : io.listPrefix(t.location() + "/data/")) {
            long[] bucket = f.location().endsWith(".parquet") ? data : dataOther;
            bucket[0]++;
            bucket[1] += f.size();
        }
        int snapshots = 0;
        for (Snapshot ignored : t.snapshots()) snapshots++;
        Snapshot cur = t.currentSnapshot();
        List<ManifestFile> current = cur.allManifests(t.io());
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("label", req(o, "label"));
        row.put("table", req(o, "table"));
        row.put("state", req(o, "state"));
        row.put("snapshots", snapshots);
        row.put("metadata_json_files", json[0]);
        row.put("metadata_json_bytes", json[1]);
        row.put("manifest_list_files", lists[0]);
        row.put("manifest_list_bytes", lists[1]);
        row.put("manifest_files", manifests[0]);
        row.put("manifest_bytes", manifests[1]);
        row.put("other_metadata_files", other[0]);
        row.put("other_metadata_bytes", other[1]);
        row.put("metadata_total_bytes", json[1] + lists[1] + manifests[1] + other[1]);
        row.put("manifests_in_current_snapshot", current.size());
        row.put("data_files_in_current_snapshot", Long.parseLong(cur.summary().get("total-data-files")));
        row.put("data_files_on_s3", data[0]);
        row.put("data_bytes_on_s3", data[1]);
        row.put("other_data_prefix_files", dataOther[0]);
        row.put("current_snapshot_files_size", cur.summary().get("total-files-size"));
        row.put("rows", cur.summary().get("total-records"));
        row.put("measured_epoch_ms", System.currentTimeMillis());
        appendCsv(Path.of(req(o, "out")), row);
        System.out.println("stats " + row);
    }

    static void expire(Map<String, String> o) throws Exception {
        Table t = Catalogs.load(catalog(o), req(o, "database"), req(o, "table"));
        t.expireSnapshots().expireOlderThan(System.currentTimeMillis() + 1).retainLast(1).commit();
        t.refresh();
        System.out.println("expired " + req(o, "table") + "; current snapshot " + t.currentSnapshot().snapshotId());
    }

    static void compact(Map<String, String> o) throws Exception {
        Table t = Catalogs.load(catalog(o), req(o, "database"), req(o, "table"));
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(intArg(o, "parallelism", 4));
        long t0 = System.nanoTime();
        RewriteDataFilesActionResult r = Actions.forTable(env, t).rewriteDataFiles().execute();
        long wall = System.nanoTime() - t0;
        t.refresh();
        System.out.println("compacted " + req(o, "table") + ": rewrote " + r.deletedDataFiles().size()
            + " data files into " + r.addedDataFiles().size() + " in " + wall + " ns");
    }

    static void dropTable(Map<String, String> o) {
        boolean dropped = Catalogs.drop(catalog(o), req(o, "database"), req(o, "table"));
        System.out.println((dropped ? "dropped " : "not found ") + req(o, "table"));
    }

    // ----------------------------------------------------------------- helpers

    static synchronized void appendCsv(Path out, Map<String, Object> row) throws IOException {
        boolean header = !Files.exists(out) || Files.size(out) == 0;
        if (out.getParent() != null) Files.createDirectories(out.getParent());
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND))) {
            if (header) w.println(String.join(",", row.keySet()));
            w.println(row.values().stream().map(String::valueOf).collect(Collectors.joining(",")));
        }
    }
}
