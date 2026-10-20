package com.example.audit.bench;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.actions.Actions;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Pipeline-level measurements for the previous-value schema (A) and the audit-trail
 * schema (B), on the Kafka to Flink to Iceberg topology of CustomerProfileAuditJob.
 *
 * Events are read from an Avro container file written by bench/pipeline_events.py
 * (same generator, config and seed as bench/benchmark.py) and sent to Kafka as Avro
 * binary datums with no framing, keyed by customer_id.
 *
 * Mode "kafka", per run: create a topic, produce every event (acks=all) timing each
 * send to its acknowledgement, read the topic's log bytes per replica from the brokers,
 * then run the Flink job (KafkaSource, noWatermarks, exactly-once checkpoints, Iceberg
 * FlinkSink into a table partitioned by day(event_timestamp) and sorted by customer_id)
 * until Iceberg holds every event. Throughput and checkpoint statistics come from the
 * Flink REST API of the embedded cluster.
 *
 * Mode "iceberg", per run: append the same events to a fresh table in a fixed number of
 * bounded Flink jobs, one Iceberg commit each, then expire snapshots, compact data files
 * and expire again, recording metadata and data files after each step and checking that
 * the full history still answers the LAG and LAST_VALUE queries identically.
 *
 * Each mode appends one row per run (or per run and step) to a raw CSV.
 */
public class PipelineBenchmark {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    /** Rows for the bounded appends in iceberg mode; read by the job running in this JVM. */
    static volatile List<RowData> rowsForAppend = Collections.emptyList();

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("usage: PipelineBenchmark kafka|iceberg --key value ...");
        }
        Map<String, String> o = parse(args);
        Events ev = Events.read(Path.of(o.get("events")));
        if (args[0].equals("kafka")) {
            kafkaMode(o, ev);
        } else if (args[0].equals("iceberg")) {
            icebergMode(o, ev);
        } else {
            throw new IllegalArgumentException("unknown mode " + args[0]);
        }
    }

    private static Map<String, String> parse(String[] args) {
        Map<String, String> o = new HashMap<>();
        for (int i = 1; i + 1 < args.length; i += 2) {
            o.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        }
        for (String k : List.of("events", "label", "model", "out", "work")) {
            if (!o.containsKey(k)) throw new IllegalArgumentException("missing --" + k);
        }
        return o;
    }

    private static int intArg(Map<String, String> o, String k, int d) {
        return o.containsKey(k) ? Integer.parseInt(o.get(k)) : d;
    }

    private static long longArg(Map<String, String> o, String k, long d) {
        return o.containsKey(k) ? Long.parseLong(o.get(k)) : d;
    }

    // ------------------------------------------------------------------ events

    /** The events of one schema, in file order, as Avro datums and as Iceberg rows. */
    static final class Events {
        final Schema avro;
        final org.apache.iceberg.Schema iceberg;
        final RowType rowType;
        final Kind[] kinds;
        final List<byte[]> keys = new ArrayList<>();
        final List<byte[]> values = new ArrayList<>();
        final List<Long> sequences = new ArrayList<>();
        final List<RowData> rows = new ArrayList<>();

        private Events(Schema avro) {
            this.avro = avro;
            this.iceberg = icebergSchema(avro);
            this.rowType = FlinkSchemaUtil.convert(iceberg);
            this.kinds = kinds(avro);
        }

        static Events read(Path file) throws IOException {
            try (DataFileReader<GenericRecord> reader =
                     new DataFileReader<>(file.toFile(), new GenericDatumReader<>())) {
                Events ev = new Events(reader.getSchema());
                GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(ev.avro);
                ByteArrayOutputStream buf = new ByteArrayOutputStream();
                BinaryEncoder enc = null;
                for (GenericRecord r : reader) {
                    buf.reset();
                    enc = EncoderFactory.get().binaryEncoder(buf, enc);
                    writer.write(r, enc);
                    enc.flush();
                    ev.values.add(buf.toByteArray());
                    ev.keys.add(r.get("customer_id").toString().getBytes(StandardCharsets.UTF_8));
                    ev.sequences.add((Long) r.get("event_sequence"));
                    ev.rows.add(toRow(r, ev.kinds));
                }
                return ev;
            }
        }
    }

    enum Kind { STRING, LONG, DOUBLE, BOOLEAN, TIMESTAMP }

    static Kind[] kinds(Schema avro) {
        Kind[] k = new Kind[avro.getFields().size()];
        for (int i = 0; i < k.length; i++) {
            Schema.Field f = avro.getFields().get(i);
            if (f.name().equals("event_timestamp")) {
                k[i] = Kind.TIMESTAMP;
                continue;
            }
            switch (base(f.schema()).getType()) {
                case STRING: k[i] = Kind.STRING; break;
                case LONG: k[i] = Kind.LONG; break;
                case DOUBLE: k[i] = Kind.DOUBLE; break;
                case BOOLEAN: k[i] = Kind.BOOLEAN; break;
                default: throw new IllegalArgumentException("unsupported type for " + f.name());
            }
        }
        return k;
    }

    static Schema base(Schema s) {
        if (s.getType() != Schema.Type.UNION) return s;
        for (Schema t : s.getTypes()) {
            if (t.getType() != Schema.Type.NULL) return t;
        }
        throw new IllegalArgumentException("null-only union");
    }

    static RowData toRow(GenericRecord r, Kind[] kinds) {
        GenericRowData row = new GenericRowData(kinds.length);
        for (int i = 0; i < kinds.length; i++) {
            Object v = r.get(i);
            if (v == null) continue;
            switch (kinds[i]) {
                case STRING: row.setField(i, StringData.fromString(v.toString())); break;
                case TIMESTAMP: row.setField(i, TimestampData.fromEpochMillis((Long) v)); break;
                default: row.setField(i, v);
            }
        }
        return row;
    }

    /** Same key columns and types as CustomerProfileAuditJob.ICEBERG_SCHEMA, attributes optional. */
    static org.apache.iceberg.Schema icebergSchema(Schema avro) {
        List<Types.NestedField> fields = new ArrayList<>();
        int id = 1;
        for (Schema.Field f : avro.getFields()) {
            switch (f.name()) {
                case "customer_id": fields.add(Types.NestedField.required(id++, f.name(), Types.StringType.get())); break;
                case "event_timestamp": fields.add(Types.NestedField.required(id++, f.name(), Types.TimestampType.withoutZone())); break;
                case "event_sequence": fields.add(Types.NestedField.required(id++, f.name(), Types.LongType.get())); break;
                default:
                    Type t;
                    switch (base(f.schema()).getType()) {
                        case STRING: t = Types.StringType.get(); break;
                        case LONG: t = Types.LongType.get(); break;
                        case DOUBLE: t = Types.DoubleType.get(); break;
                        case BOOLEAN: t = Types.BooleanType.get(); break;
                        default: throw new IllegalArgumentException(f.name());
                    }
                    fields.add(Types.NestedField.optional(id++, f.name(), t));
            }
        }
        return new org.apache.iceberg.Schema(fields);
    }

    /** Avro datum to RowData inside the Flink job, as the Kafka value deserializer. */
    static final class AvroRowDataDeserializer implements DeserializationSchema<RowData> {
        private final String schemaJson;
        private final RowType rowType;
        private transient GenericDatumReader<GenericRecord> reader;
        private transient BinaryDecoder decoder;
        private transient Kind[] kinds;

        AvroRowDataDeserializer(Schema schema, RowType rowType) {
            this.schemaJson = schema.toString();
            this.rowType = rowType;
        }

        @Override
        public RowData deserialize(byte[] message) throws IOException {
            if (reader == null) {
                Schema s = new Schema.Parser().parse(schemaJson);
                reader = new GenericDatumReader<>(s);
                kinds = kinds(s);
            }
            decoder = DecoderFactory.get().binaryDecoder(message, decoder);
            return toRow(reader.read(null, decoder), kinds);
        }

        @Override
        public boolean isEndOfStream(RowData nextElement) {
            return false;
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            return InternalTypeInfo.of(rowType);
        }
    }

    // ----------------------------------------------------------------- iceberg

    /** A fresh table in a Hadoop catalog: day(event_timestamp) partitions, sorted by customer_id. */
    static TableLoader createTable(String warehouse, String name, org.apache.iceberg.Schema schema) {
        Map<String, String> props = new HashMap<>();
        props.put("warehouse", warehouse);
        CatalogLoader catalogLoader = CatalogLoader.hadoop("hadoop", new org.apache.hadoop.conf.Configuration(), props);
        TableIdentifier id = TableIdentifier.of("audit", name);
        Catalog catalog = catalogLoader.loadCatalog();
        if (!catalog.tableExists(id)) {
            Table t = catalog.createTable(id, schema, PartitionSpec.builderFor(schema).day("event_timestamp").build());
            t.replaceSortOrder().asc("customer_id").commit();
        }
        return TableLoader.fromCatalog(catalogLoader, id);
    }

    static Table load(TableLoader loader) throws IOException {
        loader.open();
        Table t = loader.loadTable();
        t.refresh();
        return t;
    }

    static long totalRecords(Table t) {
        t.refresh();
        Snapshot s = t.currentSnapshot();
        return s == null ? 0 : Long.parseLong(s.summary().getOrDefault("total-records", "0"));
    }

    // ------------------------------------------------------------------- kafka

    static void kafkaMode(Map<String, String> o, Events ev) throws Exception {
        String bootstrap = o.getOrDefault("bootstrap", "localhost:19092");
        String compression = o.getOrDefault("compression", "none");
        int runs = intArg(o, "runs", 5);
        short rf = (short) intArg(o, "replication-factor", 3);
        int partitions = intArg(o, "partitions", 3);
        int parallelism = intArg(o, "parallelism", 3);
        long checkpointMs = longArg(o, "checkpoint-interval-ms", 60_000);
        long minPauseMs = longArg(o, "min-pause-ms", 30_000);
        int restPort = intArg(o, "rest-port", 18081);
        long pollMs = longArg(o, "poll-ms", 100);
        int n = ev.values.size();

        Properties ap = new Properties();
        ap.put("bootstrap.servers", bootstrap);
        try (AdminClient admin = AdminClient.create(ap)) {
            for (int run = 1; run <= runs; run++) {
                String topic = "pipeline-" + o.get("model") + "-" + o.get("label").toLowerCase() + "-" + compression
                    + "-r" + run + "-" + UUID.randomUUID().toString().substring(0, 8);
                createTopic(admin, topic, partitions, rf);
                Produced p = produce(bootstrap, topic, ev, compression);
                long inTopic = endOffsets(admin, topic, partitions);
                if (inTopic != n) throw new IllegalStateException("topic holds " + inTopic + " records, expected " + n);
                LogBytes lb = logBytes(admin, topic, rf);
                Path wh = Path.of(o.get("work"), "kafka-" + UUID.randomUUID());
                FlinkRun f = runFlink(bootstrap, topic, ev, "file://" + wh, parallelism, checkpointMs, minPauseMs,
                    restPort, pollMs);
                deleteTree(wh);
                admin.deleteTopics(Collections.singletonList(topic)).all().get(60, TimeUnit.SECONDS);

                Map<String, Object> row = new LinkedHashMap<>();
                row.put("model", o.get("model"));
                row.put("schema", o.get("label"));
                row.put("compression", compression);
                row.put("run", run);
                row.put("events", n);
                row.put("partitions", partitions);
                row.put("replication_factor", rf);
                row.put("min_insync_replicas", Math.min(2, rf));
                row.put("acks", "all");
                row.put("topic_log_bytes_one_copy", lb.oneCopy);
                row.put("topic_log_bytes_all_replicas", lb.allReplicas);
                row.put("producer_latency_p50_ns", p.p50);
                row.put("producer_latency_p95_ns", p.p95);
                row.put("producer_latency_p99_ns", p.p99);
                row.put("producer_latency_max_ns", p.max);
                row.put("produce_wall_ns", p.wallNs);
                row.put("flink_parallelism", parallelism);
                row.put("flink_checkpoint_interval_ms", checkpointMs);
                row.put("flink_min_pause_ms", minPauseMs);
                row.put("flink_running_ts_ms", f.runningTs);
                row.put("flink_all_records_seen_ts_ms", f.allSeenTs);
                row.put("flink_poll_ms", pollMs);
                row.put("flink_source_records_out", f.sourceRecordsOut);
                row.put("flink_committed_checkpoint_id", f.checkpointId);
                row.put("flink_checkpoint_end_to_end_ms", f.checkpointEndToEndMs);
                row.put("flink_checkpoint_checkpointed_size", f.checkpointedSize);
                row.put("flink_checkpoint_state_size", f.stateSize);
                row.put("flink_checkpoints_completed", f.completedCheckpoints);
                row.put("iceberg_rows", f.icebergRows);
                appendCsv(Path.of(o.get("out")), row);
                System.out.printf("%s %s %s run %d: one copy %d B, p50 %d ns, %d records in %d ms, checkpoint %d took %d ms%n",
                    o.get("model"), o.get("label"), compression, run, lb.oneCopy, p.p50, f.sourceRecordsOut,
                    f.allSeenTs - f.runningTs, f.checkpointId, f.checkpointEndToEndMs);
            }
        }
    }

    static void createTopic(AdminClient admin, String topic, int partitions, short rf) throws Exception {
        NewTopic nt = new NewTopic(topic, partitions, rf)
            .configs(Map.of("min.insync.replicas", String.valueOf(Math.min(2, rf))));
        admin.createTopics(Collections.singletonList(nt)).all().get(60, TimeUnit.SECONDS);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (true) {
            TopicDescription d = describe(admin, topic);
            boolean ready = d != null && d.partitions().size() == partitions;
            if (!ready) {
                if (System.nanoTime() > deadline) throw new IllegalStateException("topic " + topic + " never became ready");
                Thread.sleep(200);
                continue;
            }
            for (TopicPartitionInfo p : d.partitions()) {
                ready &= p.leader() != null && p.isr().size() == rf;
            }
            if (ready) return;
            if (System.nanoTime() > deadline) throw new IllegalStateException("topic " + topic + " never became ready");
            Thread.sleep(200);
        }
    }

    /** The topic's description, or null while brokers do not yet know the new topic. */
    static TopicDescription describe(AdminClient admin, String topic) throws Exception {
        try {
            return admin.describeTopics(Collections.singletonList(topic)).allTopicNames()
                .get(10, TimeUnit.SECONDS).get(topic);
        } catch (java.util.concurrent.ExecutionException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException) return null;
            throw e;
        }
    }

    static final class Produced {
        long p50, p95, p99, max, wallNs;
    }

    /** Sends every event with acks=all and times each send() call to its acknowledgement callback. */
    static Produced produce(String bootstrap, String topic, Events ev, String compression) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        int n = ev.values.size();
        long[] lat = new long[n];
        AtomicReference<Exception> err = new AtomicReference<>();
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props);
        producer.partitionsFor(topic); // metadata fetched before the clock starts
        long start = System.nanoTime();
        for (int i = 0; i < n; i++) {
            final int idx = i;
            final long t0 = System.nanoTime();
            producer.send(new ProducerRecord<>(topic, ev.keys.get(i), ev.values.get(i)), (md, ex) -> {
                lat[idx] = System.nanoTime() - t0;
                if (ex != null) err.compareAndSet(null, ex);
            });
        }
        producer.flush();
        long wall = System.nanoTime() - start;
        producer.close();
        if (err.get() != null) throw err.get();
        long[] sorted = lat.clone();
        Arrays.sort(sorted);
        Produced p = new Produced();
        p.p50 = nearestRank(sorted, 50);
        p.p95 = nearestRank(sorted, 95);
        p.p99 = nearestRank(sorted, 99);
        p.max = sorted[n - 1];
        p.wallNs = wall;
        return p;
    }

    static long nearestRank(long[] sorted, int pct) {
        int k = (int) Math.ceil(pct / 100.0 * sorted.length);
        return sorted[Math.max(1, k) - 1];
    }

    static long endOffsets(AdminClient admin, String topic, int partitions) throws Exception {
        Map<TopicPartition, OffsetSpec> req = new HashMap<>();
        for (int p = 0; p < partitions; p++) req.put(new TopicPartition(topic, p), OffsetSpec.latest());
        long total = 0;
        for (ListOffsetsResult.ListOffsetsResultInfo i : admin.listOffsets(req).all().get(30, TimeUnit.SECONDS).values()) {
            total += i.offset();
        }
        return total;
    }

    static final class LogBytes {
        long oneCopy, allReplicas;
    }

    /** Log bytes of the topic per replica, from the brokers' log directories, once every replica
     *  of every partition reports the same size as its leader. */
    static LogBytes logBytes(AdminClient admin, String topic, short rf) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (true) {
            TopicDescription d = describe(admin, topic);
            if (d == null) {
                Thread.sleep(500);
                continue;
            }
            List<Integer> brokers = admin.describeCluster().nodes().get(10, TimeUnit.SECONDS).stream()
                .map(Node::id).collect(Collectors.toList());
            Map<Integer, Map<String, LogDirDescription>> dirs =
                admin.describeLogDirs(brokers).allDescriptions().get(30, TimeUnit.SECONDS);
            Map<TopicPartition, Map<Integer, Long>> sizes = new HashMap<>();
            for (Map.Entry<Integer, Map<String, LogDirDescription>> b : dirs.entrySet()) {
                for (LogDirDescription ld : b.getValue().values()) {
                    for (Map.Entry<TopicPartition, ReplicaInfo> r : ld.replicaInfos().entrySet()) {
                        if (r.getKey().topic().equals(topic)) {
                            sizes.computeIfAbsent(r.getKey(), k -> new HashMap<>()).put(b.getKey(), r.getValue().size());
                        }
                    }
                }
            }
            LogBytes lb = new LogBytes();
            boolean settled = sizes.size() == d.partitions().size();
            for (TopicPartitionInfo p : d.partitions()) {
                Map<Integer, Long> s = sizes.getOrDefault(new TopicPartition(topic, p.partition()), Map.of());
                Long leader = s.get(p.leader().id());
                settled &= leader != null && s.size() == rf && s.values().stream().allMatch(leader::equals);
                if (leader != null) lb.oneCopy += leader;
                lb.allReplicas += s.values().stream().mapToLong(Long::longValue).sum();
            }
            if (settled) return lb;
            if (System.nanoTime() > deadline) throw new IllegalStateException("replica sizes never settled for " + topic);
            Thread.sleep(500);
        }
    }

    static final class FlinkRun {
        long runningTs, allSeenTs, sourceRecordsOut, checkpointId, checkpointEndToEndMs, checkpointedSize, stateSize,
            completedCheckpoints, icebergRows;
    }

    /** Runs Kafka to Iceberg until Iceberg holds every event, then cancels the job. */
    static FlinkRun runFlink(String bootstrap, String topic, Events ev, String warehouse, int parallelism,
                             long checkpointMs, long minPauseMs, int restPort, long pollMs) throws Exception {
        Map<String, String> conf = new HashMap<>();
        conf.put("rest.bind-address", "localhost");
        conf.put("rest.address", "localhost");
        conf.put("rest.bind-port", String.valueOf(restPort));
        conf.put("rest.port", String.valueOf(restPort));
        conf.put("metrics.fetcher.update-interval", "100");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(Configuration.fromMap(conf));
        env.setParallelism(parallelism);
        env.enableCheckpointing(checkpointMs, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPauseMs);

        KafkaSource<RowData> source = KafkaSource.<RowData>builder()
            .setBootstrapServers(bootstrap)
            .setTopics(topic)
            .setGroupId("pipeline-bench-" + UUID.randomUUID())
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new AvroRowDataDeserializer(ev.avro, ev.rowType))
            .build();
        DataStream<RowData> rows = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka events")
            .uid("kafka-events");
        TableLoader loader = createTable(warehouse, "events", ev.iceberg);
        FlinkSink.forRowData(rows).tableLoader(loader).uidPrefix("iceberg-audit-trail").overwrite(false).append();
        JobClient job = env.executeAsync("pipeline-bench " + topic);
        String base = "http://localhost:" + restPort + "/jobs/" + job.getJobID().toHexString();
        FlinkRun r = new FlinkRun();
        int n = ev.values.size();
        try {
            JsonNode jobInfo = waitFor(() -> {
                JsonNode j = get(base);
                return j != null && "RUNNING".equals(j.path("state").asText())
                    && j.path("timestamps").path("RUNNING").asLong() > 0 ? j : null;
            }, 120_000, pollMs);
            r.runningTs = jobInfo.path("timestamps").path("RUNNING").asLong();
            String sourceVertex = null;
            for (JsonNode v : jobInfo.path("vertices")) {
                if (v.path("name").asText().startsWith("Source: Kafka events")) sourceVertex = v.path("id").asText();
            }
            if (sourceVertex == null) throw new IllegalStateException("no source vertex in " + jobInfo);
            String metrics = base + "/vertices/" + sourceVertex + "/subtasks/metrics";
            String metricId = waitFor(() -> {
                JsonNode list = get(metrics);
                if (list == null) return null;
                for (JsonNode m : list) {
                    String id = m.path("id").asText();
                    if (id.startsWith("Source") && id.endsWith(".numRecordsOut")) return id;
                }
                return null;
            }, 120_000, pollMs);
            String query = metrics + "?get=" + metricId + "&agg=sum";
            JsonNode seen = waitFor(() -> {
                JsonNode a = get(query);
                return a != null && a.size() > 0 && a.get(0).path("sum").asDouble() >= n ? a : null;
            }, 600_000, pollMs);
            r.allSeenTs = System.currentTimeMillis();
            r.sourceRecordsOut = (long) seen.get(0).path("sum").asDouble();

            Table table = load(loader);
            waitFor(() -> totalRecords(table) >= n ? Boolean.TRUE : null, 600_000, 200);
            r.icebergRows = totalRecords(table);
            r.checkpointId = Long.parseLong(table.currentSnapshot().summary().get("flink.max-committed-checkpoint-id"));
            JsonNode details = waitFor(() -> {
                JsonNode c = get(base + "/checkpoints/details/" + r.checkpointId);
                return c != null && "COMPLETED".equals(c.path("status").asText()) ? c : null;
            }, 60_000, pollMs);
            r.checkpointEndToEndMs = details.path("end_to_end_duration").asLong();
            r.checkpointedSize = details.path("checkpointed_size").asLong();
            r.stateSize = details.path("state_size").asLong();
            r.completedCheckpoints = get(base + "/checkpoints").path("counts").path("completed").asLong();
        } finally {
            job.cancel().get(60, TimeUnit.SECONDS);
            try {
                job.getJobExecutionResult().get(60, TimeUnit.SECONDS);
            } catch (Exception expected) {
                // cancelled
            }
        }
        if (r.icebergRows != n) throw new IllegalStateException("Iceberg holds " + r.icebergRows + " rows, expected " + n);
        return r;
    }

    interface Check<T> {
        T get() throws Exception;
    }

    static <T> T waitFor(Check<T> check, long timeoutMs, long pollMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (true) {
            T v = check.get();
            if (v != null) return v;
            if (System.currentTimeMillis() > deadline) throw new IllegalStateException("timed out after " + timeoutMs + " ms");
            Thread.sleep(pollMs);
        }
    }

    static JsonNode get(String url) {
        try {
            HttpResponse<String> resp = HTTP.send(HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(5)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
            return resp.statusCode() == 200 ? JSON.readTree(resp.body()) : null;
        } catch (Exception e) {
            return null;
        }
    }

    // ----------------------------------------------------------------- iceberg

    static void icebergMode(Map<String, String> o, Events ev) throws Exception {
        int runs = intArg(o, "runs", 5);
        int commits = intArg(o, "commits", 20);
        int parallelism = intArg(o, "parallelism", 3);
        long maxSeq = ev.sequences.stream().mapToLong(Long::longValue).max().orElse(0);
        if (maxSeq % commits != 0) throw new IllegalArgumentException("H=" + maxSeq + " is not divisible by " + commits + " commits");
        long perCommit = maxSeq / commits;
        for (int run = 1; run <= runs; run++) {
            Path wh = Path.of(o.get("work"), "iceberg-" + UUID.randomUUID());
            TableLoader loader = createTable("file://" + wh, "events", ev.iceberg);
            for (int c = 0; c < commits; c++) {
                long lo = c * perCommit + 1, hi = (c + 1) * perCommit;
                List<RowData> slice = new ArrayList<>();
                for (int i = 0; i < ev.rows.size(); i++) {
                    long s = ev.sequences.get(i);
                    if (s >= lo && s <= hi) slice.add(ev.rows.get(i));
                }
                rowsForAppend = slice;
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setRuntimeMode(RuntimeExecutionMode.BATCH);
                env.setParallelism(parallelism);
                DataStream<RowData> rows = env.fromSequence(0, slice.size() - 1)
                    .map(new FromHolder()).returns(InternalTypeInfo.of(ev.rowType));
                FlinkSink.forRowData(rows).tableLoader(loader).uidPrefix("iceberg-audit-trail").overwrite(false).append();
                env.execute("append " + (c + 1) + " of " + commits);
            }
            rowsForAppend = Collections.emptyList();
            Table table = load(loader);
            Path tableDir = Path.of(URI.create(table.location()));
            String digest0 = digest(table);

            record(o, run, "after_commits", commits, table, tableDir, digest0, digest0);
            expire(table);
            record(o, run, "after_expiration", commits, table, tableDir, digest(table), digest0);
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(parallelism);
            Actions.forTable(env, table).rewriteDataFiles().execute();
            table.refresh();
            record(o, run, "after_compaction", commits, table, tableDir, digest(table), digest0);
            expire(table);
            record(o, run, "after_compaction_and_expiration", commits, table, tableDir, digest(table), digest0);
            deleteTree(wh);
        }
    }

    /** Reads rowsForAppend[index] in the bounded append job (same JVM as the embedded cluster). */
    static final class FromHolder implements MapFunction<Long, RowData> {
        @Override
        public RowData map(Long i) {
            return rowsForAppend.get(i.intValue());
        }
    }

    static void expire(Table table) {
        table.refresh();
        table.expireSnapshots().expireOlderThan(System.currentTimeMillis() + 1).retainLast(1).commit();
        table.refresh();
    }

    /** Count, distinct (customer_id, event_sequence), and a hash over LAG and previous-known
     *  values of email for every row, computed by DuckDB over the current snapshot's data files. */
    static String digest(Table table) throws Exception {
        table.refresh();
        List<String> files = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask t : tasks) files.add(Path.of(URI.create(t.file().location())).toString());
        }
        String list = files.stream().map(f -> "'" + f + "'").collect(Collectors.joining(", ", "[", "]"));
        String sql = "SELECT count(*), count(DISTINCT (customer_id, event_sequence)), sum(hash(customer_id, event_sequence, prev_email, prev_known_email)) "
            + "FROM (SELECT customer_id, event_sequence, "
            + "LAG(email) OVER (PARTITION BY customer_id ORDER BY event_timestamp, event_sequence) AS prev_email, "
            + "LAST_VALUE(email IGNORE NULLS) OVER (PARTITION BY customer_id ORDER BY event_timestamp, event_sequence "
            + "ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_known_email "
            + "FROM read_parquet(" + list + "))";
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1) + ":" + rs.getLong(2) + ":" + rs.getString(3);
        }
    }

    static void record(Map<String, String> o, int run, String state, int commits, Table table, Path tableDir,
                       String digest, String digestAfterCommits) throws IOException {
        table.refresh();
        int snapshots = 0;
        for (Snapshot ignored : table.snapshots()) snapshots++;
        Snapshot cur = table.currentSnapshot();
        long[] json = sizeOf(tableDir.resolve("metadata"), p -> p.endsWith(".metadata.json"));
        long[] lists = sizeOf(tableDir.resolve("metadata"), p -> p.startsWith("snap-") && p.endsWith(".avro"));
        long[] manifests = sizeOf(tableDir.resolve("metadata"), p -> !p.startsWith("snap-") && p.endsWith(".avro"));
        long[] other = sizeOf(tableDir.resolve("metadata"),
            p -> !p.endsWith(".metadata.json") && !p.endsWith(".avro") && !p.endsWith(".crc"));
        long[] data = sizeOf(tableDir.resolve("data"), p -> p.endsWith(".parquet"));
        String[] parts = digest.split(":");
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("model", o.get("model"));
        row.put("schema", o.get("label"));
        row.put("run", run);
        row.put("state", state);
        row.put("commits", commits);
        row.put("snapshots", snapshots);
        row.put("metadata_json_files", json[0]);
        row.put("metadata_json_bytes", json[1]);
        row.put("manifest_list_files", lists[0]);
        row.put("manifest_list_bytes", lists[1]);
        row.put("manifest_files", manifests[0]);
        row.put("manifest_bytes", manifests[1]);
        row.put("other_metadata_bytes", other[1]);
        row.put("metadata_total_bytes", json[1] + lists[1] + manifests[1] + other[1]);
        row.put("manifests_in_current_snapshot", cur.allManifests(table.io()).size());
        row.put("data_files_in_current_snapshot", Long.parseLong(cur.summary().get("total-data-files")));
        row.put("data_files_on_disk", data[0]);
        row.put("data_bytes_on_disk", data[1]);
        row.put("rows", parts[0]);
        row.put("distinct_customer_sequence", parts[1]);
        row.put("history_digest", parts[2]);
        row.put("history_digest_matches_after_commits", digest.equals(digestAfterCommits) ? "yes" : "no");
        appendCsv(Path.of(o.get("out")), row);
        System.out.printf("%s %s run %d %s: snapshots %d, metadata %d B, manifests %d, data files %d (on disk %d), rows %s, digest %s%n",
            o.get("model"), o.get("label"), run, state, snapshots, row.get("metadata_total_bytes"),
            manifests[0], row.get("data_files_in_current_snapshot"), data[0], parts[0],
            row.get("history_digest_matches_after_commits"));
        if (!digest.equals(digestAfterCommits)) {
            throw new IllegalStateException("history changed after " + state + ": " + digest + " vs " + digestAfterCommits);
        }
    }

    interface NameFilter {
        boolean test(String name);
    }

    /** {count, bytes} of regular files directly or recursively under dir whose name passes the filter. */
    static long[] sizeOf(Path dir, NameFilter f) throws IOException {
        if (!Files.exists(dir)) return new long[]{0, 0};
        long[] r = new long[2];
        try (Stream<Path> s = Files.walk(dir)) {
            for (Path p : (Iterable<Path>) s::iterator) {
                if (Files.isRegularFile(p) && f.test(p.getFileName().toString())) {
                    r[0]++;
                    r[1] += Files.size(p);
                }
            }
        }
        return r;
    }

    // ---------------------------------------------------------------- helpers

    static synchronized void appendCsv(Path out, Map<String, Object> row) throws IOException {
        boolean header = !Files.exists(out) || Files.size(out) == 0;
        if (out.getParent() != null) Files.createDirectories(out.getParent());
        try (PrintWriter w = new PrintWriter(Files.newBufferedWriter(out, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND))) {
            if (header) w.println(String.join(",", row.keySet()));
            w.println(row.values().stream().map(String::valueOf).collect(Collectors.joining(",")));
        }
    }

    static void deleteTree(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        try (Stream<Path> s = Files.walk(dir)) {
            s.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
    }
}
