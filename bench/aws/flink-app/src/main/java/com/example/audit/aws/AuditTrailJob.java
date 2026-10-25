package com.example.audit.aws;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.avro.Schema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka to Iceberg for the AWS scale run, on Amazon Managed Service for Apache Flink.
 *
 * The topology is the one the local pipeline benchmark measures (PipelineBenchmark.runFlink):
 * KafkaSource from the earliest offset, no watermarks, the Avro value deserializer for the
 * design's schema, and the Iceberg FlinkSink appending to a table partitioned by
 * day(event_timestamp) with sort order customer_id, event_timestamp, event_sequence,
 * operator uids "kafka-events" and "iceberg-audit-trail". On Managed Flink, checkpointing
 * and parallelism come from the application configuration; locally they come from the
 * properties file.
 *
 * Runtime properties, group "job": bootstrap, topic, group.id, design (A or B), catalog
 * (glue or hadoop), warehouse, database, table. Local runs only: checkpoint.interval.ms,
 * min.pause.ms, parallelism. Local run: pass the path of a properties file in the Managed
 * Flink JSON format as the first argument.
 */
public class AuditTrailJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        boolean local = env instanceof LocalStreamEnvironment;
        Properties p = properties(local, args);

        if (local) {
            env.setParallelism(Integer.parseInt(p.getProperty("parallelism", "3")));
            env.enableCheckpointing(Long.parseLong(p.getProperty("checkpoint.interval.ms", "60000")),
                CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(Long.parseLong(p.getProperty("min.pause.ms", "30000")));
        }

        String design = required(p, "design");
        Schema avro = AvroRows.designSchema(design);
        KafkaSource<RowData> source = KafkaSource.<RowData>builder()
            .setBootstrapServers(required(p, "bootstrap"))
            .setTopics(required(p, "topic"))
            .setGroupId(required(p, "group.id"))
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new AvroRows.RowDataDeserializer(avro, AvroRows.rowType(avro)))
            .build();
        DataStream<RowData> rows = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka events")
            .uid("kafka-events");

        CatalogLoader catalog = Catalogs.loader(required(p, "catalog"), required(p, "warehouse"));
        String database = required(p, "database");
        String table = required(p, "table");
        // The runner creates the tables before the job starts; this covers a local run.
        Catalogs.createTable(catalog, database, table, AvroRows.icebergSchema(avro));
        FlinkSink.forRowData(rows)
            .tableLoader(Catalogs.tableLoader(catalog, database, table))
            .uidPrefix("iceberg-audit-trail")
            .overwrite(false)
            .append();
        env.execute("audit-trail-aws " + design + " " + p.getProperty("topic"));
    }

    static Properties properties(boolean local, String[] args) throws IOException {
        Map<String, Properties> groups = local && args.length > 0
            ? KinesisAnalyticsRuntime.getApplicationProperties(args[0])
            : KinesisAnalyticsRuntime.getApplicationProperties();
        Properties p = groups.get("job");
        if (p == null) throw new IllegalArgumentException("no property group \"job\"");
        return p;
    }

    static String required(Properties p, String key) {
        String v = p.getProperty(key);
        if (v == null || v.isEmpty()) throw new IllegalArgumentException("missing job property " + key);
        return v;
    }
}
