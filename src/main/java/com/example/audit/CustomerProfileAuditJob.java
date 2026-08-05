package com.example.audit;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;

import java.util.HashMap;
import java.util.Map;

/**
 * Appends customer profile change events from Kafka into an Iceberg audit trail.
 * Stateless: each event becomes one new row, nothing is updated. Previous values
 * are reconstructed at query time with the window-function SQL in sql/.
 *
 * Args (all optional):
 *   --kafka-bootstrap-servers  default localhost:9092
 *   --topic                    default customer-profile-changes
 *   --warehouse                default file:///tmp/audit-warehouse  (local FS, runnable)
 *                              use s3://bucket/warehouse in production
 *   --io-impl                  e.g. org.apache.iceberg.aws.s3.S3FileIO  (only for s3://)
 *   --checkpoint-interval-ms   default 60000
 */
public class CustomerProfileAuditJob {

    // snake_case columns match the Avro schema and sql/ queries.
    static final Schema ICEBERG_SCHEMA = new Schema(
        Types.NestedField.required(1, "customer_id",     Types.StringType.get()),
        Types.NestedField.required(2, "event_timestamp", Types.TimestampType.withoutZone()),
        Types.NestedField.required(3, "event_sequence",  Types.LongType.get()),
        Types.NestedField.optional(4, "email",           Types.StringType.get()),
        Types.NestedField.optional(5, "phone",           Types.StringType.get()),
        Types.NestedField.optional(6, "address",         Types.StringType.get())
    );

    public static void main(String[] args) throws Exception {
        String bootstrap   = arg(args, "--kafka-bootstrap-servers", "localhost:9092");
        String topic       = arg(args, "--topic", "customer-profile-changes");
        String sink        = arg(args, "--sink", "print"); // print (runnable demo) | iceberg
        String warehouse   = arg(args, "--warehouse", "file:///tmp/audit-warehouse");
        String ioImpl      = arg(args, "--io-impl", "");
        long checkpointMs  = Long.parseLong(arg(args, "--checkpoint-interval-ms", "60000"));

        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
        // Current form on Flink 1.20+: two-arg enableCheckpointing taking
        // org.apache.flink.core.execution.CheckpointingMode. The same call taking the older
        // org.apache.flink.streaming.api.CheckpointingMode is deprecated, as is
        // CheckpointConfig.setCheckpointingMode (replaced by setCheckpointingConsistencyMode).
        // All spellings produce identical behavior; this one compiles warning-free.
        env.enableCheckpointing(checkpointMs, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000);

        KafkaSource<CustomerProfileChangeEvent> source =
            KafkaSource.<CustomerProfileChangeEvent>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId("profile-audit-consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new CustomerProfileChangeDeserializer())
                .build();

        DataStream<CustomerProfileChangeEvent> eventStream =
            env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(), // no event-time windowing, no operator state
                "Kafka customer profile events");

        if (sink.equals("iceberg")) {
            // Production / cluster path: append each event as a new Iceberg row.
            DataStream<RowData> rowDataStream = eventStream
                .map(CustomerProfileAuditJob::toRowData)
                .name("convert-to-row-data");
            System.out.println("Appending '" + topic + "' -> " + warehouse
                + " (audit.customer_profile_changes)");
            FlinkSink.forRowData(rowDataStream)
                .tableLoader(buildTableLoader(warehouse, ioImpl))
                .overwrite(false)
                .append();
        } else {
            // Default demo path: print each appended row, so the Kafka -> Flink round
            // trip runs anywhere with no Iceberg/Hadoop/S3 setup. The query-time
            // reconstruction is then shown by `make demo` over the same shape of data.
            System.out.println("Appending '" + topic + "' -> stdout (append-only audit rows)");
            eventStream
                .map(CustomerProfileAuditJob::describe)
                .name("describe-append")
                .print();
        }

        env.execute("Customer Profile Audit Trail");
    }

    static String describe(CustomerProfileChangeEvent e) {
        return "APPEND  customer_id=" + e.getCustomerId()
            + " seq=" + e.getEventSequence()
            + " ts=" + e.getEventTimestamp()
            + " email=" + e.getEmail()
            + " phone=" + e.getPhone()
            + " address=" + e.getAddress();
    }

    public static RowData toRowData(CustomerProfileChangeEvent event) {
        GenericRowData row = new GenericRowData(6);
        row.setField(0, StringData.fromString(event.getCustomerId()));
        // Iceberg's timestamp column expects TimestampData, not a raw long.
        row.setField(1, TimestampData.fromEpochMillis(event.getEventTimestamp()));
        row.setField(2, event.getEventSequence());
        row.setField(3, event.getEmail()   != null ? StringData.fromString(event.getEmail())   : null);
        row.setField(4, event.getPhone()   != null ? StringData.fromString(event.getPhone())   : null);
        row.setField(5, event.getAddress() != null ? StringData.fromString(event.getAddress()) : null);
        return row;
    }

    public static TableLoader buildTableLoader(String warehouse, String ioImpl) {
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("warehouse", warehouse);
        // S3FileIO etc. only when pointing at object storage; local file:// needs none.
        if (!ioImpl.isEmpty()) {
            catalogProperties.put("io-impl", ioImpl);
        }

        CatalogLoader catalogLoader = CatalogLoader.hadoop(
            "hadoop", new Configuration(), catalogProperties);

        TableIdentifier tableId =
            TableIdentifier.of("audit", "customer_profile_changes");

        Catalog catalog = catalogLoader.loadCatalog();
        if (!catalog.tableExists(tableId)) {
            PartitionSpec spec = PartitionSpec.builderFor(ICEBERG_SCHEMA)
                .day("event_timestamp")
                .build();
            Table table = catalog.createTable(tableId, ICEBERG_SCHEMA, spec);
            // Sort by customer_id so LAG queries scan fewer files per customer.
            table.replaceSortOrder().asc("customer_id").commit();
        }

        return TableLoader.fromCatalog(catalogLoader, tableId);
    }

    private static String arg(String[] args, String key, String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(key)) return args[i + 1];
        }
        return defaultValue;
    }
}
