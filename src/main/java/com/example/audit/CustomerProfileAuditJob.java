package com.example.audit;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
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

public class CustomerProfileAuditJob {

    // Iceberg stores timestamps as microseconds; the POJO uses milliseconds.
    // toRowData() handles the conversion.
    static final Schema ICEBERG_SCHEMA = new Schema(
        Types.NestedField.required(1, "customerId",     Types.StringType.get()),
        Types.NestedField.required(2, "eventTimestamp", Types.TimestampType.withoutZone()),
        Types.NestedField.required(3, "eventSequence",  Types.LongType.get()),
        Types.NestedField.optional(4, "email",          Types.StringType.get()),
        Types.NestedField.optional(5, "phone",          Types.StringType.get()),
        Types.NestedField.optional(6, "address",        Types.StringType.get())
    );

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000);

        KafkaSource<CustomerProfileChangeEvent> source =
            KafkaSource.<CustomerProfileChangeEvent>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("customer-profile-changes")
                .setGroupId("profile-audit-consumer")
                .setValueOnlyDeserializer(new CustomerProfileChangeDeserializer())
                .build();

        DataStream<CustomerProfileChangeEvent> eventStream =
            env.fromSource(
                source,
                // No watermarks: stateless append, no time-based windowing.
                // Checkpoints tie Kafka offset commits to Iceberg snapshot commits
                // so a restart replays cleanly without duplicates.
                WatermarkStrategy.noWatermarks(),
                "Kafka customer profile events");

        DataStream<RowData> rowDataStream = eventStream
            .map(CustomerProfileAuditJob::toRowData)
            .name("convert-to-row-data");

        FlinkSink.forRowData(rowDataStream)
            .tableLoader(buildTableLoader())
            .overwrite(false)
            .append();

        env.execute("Customer Profile Audit Trail");
    }

    static RowData toRowData(CustomerProfileChangeEvent event) {
        GenericRowData row = new GenericRowData(6);
        row.setField(0, StringData.fromString(event.getCustomerId()));
        row.setField(1, event.getEventTimestamp() * 1_000L); // ms -> us
        row.setField(2, event.getEventSequence());
        row.setField(3, event.getEmail()   != null ? StringData.fromString(event.getEmail())   : null);
        row.setField(4, event.getPhone()   != null ? StringData.fromString(event.getPhone())   : null);
        row.setField(5, event.getAddress() != null ? StringData.fromString(event.getAddress()) : null);
        return row;
    }

    private static TableLoader buildTableLoader() {
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("warehouse", "s3://your-bucket/warehouse");
        catalogProperties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");

        CatalogLoader catalogLoader = CatalogLoader.hadoop(
            "hadoop",
            new Configuration(),
            catalogProperties
        );

        TableIdentifier tableId =
            TableIdentifier.of("audit", "customer_profile_changes");

        Catalog catalog = catalogLoader.loadCatalog();
        if (!catalog.tableExists(tableId)) {
            PartitionSpec spec = PartitionSpec.builderFor(ICEBERG_SCHEMA)
                .day("eventTimestamp")
                .build();
            // Sort by customerId within each day partition so LAG window
            // functions only scan the rows they need.
            Table table = catalog.createTable(tableId, ICEBERG_SCHEMA, spec);
            table.replaceSortOrder()
                .asc("customerId")
                .commit();
        }

        return TableLoader.fromCatalog(catalogLoader, tableId);
    }
}
