package com.example.audit;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The full loop, end to end: a Flink job writes three events into a real Iceberg
 * table, the test reads them back out through Iceberg's own reader, and a LAG query
 * rebuilds the previous email from those rows.
 *
 * The job runs in batch mode, so the Iceberg commit happens when the input ends.
 * That keeps the test deterministic (no waiting on a streaming checkpoint).
 */
class IcebergRoundTripTest {

    @Test
    void flinkWritesIceberg_thenLagRebuildsPreviousEmail(@TempDir Path tmp) throws Exception {
        String warehouse = "file://" + tmp.resolve("warehouse");

        // 1. Flink writes three events into Iceberg (same toRowData and table as the job).
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);

        DataStream<RowData> rows = env.fromElements(
                new CustomerProfileChangeEvent("98765", 1000L, 1L, "alice@gmail.com", "555-1001", "123 Main St"),
                new CustomerProfileChangeEvent("98765", 2000L, 2L, "alice@yahoo.com", null, null),
                new CustomerProfileChangeEvent("98765", 3000L, 3L, "alice@outlook.com", null, null)
        ).map(CustomerProfileAuditJob::toRowData);

        FlinkSink.forRowData(rows)
                .tableLoader(CustomerProfileAuditJob.buildTableLoader(warehouse, ""))
                .overwrite(false)
                .append();
        env.execute("write-iceberg-test");

        // 2. Read the committed rows back out of the Iceberg table.
        List<Object[]> readBack = new ArrayList<>();
        TableLoader loader = CustomerProfileAuditJob.buildTableLoader(warehouse, "");
        loader.open();
        Table table = loader.loadTable();
        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record r : records) {
                LocalDateTime ts = (LocalDateTime) r.getField("event_timestamp");
                readBack.add(new Object[]{
                        r.getField("customer_id"),
                        ts.toInstant(ZoneOffset.UTC).toEpochMilli(),
                        r.getField("event_sequence"),
                        r.getField("email"),
                        r.getField("phone"),
                        r.getField("address")
                });
            }
        }
        assertEquals(3, readBack.size(), "Flink should have written three rows into Iceberg");

        // 3. Run the committed LAG query on those rows and check the reconstruction.
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE customer_profile_changes ("
                    + "customer_id VARCHAR, event_timestamp BIGINT, event_sequence BIGINT,"
                    + "email VARCHAR, phone VARCHAR, address VARCHAR)");
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO customer_profile_changes VALUES (?,?,?,?,?,?)")) {
                for (Object[] row : readBack) {
                    for (int i = 0; i < 6; i++) ps.setObject(i + 1, row[i]);
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            String sql = Files.readString(Path.of("sql/lag_history.sql")).trim();
            if (sql.endsWith(";")) sql = sql.substring(0, sql.length() - 1);
            String prevEmailAtSeq3 = null;
            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    if (rs.getLong("event_sequence") == 3L) {
                        prevEmailAtSeq3 = rs.getString("previous_email");
                    }
                }
            }
            assertEquals("alice@yahoo.com", prevEmailAtSeq3,
                    "LAG over the Iceberg table the job wrote must rebuild the previous email");
        }
    }
}
