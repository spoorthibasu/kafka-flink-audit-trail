package com.example.audit.demo;

import com.example.audit.CustomerProfileAuditJob;
import com.example.audit.CustomerProfileChangeEvent;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Comparator;

/**
 * The whole loop, out loud. A Flink job writes three events into a real Iceberg
 * table on disk; the demo then reads the Parquet files it wrote and runs the LAG query
 * over them, printing what comes back.
 *
 * Unlike DemoRunner (which seeds a table directly), every row here went through the
 * pipeline: event -> Flink -> Iceberg -> query. The job runs in batch mode, so the
 * commit lands when the input ends, which keeps this quick and free of Kafka.
 */
public class IcebergLoopRunner {

    private static final String WAREHOUSE_DIR = "/tmp/audit-loop-warehouse";

    public static void main(String[] args) throws Exception {
        Path warehouse = Path.of(WAREHOUSE_DIR);
        deleteIfExists(warehouse);

        System.out.println("\n=== Iceberg loop  (Flink writes, then the demo queries what it wrote) ===\n");
        System.out.println("1. Flink appends three events into an Iceberg table at " + warehouse);

        writeWithFlink("file://" + warehouse);

        String dataGlob = warehouse + "/audit/customer_profile_changes/data/**/*.parquet";
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE VIEW customer_profile_changes AS "
                    + "SELECT * FROM read_parquet('" + dataGlob + "')");

            System.out.println("\n2. The rows now sitting in the Iceberg table (read back from Parquet):\n");
            DemoRunner.print(stmt, "SELECT customer_id, event_timestamp, event_sequence, "
                    + "email, phone, address FROM customer_profile_changes ORDER BY event_sequence");

            System.out.println("\n3. sql/lag_history.sql over that table rebuilds each previous value:\n");
            String lag = Files.readString(Path.of("sql/lag_history.sql")).trim();
            if (lag.endsWith(";")) lag = lag.substring(0, lag.length() - 1);
            DemoRunner.print(stmt, lag);
        }

        System.out.println("\nEvery row above went event -> Flink -> Iceberg -> query. "
                + "No previous-value fields anywhere.\n");
    }

    private static void writeWithFlink(String warehouseUri) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);

        DataStream<RowData> rows = env.fromElements(
                new CustomerProfileChangeEvent("98765", 1700000001000L, 1L, "alice@gmail.com", "555-1001", "123 Main St"),
                new CustomerProfileChangeEvent("98765", 1700000002000L, 2L, "alice@yahoo.com", null, null),
                new CustomerProfileChangeEvent("98765", 1700000003000L, 3L, "alice@outlook.com", null, null)
        ).map(CustomerProfileAuditJob::toRowData);

        FlinkSink.forRowData(rows)
                .tableLoader(CustomerProfileAuditJob.buildTableLoader(warehouseUri, ""))
                .overwrite(false)
                .append();
        env.execute("iceberg-loop");
    }

    private static void deleteIfExists(Path dir) throws Exception {
        if (Files.exists(dir)) {
            Files.walk(dir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
    }
}
