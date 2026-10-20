package com.example.audit.demo;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Runs the sql/ queries with no infrastructure.
 *
 * Seeds an append-only customer_profile_changes table for customer 98765 (sparse
 * updates, the way the pipeline writes), then runs the queries in sql/ on an
 * in-process DuckDB. The plain LAG, ROW_NUMBER and dedup files run unchanged on Spark,
 * Trino and Athena; the sparse-update files have a standard spelling for those engines
 * and a DuckDB spelling under sql/duckdb/, which is what this demo runs. CI runs every
 * file on DuckDB, Spark and Trino against golden outputs (see ci/).
 *
 *   sql/lag_history.sql                   previous values via LAG
 *   sql/latest_state.sql                  newest row via ROW_NUMBER
 *   sql/duckdb/sparse_current_value.sql   last known value, frame ends at CURRENT ROW
 *   sql/duckdb/sparse_previous_value.sql  previous known value, frame ends at 1 PRECEDING
 *   sql/dedup.sql                         drop repeated (customer_id, event_sequence)
 */
public class DemoRunner {

    // Append-only history for one customer. Row 1 sets everything; rows 2 and 3 are
    // sparse updates that touch only email. Nothing is ever overwritten.
    private static final String SEED = String.join("\n",
        "CREATE TABLE customer_profile_changes (",
        "  customer_id     VARCHAR,",
        "  event_timestamp BIGINT,",
        "  event_sequence  BIGINT,",
        "  email           VARCHAR,",
        "  phone           VARCHAR,",
        "  address         VARCHAR",
        ");",
        "INSERT INTO customer_profile_changes VALUES",
        "  ('98765', 1000, 1, 'alice@gmail.com',   '555-1001', '123 Main St'),",
        "  ('98765', 2000, 2, 'alice@yahoo.com',   NULL,       NULL),",
        "  ('98765', 3000, 3, 'alice@outlook.com', NULL,       NULL);");

    /** The seed DDL+DML, exposed so tests run the committed sql/ files on the same data. */
    public static void seed(Statement stmt) throws Exception {
        for (String s : SEED.split(";")) {
            if (!s.isBlank()) stmt.execute(s);
        }
    }

    public static void main(String[] args) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {

            seed(stmt);

            banner("Audit-trail demo  (DuckDB; synthetic data, three events)");
            System.out.println("Seed: customer 98765, append-only change events (sparse updates).");
            System.out.println("Nothing is overwritten; previous values live in earlier rows.\n");
            print(stmt, "SELECT * FROM customer_profile_changes ORDER BY event_timestamp, event_sequence");

            section("[1] sql/lag_history.sql  -  previous values via LAG");
            note("Row 3 previous_email = alice@yahoo.com, reconstructed from the log alone.");
            note("LAG(phone) is null on the sparse rows: the preceding row did not touch phone.");
            runFile(stmt, "sql/lag_history.sql");

            section("[2] sql/latest_state.sql  -  newest row via ROW_NUMBER");
            note("One row per customer: the most recent event. On sparse data its untouched");
            note("fields are null; the per-field last known value is query [3].");
            runFile(stmt, "sql/latest_state.sql");

            section("[3] sql/duckdb/sparse_current_value.sql  -  last known value, frame ends at CURRENT ROW");
            note("phone is carried forward as 555-1001 across the sparse rows, where LAG gave null.");
            runFile(stmt, "sql/duckdb/sparse_current_value.sql");

            section("[4] sql/duckdb/sparse_previous_value.sql  -  previous known value, frame ends at 1 PRECEDING");
            note("Row 3 previous_email = alice@yahoo.com, the value it replaced. Row 3 previous_phone");
            note("= 555-1001: phone did not change on row 3, so the previous known value is the");
            note("current one.");
            runFile(stmt, "sql/duckdb/sparse_previous_value.sql");

            duplicates(stmt);

            orderingInvariant(stmt);

            System.out.println("\nEvery previous value came from an earlier row. No previous-value fields anywhere.\n");
        }
    }

    /** A retry resends event 2. The topic now holds two Kafka records with the same
     *  (customer_id, event_sequence), and each lands as its own Iceberg row. Exactly-once
     *  in the Flink job does not remove it: that guarantee covers Flink's own recovery
     *  path, not records the producer wrote twice. sql/dedup.sql drops it. */
    private static void duplicates(Statement stmt) throws Exception {
        stmt.execute("INSERT INTO customer_profile_changes VALUES "
                + "('98765', 2000, 2, 'alice@yahoo.com', NULL, NULL)");

        section("[5] Duplicates: a retried event appears as a repeated (customer_id, event_sequence)");
        note("Event 2 was sent twice. Without dedup, LAG sees four rows and the duplicate");
        note("reports alice@yahoo.com as its own previous email.");
        runFile(stmt, "sql/lag_history.sql");

        section("    sql/dedup.sql  -  one row per (customer_id, event_sequence)");
        note("Three rows come back. Run this first, as a view or CTE, then the window queries.");
        runFile(stmt, "sql/dedup.sql");
    }

    /** Why event_sequence exists. Two events share a timestamp; sorting by timestamp
     *  alone can put them in the wrong order, and "previous" comes out wrong. The tied
     *  rows are inserted in the wrong physical order on purpose, so the no-tiebreaker
     *  sort surfaces the bug instead of hiding it. */
    private static void orderingInvariant(Statement stmt) throws Exception {
        stmt.execute("INSERT INTO customer_profile_changes VALUES "
                + "('24680', 1000, 1, 'first@x.com',  NULL, NULL),"
                + "('24680', 2000, 3, 'third@x.com',  NULL, NULL),"   // events 2 and 3 share
                + "('24680', 2000, 2, 'second@x.com', NULL, NULL)");  // timestamp 2000

        section("[6] The ordering invariant");
        note("A second customer, 24680, changed their email three times. Events 2 and 3");
        note("landed on the same timestamp, 2000 (clock skew, a retry, the same millisecond).");

        System.out.println("\n    Sorted by event_timestamp alone (the counterexample):");
        print(stmt, "SELECT event_timestamp, email, "
                + "LAG(email) OVER (PARTITION BY customer_id ORDER BY event_timestamp) AS previous_email "
                + "FROM customer_profile_changes WHERE customer_id = '24680' ORDER BY event_timestamp");
        note("Two rows share timestamp 2000, so the sort cannot order them, and the result");
        note("is not guaranteed: another engine or a different data layout can flip it.");
        note("Here third lands before second, so third's previous is first@x.com. The");
        note("change to second is skipped. Nothing errors; the wrong history flows downstream.");

        System.out.println("\n    Sorted by event_timestamp, event_sequence:");
        print(stmt, "SELECT event_timestamp, event_sequence, email, "
                + "LAG(email) OVER (PARTITION BY customer_id ORDER BY event_timestamp, event_sequence) AS previous_email "
                + "FROM customer_profile_changes WHERE customer_id = '24680' ORDER BY event_timestamp, event_sequence");
        note("event_sequence breaks the tie (2 before 3). third's previous is now second@x.com.");
    }

    private static void runFile(Statement stmt, String relPath) throws Exception {
        print(stmt, readQuery(relPath));
    }

    /** The query text of a sql/ file with the trailing semicolon removed, so JDBC runs it. */
    public static String readQuery(String relPath) throws Exception {
        String sql = Files.readString(Path.of(relPath)).trim();
        if (sql.endsWith(";")) sql = sql.substring(0, sql.length() - 1);
        return sql;
    }

    /** Execute a query and print the result set as an aligned table. */
    static void print(Statement stmt, String sql) throws Exception {
        try (ResultSet rs = stmt.executeQuery(sql)) {
            ResultSetMetaData md = rs.getMetaData();
            int cols = md.getColumnCount();
            List<String[]> rows = new ArrayList<>();
            String[] header = new String[cols];
            for (int c = 1; c <= cols; c++) header[c - 1] = md.getColumnLabel(c);
            rows.add(header);
            while (rs.next()) {
                String[] r = new String[cols];
                for (int c = 1; c <= cols; c++) {
                    Object v = rs.getObject(c);
                    r[c - 1] = v == null ? "NULL" : v.toString();
                }
                rows.add(r);
            }
            int[] w = new int[cols];
            for (String[] r : rows)
                for (int c = 0; c < cols; c++) w[c] = Math.max(w[c], r[c].length());
            for (int i = 0; i < rows.size(); i++) {
                StringBuilder sb = new StringBuilder("  ");
                for (int c = 0; c < cols; c++) sb.append(pad(rows.get(i)[c], w[c])).append("  ");
                System.out.println(sb.toString().stripTrailing());
                if (i == 0) {
                    StringBuilder rule = new StringBuilder("  ");
                    for (int c = 0; c < cols; c++) rule.append("-".repeat(w[c])).append("  ");
                    System.out.println(rule.toString().stripTrailing());
                }
            }
        }
    }

    private static String pad(String s, int w) {
        return s + " ".repeat(Math.max(0, w - s.length()));
    }

    private static void banner(String s) {
        System.out.println("\n=== " + s + " ===\n");
    }

    private static void section(String s) {
        System.out.println("\n" + s);
    }

    private static void note(String s) {
        System.out.println("    note: " + s);
    }
}
