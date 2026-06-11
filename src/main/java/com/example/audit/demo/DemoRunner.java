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
 * updates, the way the pipeline writes), then runs the three queries in sql/ on an
 * in-process DuckDB. The same SQL runs on Spark, Trino, and Athena over Iceberg, so
 * the output is what those engines return.
 *
 *   sql/lag_history.sql     previous values via LAG
 *   sql/latest_state.sql    current state via ROW_NUMBER
 *   sql/sparse_updates.sql  last known value via LAST_VALUE
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

            banner("Audit-trail demo  (DuckDB stand-in for Spark / Trino / Athena)");
            System.out.println("Seed: customer 98765, append-only change events (sparse updates).");
            System.out.println("Nothing is overwritten; previous values live in earlier rows.\n");
            print(stmt, "SELECT * FROM customer_profile_changes ORDER BY event_sequence");

            section("[1] sql/lag_history.sql  -  previous values via LAG");
            note("Row 3 previous_email = alice@yahoo.com, reconstructed from the log alone.");
            note("LAG(phone) is null on the sparse rows, exactly the sparse-update pitfall.");
            runFile(stmt, "sql/lag_history.sql");

            section("[2] sql/latest_state.sql  -  current state via ROW_NUMBER");
            note("One row per customer: the most recent event. (For per-field latest value");
            note("on sparse data, use the LAST_VALUE form below.)");
            runFile(stmt, "sql/latest_state.sql");

            section("[3] sql/sparse_updates.sql  -  last known value via LAST_VALUE IGNORE NULLS");
            note("phone is carried forward as 555-1001 across the sparse rows, where LAG gave null.");
            runFile(stmt, "sql/sparse_updates.sql");

            orderingInvariant(stmt);

            System.out.println("\nEvery previous value came from an earlier row. No previous-value fields anywhere.\n");
        }
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

        section("[4] The ordering invariant");
        note("A second customer, 24680, changed their email three times. Events 2 and 3");
        note("landed on the same timestamp, 2000 (clock skew, a retry, the same millisecond).");

        System.out.println("\n    Sorted by event_timestamp alone:");
        print(stmt, "SELECT event_timestamp, email, "
                + "LAG(email) OVER (PARTITION BY customer_id ORDER BY event_timestamp) AS previous_email "
                + "FROM customer_profile_changes WHERE customer_id = '24680' ORDER BY event_timestamp");
        note("Two rows share timestamp 2000, so the sort can't order them, and the result");
        note("is not guaranteed: another engine or a different data layout can flip it.");
        note("Here third lands before second, so third's previous is first@x.com. The");
        note("change to second is skipped. Nothing errors; the wrong history just flows");
        note("downstream, which is far harder to catch than an explicit failure.");

        System.out.println("\n    Sorted by event_timestamp, event_sequence:");
        print(stmt, "SELECT event_timestamp, event_sequence, email, "
                + "LAG(email) OVER (PARTITION BY customer_id ORDER BY event_timestamp, event_sequence) AS previous_email "
                + "FROM customer_profile_changes WHERE customer_id = '24680' ORDER BY event_timestamp, event_sequence");
        note("event_sequence breaks the tie (2 before 3). third's previous is now second@x.com.");
    }

    private static void runFile(Statement stmt, String relPath) throws Exception {
        String sql = Files.readString(Path.of(relPath)).trim();
        if (sql.endsWith(";")) sql = sql.substring(0, sql.length() - 1);
        print(stmt, sql);
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
