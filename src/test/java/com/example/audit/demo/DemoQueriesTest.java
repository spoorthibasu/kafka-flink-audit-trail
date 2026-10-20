package com.example.audit.demo;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Runs the committed sql/ files on the demo seed in DuckDB and asserts the exact query
 * results. The cross-engine golden tests live in ci/ and run in GitHub Actions.
 */
class DemoQueriesTest {

    @Test
    void lagHistory_reconstructsPreviousEmail_andShowsSparseNull() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {
            DemoRunner.seed(stmt);
            try (ResultSet rs = stmt.executeQuery(DemoRunner.readQuery("sql/lag_history.sql"))) {
                String prevEmailAtSeq3 = null, prevPhoneAtSeq3 = "sentinel";
                while (rs.next()) {
                    if (rs.getLong("event_sequence") == 3L) {
                        prevEmailAtSeq3 = rs.getString("previous_email");
                        prevPhoneAtSeq3 = rs.getString("previous_phone");
                    }
                }
                assertEquals("alice@yahoo.com", prevEmailAtSeq3,
                    "LAG must reconstruct the previous email from the log");
                assertNull(prevPhoneAtSeq3,
                    "LAG(phone) is null on a sparse row whose predecessor did not touch phone");
            }
        }
    }

    @Test
    void sparseCurrentValue_carriesLastKnownPhoneForward() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {
            DemoRunner.seed(stmt);
            try (ResultSet rs = stmt.executeQuery(
                    DemoRunner.readQuery("sql/duckdb/sparse_current_value.sql"))) {
                String currentPhoneAtSeq3 = null;
                while (rs.next()) {
                    if (rs.getLong("event_sequence") == 3L) {
                        currentPhoneAtSeq3 = rs.getString("current_phone");
                    }
                }
                assertEquals("555-1001", currentPhoneAtSeq3,
                    "LAST_VALUE IGNORE NULLS with the frame at CURRENT ROW carries the last known phone forward");
            }
        }
    }

    @Test
    void sparsePreviousValue_reportsTheValueReplaced_orTheCurrentOneWhenUntouched() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {
            DemoRunner.seed(stmt);
            try (ResultSet rs = stmt.executeQuery(
                    DemoRunner.readQuery("sql/duckdb/sparse_previous_value.sql"))) {
                String prevEmailAtSeq1 = "sentinel", prevEmailAtSeq3 = null, prevPhoneAtSeq3 = null;
                while (rs.next()) {
                    long seq = rs.getLong("event_sequence");
                    if (seq == 1L) prevEmailAtSeq1 = rs.getString("previous_email");
                    if (seq == 3L) {
                        prevEmailAtSeq3 = rs.getString("previous_email");
                        prevPhoneAtSeq3 = rs.getString("previous_phone");
                    }
                }
                assertNull(prevEmailAtSeq1, "nothing precedes the first event");
                assertEquals("alice@yahoo.com", prevEmailAtSeq3,
                    "the frame ending at 1 PRECEDING returns the email that row 3 replaced");
                assertEquals("555-1001", prevPhoneAtSeq3,
                    "phone was untouched on row 3, so its previous known value is the current one");
            }
        }
    }

    @Test
    void dedup_returnsARepeatedEventOnce() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {
            DemoRunner.seed(stmt);
            stmt.execute("INSERT INTO customer_profile_changes VALUES "
                + "('98765', 2000, 2, 'alice@yahoo.com', NULL, NULL)");
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT COUNT(*) FROM customer_profile_changes")) {
                rs.next();
                assertEquals(4L, rs.getLong(1), "the duplicate is a fourth row in the raw table");
            }
            int rows = 0, seq2 = 0;
            try (ResultSet rs = stmt.executeQuery(DemoRunner.readQuery("sql/dedup.sql"))) {
                while (rs.next()) {
                    rows++;
                    if (rs.getLong("event_sequence") == 2L) seq2++;
                }
            }
            assertEquals(3, rows, "dedup keeps one row per (customer_id, event_sequence)");
            assertEquals(1, seq2, "the repeated event_sequence comes back once");
        }
    }
}
