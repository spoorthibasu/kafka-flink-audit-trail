package com.example.audit.demo;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Runs the committed sql/ files on the seed and asserts the exact query results.
 * If a query stops producing them, CI fails.
 */
class DemoQueriesTest {

    private String runFile(String relPath) throws Exception {
        String sql = Files.readString(Path.of(relPath)).trim();
        return sql.endsWith(";") ? sql.substring(0, sql.length() - 1) : sql;
    }

    @Test
    void lagHistory_reconstructsPreviousEmail_andShowsSparseNull() throws Exception {
        // Row three shows alice@yahoo.com as previous_email.
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {
            DemoRunner.seed(stmt);
            try (ResultSet rs = stmt.executeQuery(runFile("sql/lag_history.sql"))) {
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
                    "LAG(phone) is null on a sparse row, the sparse-update pitfall");
            }
        }
    }

    @Test
    void sparseUpdates_carriesLastKnownPhoneForward() throws Exception {
        // LAST_VALUE IGNORE NULLS returns the last known non-null value.
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {
            DemoRunner.seed(stmt);
            try (ResultSet rs = stmt.executeQuery(runFile("sql/sparse_updates.sql"))) {
                String currentPhoneAtSeq3 = null;
                while (rs.next()) {
                    if (rs.getLong("event_sequence") == 3L) {
                        currentPhoneAtSeq3 = rs.getString("current_phone");
                    }
                }
                assertEquals("555-1001", currentPhoneAtSeq3,
                    "LAST_VALUE IGNORE NULLS must carry the last known phone forward");
            }
        }
    }
}
