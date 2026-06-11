package com.example.audit.demo;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * When two events for one customer share a timestamp, the tiebreaker
 * decides what "previous" means. With event_sequence ascending (the producer's
 * order) the answer is correct; break the tie the other way and the reconstruction
 * is silently wrong. This test pins both, so the invariant can't quietly regress.
 */
class OrderingInvariantTest {

    private static final String SEED =
        "CREATE TABLE t (customer_id VARCHAR, event_timestamp BIGINT, event_sequence BIGINT, email VARCHAR);"
        + "INSERT INTO t VALUES "
        + "('24680', 1000, 1, 'first@x.com'),"
        + "('24680', 2000, 2, 'second@x.com'),"   // shares timestamp 2000
        + "('24680', 2000, 3, 'third@x.com')";    // with this one

    private String previousEmailAtSeq3(Statement stmt, String tiebreak) throws Exception {
        String sql = "SELECT event_sequence, LAG(email) OVER ("
                + "PARTITION BY customer_id ORDER BY event_timestamp, event_sequence " + tiebreak
                + ") AS prev FROM t WHERE customer_id = '24680' ORDER BY event_sequence";
        try (ResultSet rs = stmt.executeQuery(sql)) {
            String prev = null;
            while (rs.next()) {
                if (rs.getLong("event_sequence") == 3L) prev = rs.getString("prev");
            }
            return prev;
        }
    }

    @Test
    void eventSequenceDecidesThePreviousValueForTiedTimestamps() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {
            for (String s : SEED.split(";")) stmt.execute(s);

            String correct = previousEmailAtSeq3(stmt, "ASC");
            String tieBrokenTheOtherWay = previousEmailAtSeq3(stmt, "DESC");

            assertEquals("second@x.com", correct,
                "with event_sequence ascending, the previous value is the genuinely earlier one");
            assertEquals("first@x.com", tieBrokenTheOtherWay,
                "break the tie the other way and the change to second is skipped");
            assertNotEquals(correct, tieBrokenTheOtherWay,
                "the tiebreaker, not the timestamp, decides the answer, so it must be reliable");
        }
    }
}
