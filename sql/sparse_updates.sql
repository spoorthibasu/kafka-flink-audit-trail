-- Use when events are sparse: not every field changes on every event.
-- LAG returns null if the preceding row didn't touch that field.
-- LAST_VALUE IGNORE NULLS returns the last known non-null value instead.
-- The explicit ROWS frame stops engines from looking past the current row.
--
-- Engines disagree on where IGNORE NULLS goes, and neither spelling parses on the
-- other engine:
--   DuckDB                 LAST_VALUE(email IGNORE NULLS) OVER (...)  <- this file
--   Trino, Athena, Spark   LAST_VALUE(email) IGNORE NULLS OVER (...)  <- sparse_updates_portable.sql
-- The demo runs on DuckDB, so this file uses the DuckDB spelling. Use the portable
-- file against Trino, Athena, or Spark. (Spark also accepts LAST(email, true).)

SELECT
  customer_id,
  event_timestamp,
  event_sequence,
  LAST_VALUE(email IGNORE NULLS) OVER (
    PARTITION BY customer_id
    ORDER BY event_timestamp, event_sequence
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS current_email,
  LAST_VALUE(phone IGNORE NULLS) OVER (
    PARTITION BY customer_id
    ORDER BY event_timestamp, event_sequence
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS current_phone,
  LAST_VALUE(address IGNORE NULLS) OVER (
    PARTITION BY customer_id
    ORDER BY event_timestamp, event_sequence
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS current_address
FROM customer_profile_changes
WHERE customer_id = '98765'
ORDER BY event_timestamp, event_sequence;
