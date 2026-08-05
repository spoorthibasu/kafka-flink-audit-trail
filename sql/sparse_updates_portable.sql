-- Same query as sparse_updates.sql, in the SQL-standard spelling: IGNORE NULLS goes
-- after the closing paren, before OVER. This is the form Trino, Athena, and Spark
-- accept. DuckDB rejects it, so use sparse_updates.sql there, which is why the demo
-- runs the other file. Spark also accepts LAST(email, true) OVER (...) as an
-- equivalent alternative.
--
-- Use when events are sparse: not every field changes on every event.
-- LAG returns null if the preceding row didn't touch that field.
-- LAST_VALUE IGNORE NULLS returns the last known non-null value instead.
-- The explicit ROWS frame stops engines from looking past the current row.

SELECT
  customer_id,
  event_timestamp,
  event_sequence,
  LAST_VALUE(email) IGNORE NULLS OVER (
    PARTITION BY customer_id
    ORDER BY event_timestamp, event_sequence
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS current_email,
  LAST_VALUE(phone) IGNORE NULLS OVER (
    PARTITION BY customer_id
    ORDER BY event_timestamp, event_sequence
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS current_phone,
  LAST_VALUE(address) IGNORE NULLS OVER (
    PARTITION BY customer_id
    ORDER BY event_timestamp, event_sequence
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS current_address
FROM customer_profile_changes
WHERE customer_id = '98765'
ORDER BY event_timestamp, event_sequence;
