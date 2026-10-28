-- Last known value of each field as of each event.
-- For sparse events an untouched field is null, so LAG returns null when the preceding
-- row did not touch it. LAST_VALUE ... IGNORE NULLS returns the most recent non-null
-- value instead. The frame ends at CURRENT ROW, so a row that changes email reports
-- the new email.
-- engines: spark, trino, athena
-- Standard spelling: IGNORE NULLS after the closing parenthesis, before OVER. Trino
-- and Spark accept it in CI. This file was run on Athena engine version 3 against the
-- golden outputs (results/athena_sql_check.md); Athena is not in CI. DuckDB rejects
-- it; use sql/duckdb/sparse_current_value.sql there. Spark also accepts
-- LAST(email, true) OVER (...).
-- Companion query: sparse_previous_value.sql ends the frame at 1 PRECEDING.

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
