-- Previous known value of each field: the most recent non-null value strictly before
-- each event. Same query as sparse_current_value.sql with the frame ending at
-- 1 PRECEDING instead of CURRENT ROW. On a row that changes email, previous_email is
-- the value it replaced. On a row that does not touch email, previous_email equals the
-- current known value, because the null on this row is ignored.
-- engines: spark, trino, athena
-- Standard spelling: IGNORE NULLS after the closing parenthesis, before OVER. DuckDB
-- rejects it; use sql/duckdb/sparse_previous_value.sql there.
-- This file was run on Athena engine version 3 against the golden outputs
-- (results/athena_sql_check.md); Athena is not in CI.

SELECT
  customer_id,
  event_timestamp,
  event_sequence,
  LAST_VALUE(email) IGNORE NULLS OVER (
    PARTITION BY customer_id
    ORDER BY event_timestamp, event_sequence
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
  ) AS previous_email,
  LAST_VALUE(phone) IGNORE NULLS OVER (
    PARTITION BY customer_id
    ORDER BY event_timestamp, event_sequence
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
  ) AS previous_phone,
  LAST_VALUE(address) IGNORE NULLS OVER (
    PARTITION BY customer_id
    ORDER BY event_timestamp, event_sequence
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
  ) AS previous_address
FROM customer_profile_changes
WHERE customer_id = '98765'
ORDER BY event_timestamp, event_sequence;
