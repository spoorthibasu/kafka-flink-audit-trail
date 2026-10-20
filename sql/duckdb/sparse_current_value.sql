-- DuckDB variant of sql/sparse_current_value.sql. Same query, same result; the only
-- difference is where IGNORE NULLS goes. DuckDB takes it inside the parentheses:
-- LAST_VALUE(email IGNORE NULLS). Spark, Trino and Athena reject this spelling and
-- take the standard one in sql/sparse_current_value.sql.
-- engines: duckdb
-- Last known value of each field as of each event. The frame ends at CURRENT ROW.

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
