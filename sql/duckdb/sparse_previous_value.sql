-- DuckDB variant of sql/sparse_previous_value.sql. Same query, same result; DuckDB
-- takes IGNORE NULLS inside the parentheses. Spark and Trino reject this spelling in
-- CI and take the standard one in sql/sparse_previous_value.sql; for Athena this is
-- documented, not tested here.
-- engines: duckdb
-- Previous known value of each field. The frame ends at 1 PRECEDING.

SELECT
  customer_id,
  event_timestamp,
  event_sequence,
  LAST_VALUE(email IGNORE NULLS) OVER (
    PARTITION BY customer_id
    ORDER BY event_timestamp, event_sequence
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
  ) AS previous_email,
  LAST_VALUE(phone IGNORE NULLS) OVER (
    PARTITION BY customer_id
    ORDER BY event_timestamp, event_sequence
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
  ) AS previous_phone,
  LAST_VALUE(address IGNORE NULLS) OVER (
    PARTITION BY customer_id
    ORDER BY event_timestamp, event_sequence
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
  ) AS previous_address
FROM customer_profile_changes
WHERE customer_id = '98765'
ORDER BY event_timestamp, event_sequence;
