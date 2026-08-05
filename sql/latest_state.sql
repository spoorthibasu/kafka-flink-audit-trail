-- Most recent state per customer. Columns listed explicitly to drop rn.
-- Plain ROW_NUMBER, so this runs as written on DuckDB, Spark, Trino, and Athena.

SELECT
  customer_id,
  event_timestamp,
  event_sequence,
  email,
  phone,
  address
FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY event_timestamp DESC, event_sequence DESC
    ) AS rn
  FROM customer_profile_changes
) t
WHERE rn = 1;
