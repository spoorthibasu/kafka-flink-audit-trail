-- Retrieve the most recent state per customer.
-- ROW_NUMBER() is portable across Spark, Trino, and Athena.
-- List columns explicitly in the outer SELECT to exclude the rn column.

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
)
WHERE rn = 1;
