-- Drop duplicates before computing any window.
-- A retry or replay resends the same event, including its event_sequence, so a
-- duplicate appears as a repeated (customer_id, event_sequence) pair. ROW_NUMBER over
-- that pair keeps one row per pair. Exactly-once in the Flink job does not remove
-- these: it stops Flink committing the same Kafka records twice on recovery, and a
-- duplicate already in the topic is a separate Kafka record.
-- engines: duckdb, spark, trino, athena
-- Plain ROW_NUMBER, so the same text runs on every engine listed.
-- Use the result as a view or CTE in place of customer_profile_changes in the other
-- queries.

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
      PARTITION BY customer_id, event_sequence
      ORDER BY event_timestamp, event_sequence
    ) AS dup_rank
  FROM customer_profile_changes
) t
WHERE dup_rank = 1
ORDER BY customer_id, event_timestamp, event_sequence;
