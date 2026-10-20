-- Previous value of each field via LAG. The schema has no previous-value fields; the
-- old value is read from the earlier row at query time.
-- Events are ordered by event_timestamp, event_sequence. event_sequence is a
-- per-customer counter set by the producer and breaks exact timestamp ties.
-- engines: duckdb, spark, trino, athena
-- Plain LAG, so the same text runs on every engine listed.
-- Run sql/dedup.sql first if the topic may contain retried or replayed events;
-- a repeated (customer_id, event_sequence) pair otherwise appears as its own row.
-- Further back: LAG(email, 2), LAG(email, 3), and so on. The schema does not change.

SELECT
  customer_id,
  event_timestamp,
  event_sequence,
  email,
  LAG(email) OVER (
    PARTITION BY customer_id
    ORDER BY event_timestamp, event_sequence
  ) AS previous_email,
  phone,
  LAG(phone) OVER (
    PARTITION BY customer_id
    ORDER BY event_timestamp, event_sequence
  ) AS previous_phone,
  address,
  LAG(address) OVER (
    PARTITION BY customer_id
    ORDER BY event_timestamp, event_sequence
  ) AS previous_address
FROM customer_profile_changes
WHERE customer_id = '98765'
ORDER BY event_timestamp, event_sequence;
