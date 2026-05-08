-- Sparse update pattern: not every field changes on every event.
-- LAG returns null if the immediately preceding event did not update the field,
-- even if the field has a known value from an earlier row.
-- LAST_VALUE IGNORE NULLS returns the most recent non-null value instead.

SELECT
  customer_id,
  event_timestamp,
  event_sequence,
  LAST_VALUE(email) IGNORE NULLS OVER (
    PARTITION BY customer_id
    ORDER BY event_timestamp, event_sequence
  ) AS current_email,
  LAST_VALUE(phone) IGNORE NULLS OVER (
    PARTITION BY customer_id
    ORDER BY event_timestamp, event_sequence
  ) AS current_phone,
  LAST_VALUE(address) IGNORE NULLS OVER (
    PARTITION BY customer_id
    ORDER BY event_timestamp, event_sequence
  ) AS current_address
FROM customer_profile_changes
WHERE customer_id = '98765'
ORDER BY event_timestamp, event_sequence;
