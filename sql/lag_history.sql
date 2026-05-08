-- Reconstruct previous values for any field using LAG.
-- No previous fields needed in the schema.
-- Ordering uses event_sequence as a tiebreaker when timestamps collide.

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

-- To look back further, increase the offset.
-- LAG(email, 2) returns the value two changes ago.
-- LAG(email, 3) returns three changes ago.
-- The schema stays the same regardless of depth.
