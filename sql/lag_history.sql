-- Reconstruct previous values using LAG. No previous fields in the schema.
-- event_sequence breaks ties when two events share the same timestamp.

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

-- To go further back: LAG(email, 2), LAG(email, 3), etc.
-- Schema doesn't change; only the query does.
