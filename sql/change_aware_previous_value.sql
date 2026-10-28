-- Value of email before email last changed, as of each event. A previous-value field
-- that holds "the value before the attribute last changed" needs this form: on
-- full-state events most rows repeat email unchanged, and LAG(email) returns that same
-- value on them, not the value the last change replaced.
-- Three steps, each ordered by event_timestamp, event_sequence within customer_id:
--   (a) prev_email is LAG(email), the previous event's email.
--   (b) change_grp counts the rows so far whose email IS DISTINCT FROM prev_email. It
--       goes up by one on each row that changes email and holds on the rows after it.
--   (c) FIRST_VALUE(prev_email) over customer_id, change_grp is prev_email on the row
--       that started the group: the value that change replaced.
-- IS DISTINCT FROM treats NULL as a value, so a change to NULL and a change from NULL
-- each start a group, and for an email history of a, NULL, b the result at b is NULL.
-- IGNORE NULLS is not used: it cannot tell a row that is not a change from a change
-- whose prior value was NULL, and returns a at b.
-- prev_email is NULL on the first event, so the result is NULL until a change replaces
-- a non-NULL email, whether the first email is NULL or not.
-- engines: duckdb, spark, trino, athena
-- Plain LAG, COUNT and FIRST_VALUE with explicit ROWS frames, so the same text runs on
-- every engine listed.
-- CI checks DuckDB, Spark and Trino. This file was run on Athena engine version 3
-- against the golden outputs (results/athena_sql_check.md); Athena is not in CI.
-- For full-state events. On sparse events a NULL email means the event did not carry
-- email; use sparse_previous_value.sql there.
-- Run sql/dedup.sql first if the topic may contain retried or replayed events: copies
-- of one event tie on event_timestamp, event_sequence, and a ROWS frame leaves the
-- order of tied rows to the engine.

SELECT
  customer_id,
  event_timestamp,
  event_sequence,
  email,
  FIRST_VALUE(prev_email) OVER (
    PARTITION BY customer_id, change_grp
    ORDER BY event_timestamp, event_sequence
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS email_before_last_change
FROM (
  SELECT
    customer_id,
    event_timestamp,
    event_sequence,
    email,
    prev_email,
    COUNT(CASE WHEN email IS DISTINCT FROM prev_email THEN 1 END) OVER (
      PARTITION BY customer_id
      ORDER BY event_timestamp, event_sequence
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS change_grp
  FROM (
    SELECT
      customer_id,
      event_timestamp,
      event_sequence,
      email,
      LAG(email) OVER (
        PARTITION BY customer_id
        ORDER BY event_timestamp, event_sequence
      ) AS prev_email
    FROM customer_profile_changes
  ) step_a
) step_b
ORDER BY customer_id, event_timestamp, event_sequence;
