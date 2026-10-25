# Crash and restart, case 1: recovery from the last checkpoint

Observed by CrashRestartTest against Kafka at localhost:9092.

| Measure | Value |
|---|---|
| Distinct input events produced | 5000 |
| Kafka records in the topic (sum of partition end offsets) | 5000 |
| Records that passed the failing operator before it threw | 2000 |
| Checkpoints completed before the failure | 16 |
| Iceberg rows committed when run 1 died | 1939 (16 snapshots) |
| Records between the last commit and the failure, replayed from Kafka | 61 |
| Restored from | chk-16 |
| Iceberg rows after run 2 | 5000 (18 snapshots) |
| Distinct (customer_id, event_sequence) after run 2 | 5000 |
| Rows added by recovery beyond the input | 0 |

The machine-specific temporary folder in front of chk-16 was removed on 2026-10-25; nothing else in this run's record changed.
