# Crash and restart, case 2: a duplicate already in the topic

Observed by CrashRestartTest against Kafka at localhost:9092.

| Measure | Value |
|---|---|
| Distinct events produced | 5000 |
| Event sent a second time | customer_id=10025, event_sequence=1 |
| Kafka records produced | 5001 |
| Kafka records in the topic (sum of partition end offsets) | 5001 |
| Iceberg rows after the job | 5001 |
| Distinct (customer_id, event_sequence) | 5000 |
| Rows returned by sql/dedup.sql | 5000 |
