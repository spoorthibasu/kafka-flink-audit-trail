# kafka-flink-audit-trail

A Kafka + Flink pipeline that writes customer profile change events to an append-only Iceberg table on S3. Previous field values are not stored in the schema. All historical state is reconstructed at query time using SQL window functions.

## Stack

- Apache Flink 1.20.3
- Apache Iceberg 1.10.1
- flink-connector-kafka 3.3.0-1.20
- Java 11

## Project structure

```
src/main/java/com/example/audit/
  CustomerProfileAuditJob.java            Flink job
  CustomerProfileChangeEvent.java         event POJO
  CustomerProfileChangeDeserializer.java  JSON deserializer

src/main/avro/
  customer_profile_change_event.avsc      Avro schema reference

sql/
  lag_history.sql       previous value reconstruction with LAG
  latest_state.sql      most recent state per customer
  sparse_updates.sql    carry forward non-null values with LAST_VALUE IGNORE NULLS
```

## Configuration

Update `CustomerProfileAuditJob.java` before running:

- `setBootstrapServers` — Kafka broker addresses
- `warehouse` — S3 path for the Iceberg warehouse
- `io-impl` — swap `S3FileIO` for `GCSFileIO` or `ADLSFileIO` if not on AWS

## Build

```bash
mvn clean package
```

## How it works

Every event is appended as a new row. The table is never updated or overwritten. Checkpoints tie Kafka offset commits to Iceberg snapshot commits, so a restart picks up from the last consistent state without producing duplicates.

The table is partitioned by day on `eventTimestamp` and sorted by `customerId` within each partition. That sort order matters for query performance: LAG window functions only need the rows for one customer, and keeping those rows together cuts the amount of data scanned.

`eventSequence` is a monotonic counter per customer assigned at the producer. It breaks ties when two events land with the same timestamp, which is common at high throughput.

## Querying

The `sql/` directory has ready-to-run queries for Spark, Trino, and Athena:

- `lag_history.sql` — reconstruct what any field held at any point in history
- `latest_state.sql` — one row per customer showing current state
- `sparse_updates.sql` — handle fields that are not updated on every event
