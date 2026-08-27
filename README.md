# kafka-flink-audit-trail

Companion to the CACM Practice article *The History Was Already There*, under review.

A Kafka and Flink pipeline that appends customer profile change events to an Apache
Iceberg table. The schema has no previous-value fields. The old value of a field is
read back at query time with SQL window functions: `LAG`, `ROW_NUMBER`, and
`LAST_VALUE ... IGNORE NULLS`.

Most of this runs on JDK 11+ and Maven alone. The Kafka and Flink path adds Docker.

## Quickstart

```bash
make test     # unit tests, no infrastructure
make demo     # run the sql/ queries over a seeded table and print the results
make bench    # schema size comparison
make loop     # Flink writes a real Iceberg table, then query it back (a few seconds)
```

`make demo` seeds one customer's history (sparse updates, nothing overwritten) and runs
the three queries. Row 3's previous email is rebuilt from earlier rows. `LAG(phone)` is
null on the sparse rows, and `LAST_VALUE ... IGNORE NULLS` carries the last known value
forward. The queries run on DuckDB here. `lag_history.sql` and `latest_state.sql` run
unchanged on Spark, Trino, and Athena over Iceberg. `sparse_updates.sql` is the one
exception: engines disagree on where `IGNORE NULLS` goes, so the Trino/Athena/Spark
spelling lives in `sparse_updates_portable.sql`.

`make loop` runs the real Flink job into a local Iceberg table and queries it back, so
every row went event -> Flink -> Iceberg -> `LAG`. It runs in batch mode, so it needs no
Kafka or Docker, just a few extra seconds.

## Layout

```
sql/
  lag_history.sql      previous value of each field via LAG
  latest_state.sql     latest row per customer via ROW_NUMBER
  sparse_updates.sql   last known value via LAST_VALUE IGNORE NULLS (DuckDB spelling)
  sparse_updates_portable.sql  the same query, Trino/Athena/Spark spelling

src/main/avro/
  customer_profile_change_event.avsc   the event contract you'd register

src/main/java/com/example/audit/
  CustomerProfileAuditJob.java          the Flink job (prints rows, or writes Iceberg)
  CustomerProfileChangeEvent.java       the event POJO
  CustomerProfileChangeDeserializer.java  reads JSON off Kafka
  demo/DemoRunner.java                  what `make demo` runs (DuckDB)
  bench/SchemaBenchmark.java            what `make bench` runs (Avro)

bench/
  generate_schemas.py                   one attribute list, two schemas
  customer_profile_previous_value.avsc  generated: attributes plus previous_* companions
  customer_profile_audit_trail.avsc     generated: attributes only
```

Events go over the wire as JSON so the pipeline runs without a schema registry. The
Avro schema in `src/main/avro` is the contract you'd register, and the form the byte
counts are measured against.

## The pipeline (Kafka to Flink)

`make run` is a streaming job: it keeps running and holds the terminal until you stop
it with Ctrl+C. So run `make produce` from a second terminal while `make run` is going.

```bash
# terminal 1
make up        # start Kafka, create the topic (Docker)
make run       # run the Flink job (no Flink install); stays running, prints each row

# terminal 2 (leave make run going)
make produce   # send the three sample events; watch them appear in terminal 1

# back in terminal 1: Ctrl+C to stop the job, then
make down      # stop Kafka
```

`make run` prints one line per event:

```
APPEND  customer_id=98765 seq=1 email=alice@gmail.com   phone=555-1001 address=123 Main St
APPEND  customer_id=98765 seq=2 email=alice@yahoo.com   phone=null     address=null
APPEND  customer_id=98765 seq=3 email=alice@outlook.com phone=null     address=null
```

Every event is a new row; nothing gets updated.

### Writing Iceberg

`--sink iceberg` writes an append-only table partitioned by day on `event_timestamp`
and sorted by `customer_id`:

```bash
# local filesystem, no S3:
mvn -q compile exec:exec@run-job   # add: --sink iceberg --warehouse file:///tmp/audit-warehouse
# S3:
#   --sink iceberg --warehouse s3://your-bucket/warehouse \
#   --io-impl org.apache.iceberg.aws.s3.S3FileIO
```

Each Flink checkpoint commits the Kafka offsets and the Iceberg snapshot together, so a
restart picks up where it left off without duplicates. `make loop` and
`IcebergRoundTripTest` exercise the write path into a real Iceberg table and read the
rows back through Iceberg's own reader.

### On a real cluster

```bash
make build
docker compose -f docker-compose.cluster.yml up -d   # JobManager, TaskManager, Kafka; UI on :8081
docker compose -f docker-compose.cluster.yml exec kafka \
  /opt/kafka/bin/kafka-topics.sh --create --topic customer-profile-changes --bootstrap-server kafka:9092
docker compose -f docker-compose.cluster.yml exec jobmanager \
  flink run -d -c com.example.audit.CustomerProfileAuditJob \
  /jars/kafka-flink-audit-trail-1.0.0.jar --kafka-bootstrap-servers kafka:9092
docker compose -f docker-compose.cluster.yml down -v
```

## Why event_sequence

`LAG` only gives the right answer if events for one customer are in order. Timestamps
alone aren't enough once you have clock skew, retries, and batching, so every event
carries `event_sequence`, a per-customer counter set by the producer. The queries order
by `event_timestamp, event_sequence`. The last section of `make demo` shows two events
that share a timestamp: sorted by timestamp alone the tied rows can come out in the wrong
order and `LAG` reports the wrong previous value with no error; `event_sequence` pins the
order. `OrderingInvariantTest` checks both outcomes.

## License

Apache 2.0
