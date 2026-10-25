# kafka-flink-audit-trail

Companion repository for the CACM Practice article *The History Was Already There*: the
pipeline, queries, benchmarks and field study behind the article's measurements.

A Kafka and Flink pipeline that appends customer profile change events to an Apache
Iceberg table. The schema has no previous-value fields. The old value of a field is
read back at query time with SQL window functions: `LAG`, `ROW_NUMBER`, and
`LAST_VALUE ... IGNORE NULLS`.

Most of this runs on JDK 11+ and Maven alone. The Kafka and Flink path adds Docker, and
the benchmarks, the SQL matrix and the field study need Python 3.9 or later.

Every row in this repository is synthetic, except the Wikimedia field study's results,
which come from Wikimedia's public event stream. The demo seeds three events for one
customer, and the benchmarks generate their data from a fixed seed at demonstration
scale. Apart from that public stream, no production data exists here.

## Quickstart

```bash
make test        # unit tests, no infrastructure
make demo        # run five of the sql/ files over a seeded table and print the results
make bench       # schema size comparison on one synthetic event
pip install -r bench/requirements.txt   # Python 3.9 or later; make bench-full needs it
make bench-full  # the full benchmark; regenerates results/ (timed stages 449.1 s on an Apple M3)
make loop        # Flink writes a real Iceberg table, then DuckDB queries it back (a few seconds)
```

`make demo` seeds one customer's history (sparse updates, nothing overwritten) and runs
five of the eight files in `sql/` on an in-process DuckDB: `lag_history.sql`,
`latest_state.sql`, `duckdb/sparse_current_value.sql`, `duckdb/sparse_previous_value.sql`
and `dedup.sql`. The other three, `change_aware_previous_value.sql` and the
standard-spelling `sparse_current_value.sql` and `sparse_previous_value.sql`, run in the
CI matrix. Row 3's previous email is rebuilt from
earlier rows. `LAG(phone)` is null on the sparse rows, and `LAST_VALUE ... IGNORE NULLS`
carries the last known value forward. The demo then inserts a retried event and shows
`sql/dedup.sql` dropping it, and ends with the ordering counterexample.

`make loop` runs the job's row conversion and Iceberg sink in batch mode into a local
Iceberg table, then queries the table's Parquet data files with DuckDB, so every row went
event -> Flink -> Iceberg -> `LAG`. Batch mode needs no Kafka or Docker, just a few extra
seconds.

`make bench` compares the two schemas `bench/generate_schemas.py` builds from a
representative 80-attribute profile: 150 fields with the previous-value companions
against 83 without. The full benchmark (`make bench-full`) uses K=67 attributes instead,
which gives 137 fields against 70 (`results/SUMMARY.md`).

## Versions

- JDK 11 or later for the Maven build; Spark 4.0 (the SQL matrix, `bench/iceberg_layout.py`)
  and the AWS steps need JDK 17. CI builds on JDK 17.
- Python 3.9 or later; the committed results were produced on 3.9.6, and CI uses 3.11.
- Flink 1.20.3 in the root build and the cluster image (`flink:1.20.3-java17`). The AWS
  application in `bench/aws/flink-app` compiles against Flink 1.20.0 and runs on the
  Managed Service for Apache Flink 1.20 runtime.
- Iceberg 1.10.1 in both builds.
- Kafka: broker `apache/kafka:3.7.0` in Docker; client `kafka-clients` 3.4.0, through
  `flink-connector-kafka` 3.3.0-1.20; Amazon MSK 3.9.x (KRaft) in the AWS run.
- DuckDB 1.4.5: `duckdb==1.4.5` for the Python benchmarks, the field study and CI, and
  JDBC release 1.4.5.0 for the Java demo, loop and tests.
- Spark 4.0.4 (`pyspark==4.0.4`) for the SQL matrix and `bench/iceberg_layout.py`, which
  adds `iceberg-spark-runtime-4.0_2.13-1.10.1.jar`.
- Trino 483 (`trinodb/trino:483`) for the SQL matrix.
- Athena engine version 3, in the AWS run only.

## The queries

Every window orders events by `event_timestamp, event_sequence`. `event_sequence` is a
per-customer counter set by the producer, and it breaks exact timestamp ties.

| File | Returns | Written for |
|---|---|---|
| `sql/lag_history.sql` | previous value of each field, via `LAG` | DuckDB, Spark, Trino, Athena |
| `sql/latest_state.sql` | newest row per customer, via `ROW_NUMBER` | DuckDB, Spark, Trino, Athena |
| `sql/dedup.sql` | the table with repeated `(customer_id, event_sequence)` pairs dropped | DuckDB, Spark, Trino, Athena |
| `sql/change_aware_previous_value.sql` | value of email before email last changed, on full-state events, via `LAG`, a running `COUNT` of changes and `FIRST_VALUE` | DuckDB, Spark, Trino, Athena |
| `sql/sparse_current_value.sql` | last known value as of each event, frame ends at `CURRENT ROW` | Spark, Trino, Athena |
| `sql/sparse_previous_value.sql` | previous known value, frame ends at `1 PRECEDING` | Spark, Trino, Athena |
| `sql/duckdb/sparse_current_value.sql` | same query, DuckDB spelling | DuckDB |
| `sql/duckdb/sparse_previous_value.sql` | same query, DuckDB spelling | DuckDB |

`sql/lag_history.sql` and the four sparse files filter on `customer_id = '98765'`, the
customer of the demo and of the golden cases; the other three files return every
customer.

The two sparse `LAST_VALUE ... IGNORE NULLS` queries need a separate DuckDB form. The four
plain files (`LAG`, `ROW_NUMBER`, dedup and the change-aware query) have a single spelling
and run unchanged on DuckDB, Spark and Trino in CI, which checks every sql/ file on those
three engines. Athena is not in CI, and the AWS scale run executed only the benchmark's
full-state queries on Athena engine version 3, not the sql/ files. The sparse-update
queries exist in two spellings because
engines disagree on where `IGNORE NULLS` goes: Trino, Athena and Spark take it after the
closing parenthesis, `LAST_VALUE(x) IGNORE NULLS OVER (...)`; DuckDB takes it inside,
`LAST_VALUE(x IGNORE NULLS) OVER (...)`. In CI neither spelling parses on the other
engine (DuckDB against Spark and Trino); for Athena this is documented, not tested here.
Spark also accepts `LAST(x, true)`. Each file's `-- engines:` header names the engines
it is written for. `results/engine_docs.csv` gives, for DuckDB, Spark, Trino and Athena, the official page that defines LAST_VALUE with IGNORE NULLS, its documented version, and the syntax line it shows. The Trino and Athena pages do not show where IGNORE NULLS goes.

### Sparse updates

When an event carries only the fields that changed, `LAG` returns null whenever the
preceding row did not touch the field. Two queries answer the two questions:

- current value, `sql/sparse_current_value.sql`:
  `LAST_VALUE(x) IGNORE NULLS OVER (... ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`
- previous value, `sql/sparse_previous_value.sql`:
  `LAST_VALUE(x) IGNORE NULLS OVER (... ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)`

On a row that changes `x`, the second query returns the value it replaced. On a row that
does not touch `x`, it returns the current known value, because the null on that row is
ignored.

### Duplicates

A retry or replay resends the same event, so a duplicate appears as a repeated
`(customer_id, event_sequence)` pair. `sql/dedup.sql` keeps one row per pair with
`ROW_NUMBER`. Run it first, as a view or CTE in place of the raw table, so the window
queries never see the repeat.

### What exactly-once covers

Each Flink checkpoint commits the Kafka offsets and the Iceberg snapshot together. After
a failure, the job restarts from the last checkpoint and does not commit the same Kafka
records to Iceberg twice. That is the whole guarantee. A duplicate already present in
the topic, such as a producer retry that wrote the same event twice, is a separate Kafka
record and arrives as a separate Iceberg row. `sql/dedup.sql` is what drops it.

## Validated on three engines

`ci/run_sql_matrix.py` runs every file under `sql/` on DuckDB, Spark (local pyspark) and
Trino (Docker) against four golden cases, and compares the rows with the expected CSVs
under `ci/golden/`. GitHub Actions runs it on pushes to main and on pull requests;
`ci/matrix.md` is the matrix from the last local run. Three of the cases are the
article's: full-state events for customer 98765, sparse events with a phone-only row, and
a duplicated `(customer_id, event_sequence)`. Each SQL file declares the engines it is written for, and the run
fails if a file does not return exactly the expected rows on one of them.

```bash
pip install -r ci/requirements.txt
make trino-up
make sql-matrix
make trino-down
```

## Benchmark

`make bench-full` regenerates everything under `results/` from `bench/config.json`:
`results/SUMMARY.md` holds the tables, the CSV files hold every measured value, and
`results/environment.json` records the machine, the DuckDB version and the wall time.
`make bench-full` reproduces field counts, payload sizes and storage sizes exactly; latencies vary between runs; the ratios between designs are the stable result.
Resource use and cost are measured too (`results/resources.csv`, `results/cost.csv`): CPU time and peak memory vary across runs, while bytes scanned and cost reproduce exactly.
An attribute-count sweep repeats the field-count, Avro, Parquet and latency comparison at K = 10, 25, 67 and 150 on full-state events and reports the previous-value to audit-trail ratio at each point (`results/sweep_attributes.csv`).
`make bench-pipeline` measures both schemas through Kafka, Flink and Iceberg on a three-broker cluster (`make kafka3-up`): broker log bytes, producer send-to-acknowledgement latency, Flink throughput and checkpoint duration, and Iceberg metadata growth through snapshot expiration and compaction (`results/pipeline_*.csv`).
The data is synthetic, generated from the seed in the config at demonstration scale
(N entities, H events each, K attributes, default K=67). It compares three designs:

- A, previous-value schema: every attribute `x` has a companion `previous_x`.
- B, audit trail: attributes only; history is reconstructed with window functions.
- C, materialized current-state table: one row per entity holding the latest values.

Two event models are generated from one seed: full-state events, where every event
carries every attribute, and sparse events, where each event carries only the attributes
it changes and null for the rest. For each design the benchmark measures the schema
(field count, schema size), the Avro payload per event (mean, median, p95; uncompressed, deflate, snappy;
Confluent framing excluded), the Parquet size of the whole N x H dataset, and DuckDB
query latency (p50, p95, p99 over repeated runs) for one-hop previous value and the
attribute's value three events back on a single entity (on sparse events, where an
event carries an attribute only when it changes, this is three changes back), current
state and previous value over all entities, and the last-known and previous-known value
on sparse events. A sweep over H in 1, 10, 100, 1000 shows how latency and storage grow
with the length of a customer's history. Where a design cannot answer a question from
its own data, such as the value three events back on the one-hop schema A, the table
says so instead of a number.

```bash
pip install -r bench/requirements.txt
make bench-full      # timed stages 449.1 s on an Apple M3; results/SUMMARY.md
make bench-smoke     # tiny configuration, seconds; checks the script runs
```

## Further measurements

Four scripts behind sections of `results/SUMMARY.md` have no Make target. Each writes its
CSV files and its SUMMARY.md section into the `--out` directory, so `--out results`
replaces the committed files.

- `bench/sort_order.py`: the design B Parquet file in three row orders (`customer_id`;
  `customer_id, event_timestamp, event_sequence`; `customer_id` with hash order inside an
  entity), with file sizes and q1 and q3 latency and CPU time. It checks its sizes against
  the `storage.csv` that `make bench-full` writes into the same directory. Needs
  `bench/requirements.txt`.

  ```bash
  python3 bench/sort_order.py --config bench/config.json --out results
  ```

- `bench/iceberg_layout.py`: the Iceberg table as the Flink sink writes it, after the
  bin-pack compaction, and after a Spark sort rewrite. It checks the table as written
  against the `pipeline_iceberg_runs.csv` that `make bench-pipeline` writes into the same
  directory. Needs `bench/requirements.txt`, `ci/requirements.txt` (pyspark), Maven, and
  the Iceberg Spark runtime jar from Maven Central (`*.jar` is in `.gitignore`):

  ```bash
  curl -fLO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.10.1/iceberg-spark-runtime-4.0_2.13-1.10.1.jar
  python3 bench/iceberg_layout.py --config bench/config.json --out results \
    --spark-jar iceberg-spark-runtime-4.0_2.13-1.10.1.jar
  ```

- `ci/engine_scan_bytes.py`: Spark's and Trino's own input-byte counters for the `sql/`
  files in the sql-matrix setup. Needs `ci/requirements.txt` and `make trino-up`.

  ```bash
  python3 ci/engine_scan_bytes.py --engines spark,trino --out results
  ```

- `bench/engine_docs_fetch.py`: fetches the engine documentation pages that define
  LAST_VALUE with IGNORE NULLS and copies their syntax lines. Needs network access and
  `bench/requirements.txt`.

  ```bash
  python3 bench/engine_docs_fetch.py --out results
  ```

## Wikimedia field study

`study/wikimedia/` tests the article's claim on production data. Wikimedia's public
`mediawiki.page_change.v1` stream embeds a `prior_state` block in each event, which is
the previous-value pattern. The study captures the stream and loads it into Parquet. It
then reconstructs every `prior_state` field with the article's LAG query, classifies
every mismatch by cause, and measures what `prior_state` costs in JSON, Avro and Parquet
bytes and in query time. The results come from public Wikimedia data, not synthetic
data. They are in `results/wikimedia/` and the "Wikimedia field study" section of
`results/SUMMARY.md`. `study/wikimedia/README.md` gives the capture commands.
`make wikimedia-smoke` runs the same steps on a synthetic capture with planted
mismatches; CI runs it too.

## AWS scale run

`infra/` and `bench/aws/` run designs A and B through Amazon MSK, Amazon Managed Service for
Apache Flink and Iceberg on S3 with the Glue Data Catalog, at 1,000,000 entities x 100
events (100,000,000 events per design), then query A, B and C with Athena engine version
3. The events are synthetic, from the bench-full generator with the same seed and
attribute list, as full-state events, so q2_three_changes_back_single returns the
attribute's value three events back. The results are in `results/aws/` and the "AWS
scale run" section of `results/SUMMARY.md`.

**Cost warning.** This creates billed resources in your AWS account. `infra/COST_ESTIMATE.md`
estimates 71.50 USD for the whole run from on-demand us-east-1 prices, and lists what each
stage costs per hour while it exists (up to 2.92 USD per hour while a Flink application
runs). The estimate assumes Managed Flink parallelism 12; the run used parallelism 4,
because Managed Flink rejected higher parallelism until AWS raises the account's KPU
limit. Nothing stops on its own: tear down as `infra/TEARDOWN.md` describes, and keep the
budget from `infra/budget` until the charges have posted.

Rerun, from a machine with the AWS CLI, Terraform, JDK 17, Maven and `bench/requirements.txt`:

1. In the IAM console, create `infra/iam/role-boundary-policy.json` as the policy
   `audit-trail-bench-boundary`, and attach `infra/iam/cli-run-policy-infra.json` and
   `infra/iam/cli-run-policy-services.json` (with your own bucket name in place of
   `ARCHIVE_BUCKET_NAME`, or without that statement) to the IAM user the CLI uses.
2. `terraform -chdir=infra/budget init`, then
   `TF_VAR_alert_email=<address> terraform -chdir=infra/budget apply -var budget_usd=<amount>`
3. `terraform -chdir=infra/base init`, `terraform -chdir=infra/base apply`, then
   `python3 bench/aws/run.py --config bench/aws/config.aws.json setup` and `generate`.
4. `(cd bench/aws/flink-app && mvn -q -B package)`, then `terraform -chdir=infra/stream init`
   and `terraform -chdir=infra/stream apply`.
5. `python3 bench/aws/run.py --config bench/aws/config.aws.json pipeline`, `environment`,
   then `terraform -chdir=infra/stream destroy`.
6. With the same `--config`: `load-c`, `equivalence`, `stats after_commits`, `expire`,
   `stats after_expiration`, `compact`, `stats after_compaction`, `expire`,
   `stats after_compaction_and_expiration`, `athena-timing`.
7. `terraform -chdir=infra/base destroy`; once Cost Explorer shows the charges,
   `python3 bench/aws/report.py costs --start <first day> --end <day after the last>`,
   `python3 bench/aws/report.py summary`, `terraform -chdir=infra/budget destroy`, and
   `python3 bench/aws/teardown_check.py --out results/aws/teardown_listing.txt --archive-bucket <name>`.
8. Remove the three IAM policies (`infra/TEARDOWN.md`, step 6), then
   `python3 bench/aws/permissions_check.py --out results/aws/permissions_check.txt`.

`bench/aws/config.aws-smoke.json` runs the same steps at the bench-full scale (1,000
entities) first; its files go to `results/aws/smoke/`. Nothing in CI touches AWS: the
`aws-offline` job checks the step files against `benchmark.generate()`, runs the Flink
application's unit tests, validates the Terraform and regenerates the estimate.

## Layout

```
sql/
  lag_history.sql                previous value of each field via LAG
  latest_state.sql               newest row per customer via ROW_NUMBER
  dedup.sql                      one row per (customer_id, event_sequence)
  change_aware_previous_value.sql value of email before email last changed
  sparse_current_value.sql       last known value, standard IGNORE NULLS spelling
  sparse_previous_value.sql      previous known value, standard IGNORE NULLS spelling
  duckdb/                        the two sparse queries in the DuckDB spelling

src/main/avro/
  customer_profile_change_event.avsc   the event contract to register

src/test/java/com/example/audit/
  CrashRestartTest.java                 fail, restart from the last checkpoint, assert counts
  IcebergRoundTripTest.java             Flink writes Iceberg in batch mode, LAG reads it back

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
  benchmark.py                          what `make bench-full` runs
  config.json                           every benchmark parameter, including the seed
  config.smoke.json                     tiny configuration for `make bench-smoke` and CI

results/
  SUMMARY.md                            benchmark tables, environment, wall time
  *.csv, explain/                       every measured value and the EXPLAIN ANALYZE text
  crash_restart_case1.md, _case2.md     what CrashRestartTest observed on the last run

ci/
  run_sql_matrix.py                     every sql/ file on DuckDB, Spark, Trino vs golden outputs
  golden/                               four cases: input.csv and expected/<query>.csv
  matrix.md                             the dialect matrix from the last local run

docs/
  AUDIT.md                              historical record: the repository at 7eeae21 against the article

study/wikimedia/
  capture.py                            Wikimedia EventStreams capture with offset resume
  study.py                              steps 3 to 7: Parquet, LAG versus prior_state, size, cost
  README.md                             data handling and the commands to capture again

infra/
  budget/, base/, stream/               Terraform for the AWS scale run, applied in that order
  iam/                                  the IAM policies the CLI user and the run's roles need
  estimate.py, COST_ESTIMATE.md         the cost estimate from Price List prices
  TEARDOWN.md                           what is destroyed when, and how teardown is proved

bench/aws/
  generate_steps.py                     the bench-full generator, written as per-event-index step files
  flink-app/                            the Managed Flink job and the runner tools (own Maven build)
  run.py                                drives the run: runner, Flink, Athena, CloudWatch, Budgets
  report.py                             summary CSVs and the SUMMARY.md section
```

Events go over the wire as JSON so the pipeline runs without a schema registry. The
Avro schema in `src/main/avro` is the contract to register, and the form the byte
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

`make produce` sends the article's example: three full-state events for customer 98765,
each carrying email and phone. `make run` prints one line per event (the `1>` prefix is
Flink's print sink naming the subtask):

```
1> APPEND  customer_id=98765 seq=1 ts=1000 email=alice@gmail.com phone=555-397 address=null
1> APPEND  customer_id=98765 seq=2 ts=2000 email=alice@yahoo.com phone=555-397 address=null
1> APPEND  customer_id=98765 seq=3 ts=3000 email=alice@newjob.com phone=555-493 address=null
```

Every event is a new row; nothing gets updated. `sql/lag_history.sql` over those three
rows returns the previous values without any previous-value field in the schema
(address columns omitted; they are null throughout):

| event_timestamp | event_sequence | email | previous_email | phone | previous_phone |
|---|---|---|---|---|---|
| 1000 | 1 | alice@gmail.com | null | 555-397 | null |
| 2000 | 2 | alice@yahoo.com | alice@gmail.com | 555-397 | 555-397 |
| 3000 | 3 | alice@newjob.com | alice@yahoo.com | 555-493 | 555-397 |

These rows are golden case 1 in `ci/golden/case1_full_state`, which CI checks on DuckDB,
Spark and Trino.

The job checkpoints every 60 s with a 30 s minimum pause between checkpoints, in
exactly-once mode, and uses `WatermarkStrategy.noWatermarks()`: there is no event-time
windowing and no operator state beyond the Kafka offsets and the Iceberg committer.

### Writing Iceberg

`--sink iceberg` writes an append-only table partitioned by day on `event_timestamp`. The
table declares sort order `customer_id, event_timestamp, event_sequence`, but the streaming
writer does not sort rows within files. Only a compaction that sorts applies the order,
such as Spark's `rewrite_data_files` with `strategy => 'sort'`. The compaction in the
pipeline benchmark and the AWS run (Flink `Actions.rewriteDataFiles`) bin-packs files and
does not sort. The sorted-table measurements in `results/` come from the Spark sort
rewrite that `bench/iceberg_layout.py` runs on the table as written
(`results/iceberg_layout_*.csv` and the "Iceberg data layout" section of
`results/SUMMARY.md`). To run the sink:

```bash
# local filesystem, no S3:
mvn -q compile exec:exec@run-job -Djob.args="--sink iceberg --warehouse file:///tmp/audit-warehouse"
# S3:
mvn -q compile exec:exec@run-job -Djob.args="--sink iceberg --warehouse s3://your-bucket/warehouse --io-impl org.apache.iceberg.aws.s3.S3FileIO"
```

`make loop` and `IcebergRoundTripTest` exercise the write path into a real Iceberg table.
`IcebergRoundTripTest` reads the rows back through Iceberg's own reader; `make loop` reads
the table's Parquet data files with DuckDB.

### Crash and restart evidence

`CrashRestartTest` runs the job against a real Kafka broker, twice:

1. Recovery. 5,000 distinct events go to a fresh topic. The job checkpoints every 200 ms
   with retained checkpoints, and an operator between the source and the sink throws
   once after at least one checkpoint has completed and 2,000 records have passed. The
   restart strategy is "none", so the job dies. A second submission of the same pipeline
   restores from the last completed checkpoint. The test asserts that the Iceberg table
   then holds exactly 5,000 rows and no repeated `(customer_id, event_sequence)`: the
   records between the last commit and the failure were replayed from Kafka and
   committed once.
2. A duplicate already in the topic. The same events plus one of them sent a second
   time, with no failure. The test asserts 5,001 rows, 5,000 distinct pairs, and that
   `sql/dedup.sql` returns 5,000. Exactly-once covers the first case, not the second.

Both tests write what they observed to `results/crash_restart_case1.md` and
`results/crash_restart_case2.md`, and every run rewrites both files. In case 1 the number
of rows committed before the failure, and so the number replayed, differs from run to
run, so a local `make crash-test` leaves the committed case 1 file changed. GitHub
Actions runs both tests against a Kafka service container on pushes to main and on pull
requests. Locally:

```bash
make up
make crash-test
make down
```

Without a reachable broker `make test` skips both CrashRestartTest cases. The broker
check runs before either case starts, so Maven reports CrashRestartTest as
`Tests run: 0, ... Skipped: 0` rather than as two skipped tests. `make crash-test` sets
`REQUIRE_KAFKA=1` so they fail instead.

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

`LAG` only gives the right answer if events for one customer are in order. Timestamp
order holds when one producer stamps each customer's events with a clock that only moves
forward, but two events can still share a timestamp. Every event therefore carries
`event_sequence`, a per-customer counter set by the producer, and the queries order by
`event_timestamp, event_sequence`: the timestamp first, the sequence breaking ties.
Where a producer's clock can step back, or several producers write the same customer,
timestamps are no longer a valid order. The sequence then leads the sort, and it has to
come from a source every writer respects, such as the source database's version number
or transaction-log position. The last section of `make demo` shows two events
that share a timestamp: sorted by timestamp alone the tied rows can come out in the wrong
order and `LAG` reports the wrong previous value with no error; `event_sequence` pins the
order. `OrderingInvariantTest` checks both outcomes.

## License

Apache 2.0
