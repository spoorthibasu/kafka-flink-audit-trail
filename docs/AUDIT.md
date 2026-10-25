# Repository audit against the revised article

Taken before any revision change. Each item records what the repository contained at
commit 7eeae21 and whether it matches the article text.

Dialect behaviour below was checked by running each spelling on the engine named, on
2026-10-20: DuckDB 1.4.5 (Python), Spark 4.0.4 (local pyspark), Trino 483 (Docker).

## SQL files

| File | Window function | IGNORE NULLS spelling | Frame | ORDER BY | Engines claimed in header |
|---|---|---|---|---|---|
| `sql/lag_history.sql` | `LAG(x)` | not used | default | `event_timestamp, event_sequence` | DuckDB, Spark, Trino, Athena |
| `sql/latest_state.sql` | `ROW_NUMBER()` | not used | default | `event_timestamp DESC, event_sequence DESC` | DuckDB, Spark, Trino, Athena |
| `sql/sparse_updates.sql` | `LAST_VALUE(x IGNORE NULLS)` | inside the parentheses (DuckDB) | `UNBOUNDED PRECEDING AND CURRENT ROW` | `event_timestamp, event_sequence` | DuckDB only |
| `sql/sparse_updates_portable.sql` | `LAST_VALUE(x) IGNORE NULLS` | after the closing parenthesis (standard) | `UNBOUNDED PRECEDING AND CURRENT ROW` | `event_timestamp, event_sequence` | Trino, Athena, Spark |

Missing against the article:

- No query with the `ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING` frame (previous
  known value). Only the current-value frame exists.
- No deduplication query. Nothing in the repository drops a repeated
  `(customer_id, event_sequence)` pair.

Every `ORDER BY` already uses `event_timestamp, event_sequence` (ascending for the
window queries, descending for `latest_state.sql`, which selects the newest row). The
timestamp-only sort in `DemoRunner.orderingInvariant` is the deliberate counterexample
and stays.

Dialect check results:

| Spelling | DuckDB 1.4.5 | Spark 4.0.4 | Trino 483 |
|---|---|---|---|
| `LAST_VALUE(x) IGNORE NULLS OVER (...)` | parse error | runs | runs |
| `LAST_VALUE(x IGNORE NULLS) OVER (...)` | runs | parse error | parse error |
| `LAST(x, true) OVER (...)` | not tested | runs | not tested |

This matches the article's dialect statement. Athena was not tested; it is Trino-based
and the article's claim for it is carried on that basis.

## README claims

| Line | Claim | Status |
|---|---|---|
| 24 to 27 | `lag_history.sql` and `latest_state.sql` run unchanged on Spark, Trino and Athena; `sparse_updates.sql` is the exception with a portable twin | Consistent with the dialect findings, but not validated by CI. CI ran DuckDB only, through the Java demo. |
| 102 to 103 | "Each Flink checkpoint commits the Kafka offsets and the Iceberg snapshot together, so a restart picks up where it left off without duplicates." | Broader than the article. Exactly-once covers Flink's own recovery path. Duplicates already in the topic land as separate rows. |
| 14 to 19 | `make bench` is "schema size comparison" | No statement that the data is synthetic or that the scale is a demonstration. |
| whole file | No mention of duplicates in the topic or how to drop them | Missing against the article. |
| 76 to 84, Makefile `produce` | Sample events use phone `555-1001` and third email `alice@outlook.com` | Differs from the article's golden case 1 (`555-397`, `555-493`, `alice@newjob.com`). The sample is sparse, the golden case is full-state. Not part of the article list; recorded for the authors. |

## Flink configuration (`CustomerProfileAuditJob.java`)

| Setting | Repository | Article |
|---|---|---|
| Checkpoint interval | `--checkpoint-interval-ms`, default 60000 | every 60 s |
| Checkpoint mode | `CheckpointingMode.EXACTLY_ONCE` | exactly-once |
| Min pause between checkpoints | `setMinPauseBetweenCheckpoints(30_000)` | 30 s |
| Watermarks | `WatermarkStrategy.noWatermarks()` | `noWatermarks()` |

All four match.

## Iceberg table (`CustomerProfileAuditJob.buildTableLoader`)

| Setting | Repository | Article |
|---|---|---|
| Partition spec | `PartitionSpec.builderFor(schema).day("event_timestamp")` | partitioned by event date on `event_timestamp` |
| Sort order | `replaceSortOrder().asc("customer_id").asc("event_timestamp").asc("event_sequence")` (`customer_id` alone at 7eeae21), a declared sort order; `FlinkSink` writes rows in arrival order and does not sort within files | the streaming writer does not sort rows within data files; only compaction does, and it should sort by `customer_id` followed by the ordering key |

Both match. The declared sort order is `customer_id` followed by the ordering key, and both
say the streaming writer does not sort rows within files.

## Make targets

`help`, `test`, `demo`, `bench`, `loop`, `build`, `up`, `run`, `produce`, `down`.

No target runs the SQL on more than one engine, no target produces benchmark CSVs, and
no target exercises crash recovery.

## CI (`.github/workflows/ci.yml`)

One job on `ubuntu-latest`: generate bench schemas, `mvn test`, run `DemoRunner`, run
`SchemaBenchmark`, build the fat jar. Every SQL execution happens in DuckDB through the
JDBC driver (version 1.1.3 in `pom.xml`). No Spark, no Trino, no golden-output assertion
beyond the three values checked in `DemoQueriesTest`.

## Existing benchmark (`SchemaBenchmark.java`, `bench/generate_schemas.py`)

- 67 mutable attributes plus 13 system attributes, 80 in total. `SchemaBenchmarkTest`
  pins the 67 companion fields.
- Measures one representative event for each of 3, 8 and 16 changed attributes. Avro
  binary encoding only, no compression, no Confluent framing. The absence of framing is
  not stated anywhere.
- No storage, query latency or history-length measurements.

## Prose

The word "we" appears in comments in `IcebergRoundTripTest.java`, `IcebergLoopRunner.java`
and `SchemaBenchmark.java`. No em dashes found.

## Summary of disagreements with the article list

1. Duplicates: no dedup query, no statement that topic duplicates arrive as separate rows,
   and the README exactly-once sentence promises more than the article does.
2. Sparse updates: the previous-known-value query (`1 PRECEDING`) is absent.
3. Dialects: the files agree with the article, but no CI validates the claim on Spark or
   Trino.
4. Ordering, Flink configuration and Iceberg layout already match.
5. Benchmarks: byte counts come from one hand-built event each, without the compressed
   sizes, storage, latency or history-length sweep the revision calls for, and without a
   synthetic-data label.

## Found after the audit, by the cross-engine CI

`sql/lag_history.sql` at commit 7eeae21 ended with a comment after the final semicolon.
Read as a whole file, the statement sent to an engine still contains the `;`. DuckDB and
Spark accept that; Trino 483 rejects it with a syntax error. The query itself is portable;
the file was not. The comment now sits in the header, and `ci/run_sql_matrix.py` drops
trailing comment-only lines before sending a file, so the matrix tests the statement.
