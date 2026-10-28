# Decision boundary between the three designs

Computed by `bench/decision_boundary.py` from committed results only; nothing here is a new measurement. The workload is the AWS run's: synthetic full-state events, 1,000,000 entities x 100 events = 100,000,000 events per design, Iceberg on S3, queried with Athena engine version 3 (`results/aws/`).

## Assumptions

- Prices: `infra/prices.json` (AWS Price List, fetched 2026-10-23, us-east-1, on-demand): S3 Standard 0.023 USD per GB-month, Athena 5 USD per TB scanned, Managed Service for Apache Flink 0.11 USD per KPU-hour, MSK storage 0.1 USD per GB-month.
- Units: storage GB = 2^30 bytes (as `results/prices.json` cites from the S3 FAQ); Athena's per-query cost is the committed `cost_per_query_usd`, which already applies Athena's rounding to the MB and 10 MB minimum.
- Scale: the AWS run's 100,000,000 events per design. Costs at other scales are assumed to grow with the bytes; that is not measured.
- Storage: each table's current-snapshot data bytes after the Flink commits (`results/aws/iceberg.csv`, state after_commits). Files left on S3 by compaction until orphan removal are not counted.
- Reads: each read is billed and timed as one committed Athena run of that query; no result reuse or caching.
- Current-state table maintenance is not measured in the run, so two cases are given: (i) the ingestion pipeline keeps C current at no counted extra cost; (ii) C is rebuilt by one population-wide pass over B per refresh, billed like one q3 read on B.
- Ingestion compute: Managed Flink KPUs times the time from RUNNING to all events committed (`results/aws/flink.csv`), that is, an application sized so that its KPUs track the event rate. MSK broker-hours are the same cluster for both designs and are not counted.
- Kafka storage: all replicas of the zstd topic's log bytes (`results/aws/kafka.csv`), per month of retention. The run records no retention period, so it is a parameter. MSK bills provisioned broker storage, so this difference is a cost only when provisioned storage is sized to the retained log.

## C against B: population-wide current-state reads (q3)

|  | B, newest row per entity from the audit trail (ROW_NUMBER) | C, current-state table |
|---|---|---|
| Athena cost per read, USD | 0.273652... | 0.002717... |
| Athena bytes scanned per read | 60,175,997,153 | 597,471,193 |
| Athena p50 ms | 29,817 | 16,836 |
| DuckDB p50 ms, main benchmark | 60.785 | 5.348 |
| Table data bytes on S3 | 60,197,255,664 | 599,647,573 |

- Saving per population-wide read with C: 0.270934... USD. C's storage: 0.012844... USD per month.
- Case (i), C kept current by the pipeline: C pays for its storage at 0.0474... reads per month, so the first population-wide read each month already does.
- Case (ii), C rebuilt r times per month at 0.273652... USD per rebuild: break-even reads per month = (r x 0.273652... + 0.012844...) / 0.270934... = 1.0100... x r + 0.0474.... Daily rebuilds (r = 30): 30.34... reads per month; hourly (r = 720): 727.27.... C is the cheaper choice once population-wide current-state reads outnumber its rebuilds.
- C answers current state only: q1, q2 and q4 need A or B.

## A against B: embedded previous values against LAG

| Query | Athena USD per read, A | B | A minus B | Athena p50 ms, A | B | DuckDB p50 ms, A | B |
|---|---|---|---|---|---|---|---|
| q1_one_hop_previous_single | 0.002236... | 0.002388... | -0.000152... | 3,067 | 2,769 | 2.037 | 1.357 |
| q3_current_state_all | 0.273871... | 0.273652... | 0.000219... | 30,166 | 29,817 | 59.149 | 60.785 |
| q4_previous_value_all | 0.011644... | 0.007033... | 0.004611... | 5,558 | 6,696 | 7.818 | 10.227 |

q2 (three changes back) has no A row: A holds one hop.

| Fixed cost | A | B | A's extra cost |
|---|---|---|---|
| Iceberg data bytes on S3 | 119,389,089,551 | 60,197,255,664 | 1.267913... USD per month |
| Kafka log bytes, zstd, all replicas | 222,070,896,513 | 194,759,014,542 | 2.543617... USD per month of retention |
| Managed Flink KPU-hours for 100,000,000 events | 5.9957... | 2.8033... | 0.351161... USD per 100,000,000 events ingested |

- Cost boundary: A is cheaper than B for a month when 0.000152... x n_q1 - 0.000219... x n_q3 - 0.004611... x n_q4 > F, where n_q are that month's reads and F is A's extra fixed cost: 1.267913... USD of storage, plus 2.543617... USD per month of Kafka retention, plus 0.351161... USD per 100,000,000 events ingested that month.
- With single-entity previous-value reads only (n_q3 = n_q4 = 0): storage alone needs 8309.4... q1 reads per month; with a month of Kafka retention as well, 24979.2...; and with one month's ingestion of 100,000,000 new events added, 27280.6....
- Each population-wide previous-value read (q4) costs more on A than on B, so each one raises the bar by 30.21... q1 reads; each q3 read by 1.43....
- Latency boundary: A is faster than B on the population-wide previous-value read q4 (Athena p50 B/A 1.20..., DuckDB 1.30...) and slower on the single-entity read q1 (Athena B/A 0.90..., DuckDB 0.66...). On q3 the two are within 1.1...% on Athena.

Sources: `results/aws/athena.csv`, `results/aws/iceberg.csv`, `results/aws/flink.csv`, `results/aws/kafka.csv`, `results/latency.csv`, `infra/prices.json`.
