# Benchmark summary

Synthetic data at demonstration scale. Every number below was measured by `bench/benchmark.py` from `bench/config.json`; regenerate everything with `make bench-full`.

`make bench-full` reproduces field counts, payload sizes and storage sizes exactly; latencies vary between runs; the ratios between designs are the stable result.

## Environment

| Item | Value |
|---|---|
| Machine | Apple M3, 8 logical CPUs, 16.0 GiB memory |
| OS | macOS-26.6.2-arm64-arm-64bit |
| Python | 3.9.6 |
| DuckDB | 1.4.5, threads=8 |
| fastavro / cramjam | 1.12.2 / 2.11.0 |
| Git commit | 8bd6e743949b72bd93fc46b2d3f580bd3de2dce5 |
| Started (UTC) | 2026-10-20T15:07:05Z |
| Wall time | 150.5 s |

## Parameters

| Parameter | Value |
|---|---|
| Seed | 20261113 |
| N entities | 1000 |
| H events per entity | 100 |
| N x H rows | 100000 |
| K attributes | 67 (email ... marital_status) |
| Event models | full, sparse |
| Attributes changed per event | uniform 1 to 5; the first event sets all K |
| Query runs | 20 timed after 3 warm-up runs |
| Scaling sweep | N=100, H in [1, 10, 100, 1000], 10 timed runs after 2 warm-up |
| Parquet | compression snappy, zstd; row groups of 10000 rows; sorted by customer_id, event_timestamp, event_sequence |
| Deflate level | 6 |

## Designs

- A, previous-value schema: every attribute x has a companion previous_x in the event and the table.
- B, audit trail: attributes only; previous values are reconstructed with window functions.
- C, materialized current-state table: one row per entity holding the latest values. C consumes the same change events as B, so its wire payload is B's and is not listed separately.

## Schema

| Design | Fields | Attribute fields | Schema bytes (pretty .avsc) | Schema bytes (Avro canonical form) |
|---|---|---|---|---|
| A previous-value schema | 137 | 134 | 17,450 | 7,352 |
| B audit trail | 70 | 67 | 8,609 | 3,469 |
| C current-state table | 70 | 67 | 8,611 | 3,471 |

Field count includes the three key fields (customer_id, event_timestamp, event_sequence). The canonical form is Avro's Parsing Canonical Form, which drops defaults, docs and whitespace. Design C's schema describes the table row, which has the same shape as design B's event.

## Payload: Avro binary bytes per event

Avro binary encoding of each event with fastavro's schemaless writer: datum bytes only. Excluded: Confluent wire-format framing (1 magic byte plus 4-byte schema id), Avro object container headers and sync markers, Kafka record headers. Compression is applied to each event on its own: deflate is raw DEFLATE (RFC 1951, zlib level 6, the same stream Avro's deflate codec writes); snappy is the raw snappy block format with no framing. Kafka producers compress whole record batches, so a batch of similar events compresses further than these per-event numbers. Percentiles are nearest-rank over all N x H events.

### FULL model

| Design | Compression | Events | Mean bytes | Median bytes | p95 bytes | Min | Max | Total bytes |
|---|---|---|---|---|---|---|---|---|
| A previous-value schema | none | 100,000 | 1898.38 | 1906 | 2001 | 948 | 2110 | 189,837,837 |
| A previous-value schema | deflate | 100,000 | 789.62 | 790 | 830 | 658 | 883 | 78,962,005 |
| A previous-value schema | snappy | 100,000 | 1185.97 | 1184 | 1256 | 951 | 2036 | 118,596,992 |
| B audit trail | none | 100,000 | 961.76 | 962 | 1010 | 838 | 1068 | 96,176,230 |
| B audit trail | deflate | 100,000 | 684.32 | 684 | 715 | 610 | 753 | 68,432,461 |
| B audit trail | snappy | 100,000 | 966.65 | 966 | 1014 | 843 | 1073 | 96,665,340 |

### SPARSE model

| Design | Compression | Events | Mean bytes | Median bytes | p95 bytes | Min | Max | Total bytes |
|---|---|---|---|---|---|---|---|---|
| A previous-value schema | none | 100,000 | 237.0 | 228 | 302 | 152 | 1109 | 23,700,328 |
| A previous-value schema | deflate | 100,000 | 113.77 | 110 | 175 | 22 | 764 | 11,377,041 |
| A previous-value schema | snappy | 100,000 | 134.18 | 125 | 211 | 30 | 1114 | 13,418,027 |
| B audit trail | none | 100,000 | 131.08 | 122 | 160 | 84 | 1042 | 13,107,667 |
| B audit trail | deflate | 100,000 | 73.01 | 66 | 108 | 20 | 733 | 7,301,114 |
| B audit trail | snappy | 100,000 | 88.43 | 79 | 127 | 24 | 1047 | 8,843,371 |

## Storage: Parquet on disk, full N x H dataset

Written by DuckDB's Parquet writer from the same rows, sorted by customer_id, event_timestamp, event_sequence, with the row group size above. Iceberg's own writer uses different defaults (zstd, 128 MB row groups), so absolute sizes differ there; the ratios between designs are what this table shows. Design C holds N rows whatever H is.

### FULL model

| Design | Compression | Rows | Row groups | Parquet bytes | Bytes per row |
|---|---|---|---|---|---|
| A previous-value schema | snappy | 100,000 | 10 | 16,163,283 | 161.63 |
| B audit trail | snappy | 100,000 | 10 | 8,299,637 | 83.0 |
| C current-state table | snappy | 1,000 | 1 | 990,482 | 990.48 |
| A previous-value schema | zstd | 100,000 | 10 | 11,142,258 | 111.42 |
| B audit trail | zstd | 100,000 | 10 | 5,642,494 | 56.42 |
| C current-state table | zstd | 1,000 | 1 | 625,076 | 625.08 |

### SPARSE model

| Design | Compression | Rows | Row groups | Parquet bytes | Bytes per row |
|---|---|---|---|---|---|
| A previous-value schema | snappy | 100,000 | 10 | 13,565,699 | 135.66 |
| B audit trail | snappy | 100,000 | 10 | 7,593,772 | 75.94 |
| C current-state table | snappy | 1,000 | 1 | 990,205 | 990.21 |
| A previous-value schema | zstd | 100,000 | 10 | 9,163,027 | 91.63 |
| B audit trail | zstd | 100,000 | 10 | 5,027,397 | 50.27 |
| C current-state table | zstd | 1,000 | 1 | 624,884 | 624.88 |

## Query latency in DuckDB

Each query reads the snappy Parquet file through read_parquet. Latency is the wall time of `CREATE OR REPLACE TEMP TABLE ... AS <query>`, so the result is materialized inside DuckDB and Python object creation is excluded. Percentiles are nearest-rank over the timed runs. Scan rows out and scan ms come from DuckDB's JSON profiler on one extra run: the TABLE_SCAN operators' cardinality (rows emitted after pushed-down filters and row-group pruning) and their operator time. Bytes read are not available: DuckDB 1.4.5 reports total_bytes_read as 0 for read_parquet, and its cumulative_rows_scanned for a Parquet scan is the file's row count once per scanning thread, so neither reflects I/O; the raw figure is kept in latency.csv as profiler_rows_scanned. The EXPLAIN ANALYZE text for every query is under results/explain/. The single entity is C0000500. "not supported" marks a question the design cannot answer from its own data.

### FULL model

| Query | Design | p50 ms | p95 ms | p99 ms | Rows returned | Scan rows out | Scan ms |
|---|---|---|---|---|---|---|---|
| q1_one_hop_previous_single | A | 2.037 | 2.142 | 2.159 | 100 | 100 | 1.165 |
| q1_one_hop_previous_single | B | 1.357 | 1.437 | 2.569 | 100 | 100 | 1.039 |
| q1_one_hop_previous_single | C | not supported: holds no history |  |  |  |  |  |
| q2_three_changes_back_single | A | not supported: holds one hop only |  |  |  |  |  |
| q2_three_changes_back_single | B | 1.321 | 1.416 | 1.499 | 100 | 100 | 0.823 |
| q2_three_changes_back_single | C | not supported: holds no history |  |  |  |  |  |
| q3_current_state_all | A | 59.149 | 75.704 | 79.017 | 1,000 | 100,000 | 33.392 |
| q3_current_state_all | B | 60.785 | 78.221 | 78.885 | 1,000 | 100,000 | 37.727 |
| q3_current_state_all | C | 5.348 | 5.606 | 5.657 | 1,000 | 1,000 | 1.329 |
| q4_previous_value_all | A | 7.818 | 8.388 | 8.805 | 1,000 | 100,000 | 2.516 |
| q4_previous_value_all | B | 10.227 | 10.682 | 10.903 | 1,000 | 100,000 | 1.709 |
| q4_previous_value_all | C | not supported: holds no history |  |  |  |  |  |

- q1_one_hop_previous_single: one-hop previous value of email, single entity
- q2_three_changes_back_single: value of email three changes back, single entity
- q3_current_state_all: current state, all 67 attributes, all entities
- q4_previous_value_all: current and previous value of email, all entities

### SPARSE model

| Query | Design | p50 ms | p95 ms | p99 ms | Rows returned | Scan rows out | Scan ms |
|---|---|---|---|---|---|---|---|
| q1_one_hop_previous_single | A | 2.028 | 2.341 | 2.458 | 5 | 5 | 1.238 |
| q1_one_hop_previous_single | B | 1.33 | 1.505 | 1.507 | 5 | 5 | 0.911 |
| q1_one_hop_previous_single | C | not supported: holds no history |  |  |  |  |  |
| q2_three_changes_back_single | A | not supported: holds one hop only |  |  |  |  |  |
| q2_three_changes_back_single | B | 1.317 | 1.513 | 1.591 | 5 | 5 | 1.02 |
| q2_three_changes_back_single | C | not supported: holds no history |  |  |  |  |  |
| q3_current_state_all | A | 192.369 | 205.121 | 206.37 | 1,000 | 100,000 | 43.954 |
| q3_current_state_all | B | 190.245 | 197.528 | 204.078 | 1,000 | 100,000 | 38.867 |
| q3_current_state_all | C | 5.366 | 5.828 | 5.905 | 1,000 | 1,000 | 1.315 |
| q4_previous_value_all | A | 4.085 | 4.304 | 4.316 | 1,000 | 5,362 | 2.485 |
| q4_previous_value_all | B | 3.917 | 4.308 | 4.387 | 1,000 | 5,362 | 2.035 |
| q4_previous_value_all | C | not supported: holds no history |  |  |  |  |  |
| q5_last_and_previous_known_single | A | 13.818 | 14.152 | 17.854 | 100 | 100 | 9.94 |
| q5_last_and_previous_known_single | B | 10.131 | 10.491 | 11.246 | 100 | 100 | 4.36 |
| q5_last_and_previous_known_single | C | not supported: holds the last known value only |  |  |  |  |  |
| q5_last_and_previous_known_all | A | 843.703 | 934.993 | 972.588 | 100,000 | 100,000 | 63.463 |
| q5_last_and_previous_known_all | B | 764.801 | 806.912 | 861.408 | 100,000 | 100,000 | 32.404 |
| q5_last_and_previous_known_all | C | not supported: holds the last known value only |  |  |  |  |  |

- q1_one_hop_previous_single: one-hop previous value of email, single entity
- q2_three_changes_back_single: value of email three changes back, single entity
- q3_current_state_all: current state, all 67 attributes, all entities
- q4_previous_value_all: current and previous value of email, all entities
- q5_last_and_previous_known_single: last-known and previous-known value of all 67 attributes, single entity
- q5_last_and_previous_known_all: last-known and previous-known value of all 67 attributes, all entities

## Scaling with history length H

N=100 entities, H swept over [1, 10, 100, 1000], full, sparse model(s). Storage is the snappy Parquet file. Latency is p50 / p95 in ms over 10 timed runs. This answers whether query cost grows with the length of a customer's history.

### FULL model: storage

| Design | H=1 | H=10 | H=100 | H=1000 |
|---|---|---|---|---|
| A previous-value schema | 119,821 (100 rows) | 404,111 (1,000 rows) | 1,622,893 (10,000 rows) | 14,044,850 (100,000 rows) |
| B audit trail | 112,546 (100 rows) | 206,017 (1,000 rows) | 833,133 (10,000 rows) | 7,320,426 (100,000 rows) |
| C current-state table | 112,546 (100 rows) | 112,161 (100 rows) | 111,921 (100 rows) | 111,405 (100 rows) |

### FULL model: latency p50 / p95 ms

| Query | Design | H=1 | H=10 | H=100 | H=1000 |
|---|---|---|---|---|---|
| q1_one_hop_previous_single | A | 0.884 / 0.983 | 1.017 / 1.389 | 1.132 / 1.431 | 2.217 / 2.509 |
| q1_one_hop_previous_single | B | 0.692 / 0.746 | 0.751 / 0.802 | 0.887 / 1.076 | 1.535 / 1.777 |
| q1_one_hop_previous_single | C | not supported: holds no history |  |  |  |
| q2_three_changes_back_single | A | not supported: holds one hop only |  |  |  |
| q2_three_changes_back_single | B | 0.665 / 0.705 | 0.74 / 0.761 | 0.832 / 1.01 | 1.533 / 1.703 |
| q2_three_changes_back_single | C | not supported: holds no history |  |  |  |
| q3_current_state_all | A | 5.81 / 6.076 | 6.651 / 7.159 | 16.172 / 19.13 | 58.469 / 67.034 |
| q3_current_state_all | B | 5.294 / 5.511 | 6.322 / 6.43 | 15.916 / 34.557 | 54.866 / 64.542 |
| q3_current_state_all | C | 1.537 / 1.62 | 1.539 / 1.711 | 1.896 / 2.293 | 1.635 / 1.982 |
| q4_previous_value_all | A | 1.511 / 1.568 | 1.682 / 1.891 | 3.173 / 3.539 | 8.38 / 26.677 |
| q4_previous_value_all | B | 1.635 / 3.981 | 2.056 / 2.339 | 3.162 / 4.072 | 9.585 / 10.283 |
| q4_previous_value_all | C | not supported: holds no history |  |  |  |

### FULL model: scan rows out / scan ms

| Query | Design | H=1 | H=10 | H=100 | H=1000 |
|---|---|---|---|---|---|
| q1_one_hop_previous_single | A | 1 / 0.019 | 10 / 0.085 | 100 / 0.158 | 1,000 / 1.195 |
| q1_one_hop_previous_single | B | 1 / 0.015 | 10 / 0.066 | 100 / 0.126 | 1,000 / 0.746 |
| q2_three_changes_back_single | B | 1 / 0.016 | 10 / 0.064 | 100 / 0.128 | 1,000 / 0.555 |
| q3_current_state_all | A | 100 / 0.275 | 1,000 / 0.575 | 10,000 / 1.827 | 100,000 / 32.97 |
| q3_current_state_all | B | 100 / 0.27 | 1,000 / 0.582 | 10,000 / 1.774 | 100,000 / 33.141 |
| q3_current_state_all | C | 100 / 0.241 | 100 / 0.237 | 100 / 0.293 | 100 / 0.245 |
| q4_previous_value_all | A | 100 / 0.018 | 1,000 / 0.037 | 10,000 / 0.141 | 100,000 / 2.297 |
| q4_previous_value_all | B | 100 / 0.015 | 1,000 / 0.027 | 10,000 / 0.097 | 100,000 / 2.687 |

### SPARSE model: storage

| Design | H=1 | H=10 | H=100 | H=1000 |
|---|---|---|---|---|
| A previous-value schema | 119,647 (100 rows) | 258,459 (1,000 rows) | 1,348,931 (10,000 rows) | 12,761,073 (100,000 rows) |
| B audit trail | 112,372 (100 rows) | 189,829 (1,000 rows) | 754,963 (10,000 rows) | 6,741,533 (100,000 rows) |
| C current-state table | 112,372 (100 rows) | 111,837 (100 rows) | 112,633 (100 rows) | 113,686 (100 rows) |

### SPARSE model: latency p50 / p95 ms

| Query | Design | H=1 | H=10 | H=100 | H=1000 |
|---|---|---|---|---|---|
| q1_one_hop_previous_single | A | 0.957 / 1.083 | 1.006 / 1.161 | 1.076 / 1.276 | 2.037 / 2.413 |
| q1_one_hop_previous_single | B | 0.707 / 0.982 | 0.776 / 1.027 | 0.844 / 0.94 | 1.38 / 1.545 |
| q1_one_hop_previous_single | C | not supported: holds no history |  |  |  |
| q2_three_changes_back_single | A | not supported: holds one hop only |  |  |  |
| q2_three_changes_back_single | B | 0.686 / 0.815 | 0.727 / 0.796 | 0.844 / 0.98 | 1.379 / 1.417 |
| q2_three_changes_back_single | C | not supported: holds no history |  |  |  |
| q3_current_state_all | A | 12.671 / 13.813 | 15.022 / 15.539 | 34.188 / 35.217 | 194.094 / 336.075 |
| q3_current_state_all | B | 12.45 / 12.819 | 14.479 / 14.871 | 33.887 / 35.739 | 185.223 / 218.283 |
| q3_current_state_all | C | 1.639 / 1.967 | 1.643 / 1.863 | 1.61 / 1.805 | 1.563 / 1.744 |
| q4_previous_value_all | A | 1.557 / 1.836 | 1.649 / 1.869 | 1.83 / 2.042 | 3.432 / 3.572 |
| q4_previous_value_all | B | 1.678 / 2.09 | 1.841 / 2.293 | 1.977 / 2.412 | 3.167 / 4.379 |
| q4_previous_value_all | C | not supported: holds no history |  |  |  |
| q5_last_and_previous_known_single | A | 6.468 / 6.883 | 8.779 / 9.214 | 12.028 / 12.159 | 25.198 / 28.833 |
| q5_last_and_previous_known_single | B | 5.59 / 5.982 | 7.064 / 7.681 | 9.185 / 20.76 | 19.676 / 19.857 |
| q5_last_and_previous_known_single | C | not supported: holds the last known value only |  |  |  |
| q5_last_and_previous_known_all | A | 14.859 / 16.42 | 22.002 / 22.405 | 101.763 / 116.724 | 767.317 / 862.814 |
| q5_last_and_previous_known_all | B | 12.617 / 12.894 | 20.076 / 20.677 | 92.338 / 113.922 | 725.678 / 846.069 |
| q5_last_and_previous_known_all | C | not supported: holds the last known value only |  |  |  |

### SPARSE model: scan rows out / scan ms

| Query | Design | H=1 | H=10 | H=100 | H=1000 |
|---|---|---|---|---|---|
| q1_one_hop_previous_single | A | 1 / 0.02 | 2 / 0.079 | 3 / 0.152 | 49 / 1.263 |
| q1_one_hop_previous_single | B | 1 / 0.017 | 2 / 0.066 | 3 / 0.125 | 49 / 0.617 |
| q2_three_changes_back_single | B | 1 / 0.017 | 2 / 0.071 | 3 / 0.146 | 49 / 1.196 |
| q3_current_state_all | A | 100 / 0.298 | 1,000 / 0.58 | 10,000 / 1.816 | 100,000 / 36.83 |
| q3_current_state_all | B | 100 / 0.292 | 1,000 / 0.564 | 10,000 / 2.668 | 100,000 / 31.032 |
| q3_current_state_all | C | 100 / 0.252 | 100 / 0.236 | 100 / 0.244 | 100 / 0.235 |
| q4_previous_value_all | A | 100 / 0.018 | 154 / 0.037 | 576 / 0.132 | 4,657 / 2.412 |
| q4_previous_value_all | B | 100 / 0.017 | 154 / 0.031 | 576 / 0.094 | 4,657 / 2.082 |
| q5_last_and_previous_known_single | A | 1 / 0.431 | 10 / 2.365 | 100 / 4.165 | 1,000 / 9.98 |
| q5_last_and_previous_known_single | B | 1 / 0.235 | 10 / 1.167 | 100 / 2.287 | 1,000 / 4.842 |
| q5_last_and_previous_known_all | A | 100 / 0.489 | 1,000 / 1.02 | 10,000 / 3.946 | 100,000 / 75.875 |
| q5_last_and_previous_known_all | B | 100 / 0.291 | 1,000 / 0.692 | 10,000 / 1.857 | 100,000 / 33.316 |

## Equivalence of the three designs

Before timing, each dataset was checked so that the designs agree wherever they answer the same question: the row sets below were compared after sorting. A difference would have aborted the run.

| Dataset | Query | Designs | Columns compared | Rows compared | Equal |
|---|---|---|---|---|---|
| main_full | q1_one_hop_previous_single | A vs B | all | 100 | yes |
| main_full | q3_current_state_all | A vs B | all | 1,000 | yes |
| main_full | q3_current_state_all | B vs C | all | 1,000 | yes |
| main_full | q4_previous_value_all | A vs B | all | 1,000 | yes |
| main_sparse | q1_one_hop_previous_single | A vs B | all | 5 | yes |
| main_sparse | q3_current_state_all | A vs B | all | 1,000 | yes |
| main_sparse | q3_current_state_all | B vs C | all | 1,000 | yes |
| main_sparse | q4_previous_value_all | A vs B | all | 1,000 | yes |
| main_sparse | q5_last_and_previous_known_all | A vs B | current_* only | 100,000 | yes |
| sweep_full_h1 | q1_one_hop_previous_single | A vs B | all | 1 | yes |
| sweep_full_h1 | q3_current_state_all | A vs B | all | 100 | yes |
| sweep_full_h1 | q3_current_state_all | B vs C | all | 100 | yes |
| sweep_full_h1 | q4_previous_value_all | A vs B | all | 100 | yes |
| sweep_full_h10 | q1_one_hop_previous_single | A vs B | all | 10 | yes |
| sweep_full_h10 | q3_current_state_all | A vs B | all | 100 | yes |
| sweep_full_h10 | q3_current_state_all | B vs C | all | 100 | yes |
| sweep_full_h10 | q4_previous_value_all | A vs B | all | 100 | yes |
| sweep_full_h100 | q1_one_hop_previous_single | A vs B | all | 100 | yes |
| sweep_full_h100 | q3_current_state_all | A vs B | all | 100 | yes |
| sweep_full_h100 | q3_current_state_all | B vs C | all | 100 | yes |
| sweep_full_h100 | q4_previous_value_all | A vs B | all | 100 | yes |
| sweep_full_h1000 | q1_one_hop_previous_single | A vs B | all | 1,000 | yes |
| sweep_full_h1000 | q3_current_state_all | A vs B | all | 100 | yes |
| sweep_full_h1000 | q3_current_state_all | B vs C | all | 100 | yes |
| sweep_full_h1000 | q4_previous_value_all | A vs B | all | 100 | yes |
| sweep_sparse_h1 | q1_one_hop_previous_single | A vs B | all | 1 | yes |
| sweep_sparse_h1 | q3_current_state_all | A vs B | all | 100 | yes |
| sweep_sparse_h1 | q3_current_state_all | B vs C | all | 100 | yes |
| sweep_sparse_h1 | q4_previous_value_all | A vs B | all | 100 | yes |
| sweep_sparse_h1 | q5_last_and_previous_known_all | A vs B | current_* only | 100 | yes |
| sweep_sparse_h10 | q1_one_hop_previous_single | A vs B | all | 2 | yes |
| sweep_sparse_h10 | q3_current_state_all | A vs B | all | 100 | yes |
| sweep_sparse_h10 | q3_current_state_all | B vs C | all | 100 | yes |
| sweep_sparse_h10 | q4_previous_value_all | A vs B | all | 100 | yes |
| sweep_sparse_h10 | q5_last_and_previous_known_all | A vs B | current_* only | 1,000 | yes |
| sweep_sparse_h100 | q1_one_hop_previous_single | A vs B | all | 3 | yes |
| sweep_sparse_h100 | q3_current_state_all | A vs B | all | 100 | yes |
| sweep_sparse_h100 | q3_current_state_all | B vs C | all | 100 | yes |
| sweep_sparse_h100 | q4_previous_value_all | A vs B | all | 100 | yes |
| sweep_sparse_h100 | q5_last_and_previous_known_all | A vs B | current_* only | 10,000 | yes |
| sweep_sparse_h1000 | q1_one_hop_previous_single | A vs B | all | 49 | yes |
| sweep_sparse_h1000 | q3_current_state_all | A vs B | all | 100 | yes |
| sweep_sparse_h1000 | q3_current_state_all | B vs C | all | 100 | yes |
| sweep_sparse_h1000 | q4_previous_value_all | A vs B | all | 100 | yes |
| sweep_sparse_h1000 | q5_last_and_previous_known_all | A vs B | current_* only | 100,000 | yes |

## Files

- `schema.csv`, `schema_A.avsc`, `schema_B.avsc`, `schema_C.avsc`
- `payload.csv`, `storage.csv`, `latency.csv`, `equivalence.csv`
- `scaling_storage.csv`, `scaling_latency.csv`
- `explain/` EXPLAIN ANALYZE text per dataset, query and design
- `environment.json` machine, versions, configuration, wall time
