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

## Resource use and cost

Measured by `bench/resources.py` from `bench/config.json` and `bench/prices.json` on 2026-10-20T19:29:04Z, on the same datasets as the tables above (seed 20261113, N=1000, H=100, K=67): the script regenerates them and checks every Parquet size against storage.csv. Machine: Apple M3, 8 logical CPUs. DuckDB 1.4.5, threads=8. Wall time 193.0 s. CPU time and peak memory vary between runs; bytes scanned and cost reproduce exactly.

- CPU ms: DuckDB profiler metric `cpu_time`, the sum of `operator_timing` over every operator and thread, times 1000. It excludes parsing and planning and is not operating-system process CPU time. Values are kept exactly as DuckDB writes them to the profiler JSON; long tails such as 0.08604200000000001 come from DuckDB's floating-point summation.
- Peak memory MB: profiler metric `system_peak_buffer_memory`, the database instance's buffer pool usage sampled after each operator call, maximum within the query, in units of 1,000,000 bytes.
- Spill MB: profiler metric `system_peak_temp_dir_size`, temp-directory usage sampled the same way, in units of 1,000,000 bytes.
- In DuckDB 1.4.5 these two peaks are running maxima across profiled queries on one connection (QueryProfiler::Reset does not clear them), so each of the 20 timed runs uses a fresh in-memory database, executes the query for 3 unprofiled warm-up runs, drops the result, and profiles the next execution. Values are the median of the 20 runs (the mean of the two middle values); resources.csv also holds min and max.
- Bytes scanned: from `parquet_metadata()` of the snappy file each query reads. Columns are the TABLE_SCAN operator's `Projections` plus any column named in its pushed-down `Filters`. Row groups are those whose min/max statistics (or null counts, for IS NOT NULL) do not exclude every row under those filters. The value is the sum of `total_compressed_size` over those column chunks. Footer and page-index reads are excluded. DuckDB 1.4.5 reports neither bytes read nor pruned row groups for Parquet, so this is derived, not observed.
- Row groups as written, read from `parquet_metadata()`: with ROW_GROUP_SIZE 10000, DuckDB 1.4.5 wrote FULL model: design A 9 x 10240 + 1 x 7840 rows, design B 9 x 10240 + 1 x 7840 rows, design C 1 x 1000 rows; SPARSE model: design A 9 x 10240 + 1 x 7840 rows, design B 9 x 10240 + 1 x 7840 rows, design C 1 x 1000 rows. The Parameters table above gives the setting, not these counts.

### FULL model: resources

| Query | Design | CPU ms | Peak memory MB | Spill MB | Bytes scanned | Columns read | Row groups read |
|---|---|---|---|---|---|---|---|
| q1_one_hop_previous_single | A | 1.2774775000000001 | 2.927104 | 0 | 68,460 | 5 of 137 | 1 of 10 |
| q1_one_hop_previous_single | B | 0.757563 | 2.213888 | 0 | 56,100 | 4 of 70 | 1 of 10 |
| q1_one_hop_previous_single | C | not supported: holds no history |  |  |  |  |  |
| q2_three_changes_back_single | A | not supported: holds one hop only |  |  |  |  |  |
| q2_three_changes_back_single | B | 0.71947750000000005 | 2.213888 | 0 | 56,100 | 4 of 70 | 1 of 10 |
| q2_three_changes_back_single | C | not supported: holds no history |  |  |  |  |  |
| q3_current_state_all | A | 248.109209499999965 | 304.012352 | 0 | 7,590,968 | 70 of 137 | 10 of 10 |
| q3_current_state_all | B | 248.686181000000015 | 300.807872 | 0 | 7,590,968 | 70 of 70 | 10 of 10 |
| q3_current_state_all | C | 2.7382085 | 8.556032 | 0 | 977,485 | 68 of 70 | 1 of 1 |
| q4_previous_value_all | A | 20.703182 | 123.205952 | 0 | 667,145 | 5 of 137 | 10 of 10 |
| q4_previous_value_all | B | 35.640286499999998 | 122.080448 | 0 | 546,737 | 4 of 70 | 10 of 10 |
| q4_previous_value_all | C | not supported: holds no history |  |  |  |  |  |

### SPARSE model: resources

| Query | Design | CPU ms | Peak memory MB | Spill MB | Bytes scanned | Columns read | Row groups read |
|---|---|---|---|---|---|---|---|
| q1_one_hop_previous_single | A | 1.2793749999999999 | 2.905088 | 0 | 65,336 | 5 of 137 | 1 of 10 |
| q1_one_hop_previous_single | B | 1.0642324999999999 | 2.213888 | 0 | 55,559 | 4 of 70 | 1 of 10 |
| q1_one_hop_previous_single | C | not supported: holds no history |  |  |  |  |  |
| q2_three_changes_back_single | A | not supported: holds one hop only |  |  |  |  |  |
| q2_three_changes_back_single | B | 1.04625149999999975 | 2.197504 | 0 | 55,559 | 4 of 70 | 1 of 10 |
| q2_three_changes_back_single | C | not supported: holds no history |  |  |  |  |  |
| q3_current_state_all | A | 1056.3640775 | 532.074496 | 0 | 6,883,681 | 70 of 137 | 10 of 10 |
| q3_current_state_all | B | 1110.318538 | 531.03616 | 0 | 6,883,681 | 70 of 70 | 10 of 10 |
| q3_current_state_all | C | 2.7548755 | 8.556032 | 0 | 977,098 | 68 of 70 | 1 of 1 |
| q4_previous_value_all | A | 7.2736495 | 108.971776 | 0 | 620,911 | 5 of 137 | 10 of 10 |
| q4_previous_value_all | B | 9.0979165 | 104.94208 | 0 | 533,310 | 4 of 70 | 10 of 10 |
| q4_previous_value_all | C | not supported: holds no history |  |  |  |  |  |
| q5_last_and_previous_known_single | A | 9.964895500000001 | 63.866368 | 0 | 1,248,740 | 137 of 137 | 1 of 10 |
| q5_last_and_previous_known_single | B | 5.86947999999999975 | 33.35168 | 0 | 705,374 | 70 of 70 | 1 of 10 |
| q5_last_and_previous_known_single | C | not supported: holds the last known value only |  |  |  |  |  |
| q5_last_and_previous_known_all | A | 1511.3069134999998 | 750.862336 | 0 | 12,180,853 | 137 of 137 | 10 of 10 |
| q5_last_and_previous_known_all | B | 1286.6908769999998 | 645.15328 | 0 | 6,883,681 | 70 of 70 | 10 of 10 |
| q5_last_and_previous_known_all | C | not supported: holds the last known value only |  |  |  |  |  |

### Prices

| Item | Price | Region | Page | Read on |
|---|---|---|---|---|
| S3 Standard storage | 0.023 USD per GB-month (First 50 TB / Month); 1 GB = 1,073,741,824 bytes per the S3 FAQ | US East (N. Virginia), us-east-1 | https://aws.amazon.com/s3/pricing/ | 2026-10-20 |
| Athena SQL queries | 5.00 USD per TB scanned; rounded up to the nearest MB; 10 MB minimum per query | US East (N. Virginia), us-east-1 | https://aws.amazon.com/athena/pricing/ | 2026-10-20 |

The Athena pricing page and the Athena FAQ do not define TB or MB in bytes. Binary units are applied (TB = 2^40 bytes, MB = 2^20 bytes), matching the 2^30-byte GB in the Amazon S3 FAQ. The tables use TB = 1,099,511,627,776 bytes and MB = 1,048,576 bytes.

### Storage cost: S3 Standard

Parquet bytes from storage.csv times the price. Per million events scales the dataset's 100,000 events linearly; design C's size follows the number of entities, so its per-event figure holds only for 100 events per entity.

| Model | Design | Compression | Parquet bytes | USD per month, dataset | USD per million events per month |
|---|---|---|---|---|---|
| full | A | snappy | 16,163,283 | 0.000346224297769367694854736328125 | 0.00346224297769367694854736328125 |
| full | B | snappy | 8,299,637 | 0.000177781703881919384002685546875 | 0.00177781703881919384002685546875 |
| full | C | snappy | 990,482 | 0.00002121653966605663299560546875 | 0.0002121653966605663299560546875 |
| full | A | zstd | 11,142,258 | 0.00023867183737456798553466796875 | 0.0023867183737456798553466796875 |
| full | B | zstd | 5,642,494 | 0.00012086458690464496612548828125 | 0.0012086458690464496612548828125 |
| full | C | zstd | 625,076 | 0.0000133893899619579315185546875 | 0.000133893899619579315185546875 |
| sparse | A | snappy | 13,565,699 | 0.000290582959540188312530517578125 | 0.00290582959540188312530517578125 |
| sparse | B | snappy | 7,593,772 | 0.0001626617796719074249267578125 | 0.001626617796719074249267578125 |
| sparse | C | snappy | 990,205 | 0.000021210606209933757781982421875 | 0.00021210606209933757781982421875 |
| sparse | A | zstd | 9,163,027 | 0.000196275879628956317901611328125 | 0.00196275879628956317901611328125 |
| sparse | B | zstd | 5,027,397 | 0.000107688951306045055389404296875 | 0.00107688951306045055389404296875 |
| sparse | C | zstd | 624,884 | 0.0000133852772414684295654296875 | 0.000133852772414684295654296875 |

### Query cost: Athena

Bytes scanned times the price, per 1,000 runs of the query. "With minimum" bills each query at max(10 MB, bytes scanned rounded up to the nearest MB).

| Model | Query | Design | Bytes scanned | Billed bytes with minimum | USD per 1,000 queries, per-TB rate | USD per 1,000 queries, with minimum |
|---|---|---|---|---|---|---|
| full | q1_one_hop_previous_single | A | 68,460 | 10,485,760 | 0.00031132003641687333583831787109375 | 0.0476837158203125 |
| full | q1_one_hop_previous_single | B | 56,100 | 10,485,760 | 0.00025511326384730637073516845703125 | 0.0476837158203125 |
| full | q2_three_changes_back_single | B | 56,100 | 10,485,760 | 0.00025511326384730637073516845703125 | 0.0476837158203125 |
| full | q3_current_state_all | A | 7,590,968 | 10,485,760 | 0.0345197258866392076015472412109375 | 0.0476837158203125 |
| full | q3_current_state_all | B | 7,590,968 | 10,485,760 | 0.0345197258866392076015472412109375 | 0.0476837158203125 |
| full | q3_current_state_all | C | 977,485 | 10,485,760 | 0.0044450871428125537931919097900390625 | 0.0476837158203125 |
| full | q4_previous_value_all | A | 667,145 | 10,485,760 | 0.0030338242140715010464191436767578125 | 0.0476837158203125 |
| full | q4_previous_value_all | B | 546,737 | 10,485,760 | 0.0024862720238161273300647735595703125 | 0.0476837158203125 |
| sparse | q1_one_hop_previous_single | A | 65,336 | 10,485,760 | 0.0002971137291751801967620849609375 | 0.0476837158203125 |
| sparse | q1_one_hop_previous_single | B | 55,559 | 10,485,760 | 0.0002526530806790105998516082763671875 | 0.0476837158203125 |
| sparse | q2_three_changes_back_single | B | 55,559 | 10,485,760 | 0.0002526530806790105998516082763671875 | 0.0476837158203125 |
| sparse | q3_current_state_all | A | 6,883,681 | 10,485,760 | 0.0313033569909748621284961700439453125 | 0.0476837158203125 |
| sparse | q3_current_state_all | B | 6,883,681 | 10,485,760 | 0.0313033569909748621284961700439453125 | 0.0476837158203125 |
| sparse | q3_current_state_all | C | 977,098 | 10,485,760 | 0.004443327270564623177051544189453125 | 0.0476837158203125 |
| sparse | q4_previous_value_all | A | 620,911 | 10,485,760 | 0.0028235763238626532256603240966796875 | 0.0476837158203125 |
| sparse | q4_previous_value_all | B | 533,310 | 10,485,760 | 0.002425213097012601792812347412109375 | 0.0476837158203125 |
| sparse | q5_last_and_previous_known_single | A | 1,248,740 | 10,485,760 | 0.00567861206945963203907012939453125 | 0.0476837158203125 |
| sparse | q5_last_and_previous_known_single | B | 705,374 | 10,485,760 | 0.003207669578841887414455413818359375 | 0.0476837158203125 |
| sparse | q5_last_and_previous_known_all | A | 12,180,853 | 12,582,912 | 0.0553921063328743912279605865478515625 | 0.057220458984375 |
| sparse | q5_last_and_previous_known_all | B | 6,883,681 | 10,485,760 | 0.0313033569909748621284961700439453125 | 0.0476837158203125 |

Files: `resources.csv`, `cost.csv`, `prices.json`, `resources_environment.json`.

## Generality sweep over attribute count

Measured by `bench/sweep_attributes.py` from `bench/config.json` on 2026-10-20T19:49:43Z: FULL model (every event carries every attribute), seed 20261113, N=1000, H=100, K in [10, 25, 67, 150]. Machine: Apple M3, 8 logical CPUs. DuckDB 1.4.5, threads=8. Wall time 105.6 s. Field counts, Avro bytes and Parquet bytes reproduce exactly; latencies vary between runs.

- Attributes beyond the 67 in `bench/generate_schemas.py` are string columns named attribute_<i>.
- Avro bytes: fastavro schemaless datum per event, no Confluent framing; deflate is raw DEFLATE at zlib level 6 per event. Mean is the exact mean over all events.
- Parquet bytes: DuckDB writer, ROW_GROUP_SIZE 10000, sorted by customer_id, event_timestamp, event_sequence.
- Latency: p50 (nearest rank) of 20 timed `CREATE OR REPLACE TEMP TABLE ... AS <query>` runs after 3 warm-up runs over the snappy file, the protocol of the latency table above.
- Ratio is A divided by B. A ratio that does not terminate shows its first ten decimals followed by "..."; the digits shown are exact, not rounded.
- At K=67 the Avro totals and Parquet sizes equal the committed payload.csv and storage.csv values (10 checks).

### Schema and payload

| K | Fields A | Fields B | Ratio | Avro mean A | Avro mean B | Ratio | Avro deflate mean A | Avro deflate mean B | Ratio |
|---|---|---|---|---|---|---|---|---|---|
| 10 | 23 | 13 | 1.7692307692... | 334.69852 | 176.28437 | 1.8986284490... | 200.1021 | 155.31714 | 1.2883452528... |
| 25 | 53 | 28 | 1.8928571428... | 812.9553 | 416.5356 | 1.9517066488... | 373.66542 | 313.38097 | 1.1923679347... |
| 67 | 137 | 70 | 1.9571428571... | 1898.37837 | 961.7623 | 1.9738540073... | 789.62005 | 684.32461 | 1.1538676798... |
| 150 | 303 | 153 | 1.9803921568... | 4542.88803 | 2290.24712 | 1.9835798461... | 1758.05445 | 1538.54977 | 1.1426698598... |

Medians, per event: K=10 uncompressed 336 / 176, deflate 200 / 155; K=25 uncompressed 816 / 416, deflate 374 / 314; K=67 uncompressed 1906 / 962, deflate 790 / 684; K=150 uncompressed 4564 / 2291, deflate 1760 / 1539 (A / B).

### Storage

| K | Parquet snappy A | Parquet snappy B | Ratio | Parquet zstd A | Parquet zstd B | Ratio | zstd C |
|---|---|---|---|---|---|---|---|
| 10 | 13,090,706 | 6,788,079 | 1.9284846272... | 7,851,524 | 4,003,824 | 1.9610062779... | 108,634 |
| 25 | 15,628,489 | 8,052,620 | 1.9407955423... | 10,782,703 | 5,469,977 | 1.9712519814... | 268,053 |
| 67 | 16,163,283 | 8,299,637 | 1.9474686664... | 11,142,258 | 5,642,494 | 1.9747044480... | 625,076 |
| 150 | 21,329,802 | 10,850,602 | 1.9657713000... | 14,899,350 | 7,509,705 | 1.9840126875... | 1,509,249 |

### Latency, p50 ms

| K | q1 one hop A | q1 one hop B | Ratio | q3 current state A | q3 current state B | Ratio | q3 current state C |
|---|---|---|---|---|---|---|---|
| 10 | 0.961 | 0.859 | 1.1187427240... | 16.252 | 15.053 | 1.0796518966... | 1.026 |
| 25 | 1.471 | 0.998 | 1.4739478957... | 28.021 | 25.585 | 1.0952120383... | 2.329 |
| 67 | 2.037 | 1.367 | 1.4901243599... | 59.728 | 54.716 | 1.0916002631... | 5.342 |
| 150 | 4.035 | 2.091 | 1.9296987087... | 134.922 | 131.431 | 1.0265614657... | 12.487 |

Files: `sweep_attributes.csv`, `sweep_attributes_environment.json`.

## Engine documentation for LAST_VALUE with IGNORE NULLS

Official pages that define LAST_VALUE with IGNORE NULLS, fetched on 2026-10-20. The syntax line is copied verbatim from each page. Generated by `bench/engine_docs.py`.

| Engine | Page | Documented version | Syntax line on the page | IGNORE NULLS placement |
|---|---|---|---|---|
| DuckDB | https://duckdb.org/docs/lts/sql/functions/window_functions | DuckDB 1.4 (LTS); the page reads "This page is for DuckDB 1.4 (LTS)." | `last_value(expr[ ORDER BY ordering][ IGNORE NULLS])` | inside the parentheses |
| Spark SQL | https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html | Spark 4.2.0; the page title reads "Window Functions - Spark 4.2.0 Documentation" | `window_function [ nulls_option ] OVER` | after the closing parenthesis, before OVER |
| Trino | https://trino.io/docs/current/functions/window.html | Trino 484; the page title ends "Trino 484 Documentation" | `last_value(x) → [same as input]` | not shown on the page |
| Athena | https://docs.aws.amazon.com/athena/latest/ug/engine-versions-reference-0003.html | Athena engine version 3 | Cause: IGNORE NULLS can now be used only with the value functions first_value, last_value, nth_value, lead, and lag. This change was made to conform to the ANSI SQL specification. | not shown on the page |

- DuckDB: Description on the page: "Returns expr evaluated at the row that is the last row (among rows with a non-null value of expr if IGNORE NULLS is set) of the window frame."
- Spark SQL: The page also shows "Note: Only LAG | LEAD | NTH_VALUE | FIRST_VALUE | LAST_VALUE can be used with IGNORE NULLS." and the example line "LAST_VALUE(v) IGNORE NULLS OVER w last_value".
- Trino: The Value functions section states: "By default, null values are respected. If IGNORE NULLS is specified, all rows where x is null are excluded from the calculation." Neither this page nor https://trino.io/docs/current/sql/select.html shows where IGNORE NULLS is written; CI runs the after-the-parenthesis form on Trino 483.
- Athena: No Athena page found that gives a LAST_VALUE syntax line. This page links "value functions" to https://trino.io/docs/current/functions/window.html#value-functions, the Trino 484 page above, and does not state which Trino version engine version 3 follows.

CI (`ci/run_sql_matrix.py`) checks the placements on DuckDB 1.4.5, Spark 4.0.4 and Trino 483, which are the versions it runs, not the documentation versions above.

Files: `engine_docs.csv`.

## Pipeline measurements

Measured by `bench/pipeline.py` from `bench/config.json` on 2026-10-20T23:16:03Z: designs A and B through the Kafka to Flink to Iceberg topology of CustomerProfileAuditJob, seed 20261113, N=1000, H=100 (100,000 events per run), K=67, models full, sparse. Medians over 5 runs. Machine: Apple M3, 8 logical CPUs. Kafka 3 brokers (apache/kafka:3.7.0 in Docker on the same machine), Flink 1.20.3 embedded, Iceberg 1.10.1 on the local filesystem. Wall time 2342.0 s. Synthetic data at demonstration scale.

- Events: written by this script from the benchmark generator, sent in event-time order as Avro binary datums with no Confluent framing, keyed by customer_id, to a fresh topic per run with 3 partitions, replication factor 3 and min.insync.replicas 2. The job deserializes them with the same Avro schema into Iceberg rows.
- Broker storage: the topic's log bytes per replica from the brokers' log directories (AdminClient describeLogDirs), read once every replica matches its leader. One copy is the sum of the leader replicas; all replicas is the sum over brokers. Replication traffic is derived, not measured: one-copy bytes x (replication factor - 1).
- Producer latency: time from each send() call to its acknowledgement callback, acks=all, Kafka client 3.4.0 defaults otherwise; nearest-rank p50, p95 and p99 over all events of a run, then the median over runs. Events are sent back to back with no pacing, so the time includes each record's wait in the producer's buffer behind earlier records; it is latency at the producer's maximum send rate, not for an isolated event.
- Flink: parallelism 3, checkpoint every 60000 ms, minimum pause 30000 ms, exactly-once, noWatermarks, the reference job's settings. Throughput = events / (time the source operator's numRecordsOut, summed over subtasks, first equals the event count - the job's RUNNING timestamp), both from the Flink REST API, polled every 100 ms with the metric fetcher set to 100 ms. Checkpoint duration and size are the REST checkpoint statistics (end_to_end_duration, checkpointed_size) of the checkpoint whose Iceberg commit made every event visible.

### FULL model: Kafka broker storage

| Design | Compression | Log bytes, one copy | Bytes per event, one copy | Log bytes, all replicas | Replication traffic bytes (derived) |
|---|---|---|---|---|---|
| A previous-value schema | none | 192307974 | 1923.07974 | 576923922 | 384615948 |
| A previous-value schema | zstd | 73947561 | 739.47561 | 221842683 | 147895122 |
| B audit trail | none | 98259232 | 982.59232 | 294777696 | 196518464 |
| B audit trail | zstd | 64718917 | 647.18917 | 194156751 | 129437834 |

### FULL model: producer latency, send to acknowledgement, ms

| Design | Compression | p50 | p95 | p99 |
|---|---|---|---|---|
| A previous-value schema | none | 1035.134 | 1957.135541 | 2108.171083 |
| A previous-value schema | zstd | 871.356125 | 1309.548583 | 1372.614 |
| B audit trail | none | 748.521333 | 1441.931875 | 1519.281708 |
| B audit trail | zstd | 738.188708 | 982.152834 | 1051.895791 |

### FULL model: Flink throughput and checkpoints

| Design | Topic compression | Events per second | Checkpoint end-to-end ms | Checkpointed size bytes | Checkpoints completed |
|---|---|---|---|---|---|
| A previous-value schema | none | 18406.0371801951... | 208 | 4428 | 1 |
| A previous-value schema | zstd | 22084.8056537102... | 276 | 4428 | 1 |
| B audit trail | none | 62617.4076393237... | 256 | 4428 | 1 |
| B audit trail | zstd | 50735.6671740233... | 197 | 4428 | 1 |

### SPARSE model: Kafka broker storage

| Design | Compression | Log bytes, one copy | Bytes per event, one copy | Log bytes, all replicas | Replication traffic bytes (derived) |
|---|---|---|---|---|---|
| A previous-value schema | none | 25499103 | 254.99103 | 76497309 | 50998206 |
| A previous-value schema | zstd | 9061197 | 90.61197 | 27183591 | 18122394 |
| B audit trail | none | 14907975 | 149.07975 | 44723925 | 29815950 |
| B audit trail | zstd | 5965863 | 59.65863 | 17897589 | 11931726 |

### SPARSE model: producer latency, send to acknowledgement, ms

| Design | Compression | p50 | p95 | p99 |
|---|---|---|---|---|
| A previous-value schema | none | 233.358333 | 395.883875 | 412.285291 |
| A previous-value schema | zstd | 320.452375 | 361.476541 | 364.641125 |
| B audit trail | none | 344.942209 | 417.859125 | 432.024666 |
| B audit trail | zstd | 458.628 | 977.6815 | 1000.312708 |

### SPARSE model: Flink throughput and checkpoints

| Design | Topic compression | Events per second | Checkpoint end-to-end ms | Checkpointed size bytes | Checkpoints completed |
|---|---|---|---|---|---|
| A previous-value schema | none | 131926.1213720316... | 84 | 4434 | 1 |
| A previous-value schema | zstd | 139664.8044692737... | 90 | 4434 | 1 |
| B audit trail | none | 165837.4792703150... | 93 | 4434 | 1 |
| B audit trail | zstd | 170068.0272108843... | 99 | 4434 | 1 |

### Iceberg metadata growth

Each run appends the same events to a fresh table in 20 bounded Flink jobs through the same Iceberg sink, one commit each, commit k holding event_sequence values of slice k for every customer. Then: snapshot expiration (expireSnapshots, retainLast 1), compaction (Actions.rewriteDataFiles, default target file size), and expiration again. Metadata bytes are the files under the table's metadata/ directory (metadata JSON, manifest lists, manifests, version hint); Iceberg's defaults keep old metadata JSON files. History check: DuckDB over the current snapshot's data files computes the row count, distinct (customer_id, event_sequence), and a hash over LAG(email) and the previous known email (LAST_VALUE IGNORE NULLS, frame ending at 1 PRECEDING) for every row; the hash must equal the one taken after the commits.

#### FULL model

| Design | State | Snapshots | Metadata bytes | Metadata JSON files | Manifest lists | Manifest files | Manifests in current snapshot | Data files in current snapshot | Data files on disk | Rows | History unchanged |
|---|---|---|---|---|---|---|---|---|---|---|---|
| A previous-value schema | after commits | 20 | 1008396 | 22 | 20 | 20 | 20 | 60 | 60 | 100000 | yes |
| A previous-value schema | after expiration | 1 | 930078 | 23 | 1 | 20 | 20 | 60 | 60 | 100000 | yes |
| A previous-value schema | after compaction | 2 | 1384032 | 24 | 2 | 41 | 21 | 2 | 62 | 100000 | yes |
| A previous-value schema | after compaction and expiration | 1 | 983777 | 25 | 1 | 21 | 21 | 2 | 62 | 100000 | yes |
| B audit trail | after commits | 20 | 787228 | 22 | 20 | 20 | 20 | 60 | 60 | 100000 | yes |
| B audit trail | after expiration | 1 | 703911 | 23 | 1 | 20 | 20 | 60 | 60 | 100000 | yes |
| B audit trail | after compaction | 2 | 1035884 | 24 | 2 | 41 | 21 | 2 | 62 | 100000 | yes |
| B audit trail | after compaction and expiration | 1 | 741929 | 25 | 1 | 21 | 21 | 2 | 62 | 100000 | yes |

#### SPARSE model

| Design | State | Snapshots | Metadata bytes | Metadata JSON files | Manifest lists | Manifest files | Manifests in current snapshot | Data files in current snapshot | Data files on disk | Rows | History unchanged |
|---|---|---|---|---|---|---|---|---|---|---|---|
| A previous-value schema | after commits | 20 | 1097589 | 22 | 20 | 20 | 20 | 60 | 60 | 100000 | yes |
| A previous-value schema | after expiration | 1 | 1019271 | 23 | 1 | 20 | 20 | 60 | 60 | 100000 | yes |
| A previous-value schema | after compaction | 2 | 1564440 | 24 | 2 | 41 | 21 | 2 | 62 | 100000 | yes |
| A previous-value schema | after compaction and expiration | 1 | 1074575 | 25 | 1 | 21 | 21 | 2 | 62 | 100000 | yes |
| B audit trail | after commits | 20 | 839515 | 22 | 20 | 20 | 20 | 60 | 60 | 100000 | yes |
| B audit trail | after expiration | 1 | 756248 | 23 | 1 | 20 | 20 | 60 | 60 | 100000 | yes |
| B audit trail | after compaction | 2 | 1142178 | 24 | 2 | 41 | 21 | 2 | 62 | 100000 | yes |
| B audit trail | after compaction and expiration | 1 | 795482 | 25 | 1 | 21 | 21 | 2 | 62 | 100000 | yes |

Files: `pipeline_kafka.csv`, `pipeline_iceberg.csv` (medians), `pipeline_kafka_runs.csv`, `pipeline_iceberg_runs.csv` (every run), `pipeline_environment.json`.

## Wikimedia field study

Not synthetic: a capture of Wikimedia's public `mediawiki.page_change.v1` stream from EventStreams (https://stream.wikimedia.org/v2/stream/mediawiki.page_change.v1), read with `study/wikimedia/capture.py`. Raw events are not in the repository; `study/wikimedia/README.md` gives the commands to capture again.

Every number below was measured by `python3 study/wikimedia/study.py --capture <data>/capture-earliest-to-2026-10-20T00Z --capture <data>/capture-2026-10-20T00Z-2026-10-23T02Z --work <work dir> --out results/wikimedia --summary results/SUMMARY.md` on Apple M3 (8 logical CPUs), DuckDB 1.4.5, wall time 1332.8 s. Rates are exact fractions, shown with their first ten decimals truncated where they do not terminate.

### Capture

Each topic is one Kafka partition per Wikimedia datacenter. The capture resumes after every disconnect from the next Kafka offset (`meta.offset`) of each topic and logs any jump in offsets as a gap. Canary events (`meta.domain` = canary) are dropped. Commands: `capture.py --start earliest --end-offsets ...` for the oldest retained events up to where the second part began, and `capture.py --start ... --end ...` for the second part, both at --rate 1000 events per second on one connection.

| Capture | Topic | First offset | First meta.dt | End offset (exclusive) | End meta.dt | Events written | Canaries dropped | Duplicates skipped | Received after end | Offset gaps |
|---|---|---|---|---|---|---|---|---|---|---|
| capture-earliest-to-2026-10-20T00Z | codfw.mediawiki.page_change.v1:0 | 838991007 | 2026-10-14T05:15:00.000Z | 839004750 | 2026-10-20T00:15:00.000Z | 13,601 | 142 | 0 | 319,217 | 0 |
| capture-earliest-to-2026-10-20T00Z | eqiad.mediawiki.page_change.v1:0 | 1135868636 | 2026-10-16T01:48:44.064Z | 1141361946 | 2026-10-20T00:00:00.161Z | 5,493,213 | 97 | 0 | 0 | 0 |
| capture-2026-10-20T00Z-2026-10-23T02Z | codfw.mediawiki.page_change.v1:0 | 839004750 | 2026-10-20T00:15:00.000Z | 839794368 | 2026-10-23T02:15:00.000Z | 789,542 | 76 | 0 | 2 | 0 |
| capture-2026-10-20T00Z-2026-10-23T02Z | eqiad.mediawiki.page_change.v1:0 | 1141361946 | 2026-10-20T00:00:00.161Z | 1144455779 | 2026-10-23T02:00:00.069Z | 3,093,757 | 76 | 0 | 0 | 0 |

| Capture | Connection record | Reason | Count |
|---|---|---|---|
| capture-2026-10-20T00Z-2026-10-23T02Z | connected |  | 16 |
| capture-2026-10-20T00Z-2026-10-23T02Z | disconnected | IncompleteRead: IncompleteRead(0 bytes read) | 13 |
| capture-2026-10-20T00Z-2026-10-23T02Z | disconnected | server closed the stream | 1 |
| capture-2026-10-20T00Z-2026-10-23T02Z | restart_from_checkpoint |  | 1 |
| capture-earliest-to-2026-10-20T00Z | connected |  | 16 |
| capture-earliest-to-2026-10-20T00Z | disconnected | IncompleteRead: IncompleteRead(0 bytes read) | 13 |
| capture-earliest-to-2026-10-20T00Z | disconnected | server closed the stream | 1 |
| capture-earliest-to-2026-10-20T00Z | restart_from_checkpoint |  | 1 |
| capture-earliest-to-2026-10-20T00Z | stopped_on_signal |  | 1 |

Offset gaps across both parts and between them: 0. Every offset from the first to the end offset of each topic was received once.

### Stream and schema

Loaded events: 9,390,113, from meta.dt 2026-10-14T10:29:07.853Z to 2026-10-23T01:59:59.948Z. Schema versions (`$schema`): /mediawiki/page/change/1.12.0 9,390,113. Events per page_change_kind: create 1,069,941, delete 96,000, edit 8,187,754, move 34,612, undelete 611, visibility_change 1,195.

prior_state fields present, per page_change_kind (events of that kind carrying the field):

| Field | create | delete | edit | move | undelete | visibility_change |
|---|---|---|---|---|---|---|
| prior_state.page.is_redirect | 0 of 1,069,941 | 0 of 96,000 | 0 of 8,187,754 | 0 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.page.namespace_id | 0 of 1,069,941 | 0 of 96,000 | 0 of 8,187,754 | 4,464 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.page.namespace_is_content | 0 of 1,069,941 | 0 of 96,000 | 0 of 8,187,754 | 3,221 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.page.page_id | 0 of 1,069,941 | 0 of 96,000 | 0 of 8,187,754 | 0 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.page.page_title | 0 of 1,069,941 | 0 of 96,000 | 0 of 8,187,754 | 34,612 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.page.revision_count | 0 of 1,069,941 | 0 of 96,000 | 0 of 8,187,754 | 0 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.comment | 0 of 1,069,941 | 0 of 96,000 | 8,186,678 of 8,187,754 | 34,611 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.content_slots.main.content_body | 0 of 1,069,941 | 0 of 96,000 | 0 of 8,187,754 | 0 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.content_slots.main.content_format | 0 of 1,069,941 | 0 of 96,000 | 8,187,753 of 8,187,754 | 34,612 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.content_slots.main.content_model | 0 of 1,069,941 | 0 of 96,000 | 8,187,753 of 8,187,754 | 34,612 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.content_slots.main.content_sha1 | 0 of 1,069,941 | 0 of 96,000 | 8,187,753 of 8,187,754 | 34,612 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.content_slots.main.content_size | 0 of 1,069,941 | 0 of 96,000 | 8,187,753 of 8,187,754 | 34,612 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.content_slots.main.origin_rev_id | 0 of 1,069,941 | 0 of 96,000 | 8,187,753 of 8,187,754 | 34,612 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.content_slots.main.slot_role | 0 of 1,069,941 | 0 of 96,000 | 8,187,753 of 8,187,754 | 34,612 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.content_slots.mediainfo.content_body | 0 of 1,069,941 | 0 of 96,000 | 0 of 8,187,754 | 0 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.content_slots.mediainfo.content_format | 0 of 1,069,941 | 0 of 96,000 | 1,857,495 of 8,187,754 | 3,841 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.content_slots.mediainfo.content_model | 0 of 1,069,941 | 0 of 96,000 | 1,857,495 of 8,187,754 | 3,841 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.content_slots.mediainfo.content_sha1 | 0 of 1,069,941 | 0 of 96,000 | 1,857,495 of 8,187,754 | 3,841 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.content_slots.mediainfo.content_size | 0 of 1,069,941 | 0 of 96,000 | 1,857,495 of 8,187,754 | 3,841 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.content_slots.mediainfo.origin_rev_id | 0 of 1,069,941 | 0 of 96,000 | 1,857,495 of 8,187,754 | 3,841 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.content_slots.mediainfo.slot_role | 0 of 1,069,941 | 0 of 96,000 | 1,857,495 of 8,187,754 | 3,841 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.editor.edit_count | 0 of 1,069,941 | 0 of 96,000 | 8,159,937 of 8,187,754 | 34,450 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.editor.edit_global_count | 0 of 1,069,941 | 0 of 96,000 | 8,159,222 of 8,187,754 | 34,446 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.editor.first_edit_dt | 0 of 1,069,941 | 0 of 96,000 | 8,159,215 of 8,187,754 | 34,444 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.editor.groups | 0 of 1,069,941 | 0 of 96,000 | 8,187,495 of 8,187,754 | 34,609 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.editor.is_bot | 0 of 1,069,941 | 0 of 96,000 | 8,187,495 of 8,187,754 | 34,609 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.editor.is_system | 0 of 1,069,941 | 0 of 96,000 | 8,187,495 of 8,187,754 | 34,609 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.editor.is_temp | 0 of 1,069,941 | 0 of 96,000 | 8,187,495 of 8,187,754 | 34,609 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.editor.registration_dt | 0 of 1,069,941 | 0 of 96,000 | 8,026,893 of 8,187,754 | 33,442 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.editor.user_central_id | 0 of 1,069,941 | 0 of 96,000 | 8,159,355 of 8,187,754 | 34,447 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.editor.user_id | 0 of 1,069,941 | 0 of 96,000 | 8,159,937 of 8,187,754 | 34,450 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.editor.user_text | 0 of 1,069,941 | 0 of 96,000 | 8,187,495 of 8,187,754 | 34,609 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.editor.wiki_id | 0 of 1,069,941 | 0 of 96,000 | 8,187,495 of 8,187,754 | 34,609 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.is_comment_visible | 0 of 1,069,941 | 55 of 96,000 | 8,187,753 of 8,187,754 | 34,612 of 34,612 | 0 of 611 | 1,082 of 1,195 |
| prior_state.revision.is_content_visible | 0 of 1,069,941 | 55 of 96,000 | 8,187,753 of 8,187,754 | 34,612 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.is_editor_visible | 0 of 1,069,941 | 55 of 96,000 | 8,187,753 of 8,187,754 | 34,612 of 34,612 | 0 of 611 | 172 of 1,195 |
| prior_state.revision.is_minor_edit | 0 of 1,069,941 | 0 of 96,000 | 8,187,753 of 8,187,754 | 34,612 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.rev_dt | 0 of 1,069,941 | 0 of 96,000 | 8,187,753 of 8,187,754 | 34,612 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.rev_id | 0 of 1,069,941 | 0 of 96,000 | 8,187,753 of 8,187,754 | 34,612 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.rev_parent_id | 0 of 1,069,941 | 0 of 96,000 | 8,187,753 of 8,187,754 | 34,612 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.rev_sha1 | 0 of 1,069,941 | 0 of 96,000 | 8,187,753 of 8,187,754 | 34,612 of 34,612 | 0 of 611 | 0 of 1,195 |
| prior_state.revision.rev_size | 0 of 1,069,941 | 0 of 96,000 | 8,187,753 of 8,187,754 | 34,612 of 34,612 | 0 of 611 | 0 of 1,195 |

### Ordering key

Entity key (wiki_id, page.page_id). Ordering key `dt, revision__rev_id, meta__dt, meta__topic, meta__offset`. The schema defines `dt` as the event time, "when the event occurred", in whole seconds, and `meta.dt` as the time Wikimedia's system received the event. `revision.rev_id` is the database revision ID, assigned by the wiki in commit order. So (dt, rev_id) is the article's (event_timestamp, event_sequence); meta.dt, then the Kafka topic and offset, only make the order total where both are tied. Kafka offsets alone order events only within one datacenter topic. Tied events below share their entity and every column of the candidate. "Equal" counts events whose LAG(revision.rev_id) under that ordering, completed with the remaining key columns, equals prior_state.revision.rev_id, out of events with both.

| Ordering | Tie groups | Tied events | Equal / compared | Pages in both topics: equal / compared | Cross-topic pairs | Cross-topic: equal / compared |
|---|---|---|---|---|---|---|
| dt, revision.rev_id | 1 | 2 | 3,155,922 / 3,160,644 | 610,760 / 612,514 | 147,208 | 144,509 / 145,587 |
| meta.dt | 18 | 36 | 3,146,296 / 3,160,332 | 607,592 / 612,487 | 147,197 | 143,989 / 145,571 |
| dt | 142,694 | 297,581 | 3,155,922 / 3,160,644 | 610,760 / 612,514 | 147,208 | 144,509 / 145,587 |
| revision.rev_id | 26,277 | 52,774 | 3,155,915 / 3,160,665 | 610,760 / 612,517 | 147,210 | 144,510 / 145,589 |
| meta.dt, revision.rev_id | 0 | 0 | 3,146,296 / 3,160,332 | 607,592 / 612,487 | 147,197 | 143,989 / 145,571 |
| revision.rev_id, meta.dt | 0 | 0 | 3,155,915 / 3,160,665 | 610,760 / 612,517 | 147,210 | 144,510 / 145,589 |
| Kafka topic, offset | 0 | 0 | 3,026,345 / 3,137,188 | 487,667 / 589,343 | 113,240 | 23,952 / 88,851 |

The primary key leaves 1 tie group(s) of 2 events (jawiki:5309446:111461907, wiki_id:page_id:rev_id); meta.dt orders them.

Pages with events in both the eqiad and the codfw topic: 113,240.

Ordering sensitivity: revision mismatches (prior_state.revision.rev_id differs from LAG(revision.rev_id)) on the same data under three keys, each completed with the remaining key columns, over events with a predecessor in a loss-free interval. The primary analysis uses event time first.

| Ordering | Compared | Equal | Mismatched | Merge or re-keying: prior revision captured under another page_id | Missed event: prior revision newer than the LAG revision, not captured | Missed event: prior revision older than the LAG revision, not captured | Ordering: prior revision ordered after this event | Ordering: prior revision ordered before the LAG row |
|---|---|---|---|---|---|---|---|---|
| receive time: meta.dt | 3,160,332 | 3,146,296 | 14,036 | 0 | 4,707 | 1,162 | 2,844 | 5,323 |
| event time first: dt, rev_id (primary) | 3,160,644 | 3,155,922 | 4,722 | 0 | 4,714 | 2 | 0 | 6 |
| sequence first: rev_id, dt | 3,160,665 | 3,155,915 | 4,750 | 1 | 4,722 | 4 | 0 | 23 |

### Reconstruction with LAG

The query is the article's LAG form (`reconstruction.sql`): one LAG per prior_state field over the entity, ordered by the key above. An event is compared when its predecessor was captured and no offset gap falls between the two events' meta.dt.

| Measure | Value |
|---|---|
| Events | 9,390,113 |
| Entities (wiki_id, page_id) | 6,202,894 |
| Wikis | 709 |
| Excluded: no captured predecessor | 6,202,894 |
| Excluded: predecessor interval not loss-free | 0 |
| Compared | 3,187,219 |
| Compared, no field mismatched | 134,976 |

Per field, over compared events. Carried: prior_state has the field; exact match: equal to LAG. Omitted: prior_state lacks the field; unchanged: LAG equals the event's own value, as "only present if changed" implies.

| Field | Carried | Match | Differ | LAG null | Exact-match rate | Omitted | Omitted, unchanged | Omitted, changed | Omission-consistent rate |
|---|---|---|---|---|---|---|---|---|---|
| prior_state.page.is_redirect | 0 | 0 | 0 | 0 |  | 3,187,219 | 3,177,024 | 10,195 | 0.9968012866... |
| prior_state.page.namespace_id | 3,707 | 3,704 | 3 | 0 | 0.9991907202... | 3,183,512 | 3,183,509 | 3 | 0.9999990576... |
| prior_state.page.namespace_is_content | 2,872 | 2,869 | 3 | 0 | 0.9989554317... | 3,184,347 | 3,184,344 | 3 | 0.9999990578... |
| prior_state.page.page_id | 0 | 0 | 0 | 0 |  | 3,187,219 | 3,187,219 | 0 | 1 |
| prior_state.page.page_title | 16,701 | 16,690 | 11 | 0 | 0.9993413568... | 3,170,518 | 3,170,493 | 25 | 0.9999921148... |
| prior_state.page.revision_count | 0 | 0 | 0 | 0 |  | 344 | 0 | 344 | 0 |
| prior_state.revision.comment | 3,159,637 | 3,155,074 | 4,559 | 4 | 0.9985558467... | 26,593 | 25,363 | 1,230 | 0.9537472267... |
| prior_state.revision.content_slots.main.content_body | 0 | 0 | 0 | 0 |  | 0 | 0 | 0 |  |
| prior_state.revision.content_slots.main.content_format | 3,160,644 | 3,160,642 | 2 | 0 | 0.9999993672... | 26,574 | 26,522 | 52 | 0.9980432001... |
| prior_state.revision.content_slots.main.content_model | 3,160,644 | 3,160,638 | 6 | 0 | 0.9999981016... | 26,574 | 26,522 | 52 | 0.9980432001... |
| prior_state.revision.content_slots.main.content_sha1 | 3,160,644 | 3,159,914 | 730 | 0 | 0.9997690344... | 26,574 | 26,481 | 93 | 0.9965003386... |
| prior_state.revision.content_slots.main.content_size | 3,160,644 | 3,159,953 | 691 | 0 | 0.9997813736... | 26,574 | 26,481 | 93 | 0.9965003386... |
| prior_state.revision.content_slots.main.origin_rev_id | 3,160,644 | 3,159,904 | 740 | 0 | 0.9997658704... | 26,574 | 26,475 | 99 | 0.9962745540... |
| prior_state.revision.content_slots.main.slot_role | 3,160,644 | 3,160,644 | 0 | 0 | 1 | 26,574 | 26,522 | 52 | 0.9980432001... |
| prior_state.revision.content_slots.mediainfo.content_body | 0 | 0 | 0 | 0 |  | 0 | 0 | 0 |  |
| prior_state.revision.content_slots.mediainfo.content_format | 393,897 | 393,892 | 0 | 5 | 0.9999873063... | 3,708 | 3,707 | 1 | 0.9997303128... |
| prior_state.revision.content_slots.mediainfo.content_model | 393,897 | 393,892 | 0 | 5 | 0.9999873063... | 3,708 | 3,707 | 1 | 0.9997303128... |
| prior_state.revision.content_slots.mediainfo.content_sha1 | 393,897 | 393,842 | 50 | 5 | 0.9998603695... | 3,708 | 3,706 | 2 | 0.9994606256... |
| prior_state.revision.content_slots.mediainfo.content_size | 393,897 | 393,842 | 50 | 5 | 0.9998603695... | 3,708 | 3,706 | 2 | 0.9994606256... |
| prior_state.revision.content_slots.mediainfo.origin_rev_id | 393,897 | 393,873 | 19 | 5 | 0.9999390703... | 3,708 | 3,707 | 1 | 0.9997303128... |
| prior_state.revision.content_slots.mediainfo.slot_role | 393,897 | 393,892 | 0 | 5 | 0.9999873063... | 3,708 | 3,707 | 1 | 0.9997303128... |
| prior_state.revision.editor.edit_count | 3,154,035 | 153,113 | 3,000,832 | 90 | 0.0485451176... | 26,497 | 890 | 25,607 | 0.0335887081... |
| prior_state.revision.editor.edit_global_count | 3,153,645 | 136,457 | 3,017,091 | 97 | 0.0432696134... | 26,501 | 571 | 25,930 | 0.0215463567... |
| prior_state.revision.editor.first_edit_dt | 3,153,314 | 3,136,110 | 2,710 | 14,494 | 0.9945441525... | 25,978 | 23,388 | 2,590 | 0.9003002540... |
| prior_state.revision.editor.groups | 3,160,538 | 3,153,359 | 7,179 | 0 | 0.9977285512... | 26,524 | 26,177 | 347 | 0.9869175086... |
| prior_state.revision.editor.is_bot | 3,160,538 | 3,158,836 | 1,702 | 0 | 0.9994614840... | 26,524 | 26,332 | 192 | 0.9927612728... |
| prior_state.revision.editor.is_system | 3,160,538 | 3,160,521 | 17 | 0 | 0.9999946211... | 26,524 | 26,354 | 170 | 0.9935907103... |
| prior_state.revision.editor.is_temp | 3,160,538 | 3,160,403 | 135 | 0 | 0.9999572857... | 26,524 | 26,348 | 176 | 0.9933645000... |
| prior_state.revision.editor.registration_dt | 3,095,239 | 3,092,870 | 2,244 | 125 | 0.9992346309... | 25,746 | 25,462 | 284 | 0.9889691602... |
| prior_state.revision.editor.user_central_id | 3,153,739 | 3,151,266 | 2,378 | 95 | 0.9992158514... | 26,503 | 26,276 | 227 | 0.9914349318... |
| prior_state.revision.editor.user_id | 3,154,035 | 3,151,566 | 2,379 | 90 | 0.9992171932... | 26,497 | 26,277 | 220 | 0.9916971732... |
| prior_state.revision.editor.user_text | 3,160,538 | 3,157,745 | 2,793 | 0 | 0.9991162896... | 26,524 | 26,270 | 254 | 0.9904237671... |
| prior_state.revision.editor.wiki_id | 3,160,538 | 3,160,538 | 0 | 0 | 1 | 26,524 | 26,356 | 168 | 0.9936661137... |
| prior_state.revision.is_comment_visible | 3,161,773 | 3,161,750 | 23 | 0 | 0.9999927256... | 25,446 | 25,445 | 1 | 0.9999607010... |
| prior_state.revision.is_content_visible | 3,160,696 | 3,160,676 | 20 | 0 | 0.9999936722... | 26,523 | 26,522 | 1 | 0.9999622968... |
| prior_state.revision.is_editor_visible | 3,160,861 | 3,160,858 | 3 | 0 | 0.9999990508... | 26,358 | 26,353 | 5 | 0.9998103042... |
| prior_state.revision.is_minor_edit | 3,160,644 | 3,159,589 | 1,055 | 0 | 0.9996662072... | 26,575 | 26,538 | 37 | 0.9986077140... |
| prior_state.revision.rev_dt | 3,160,644 | 3,155,960 | 4,684 | 0 | 0.9985180235... | 26,575 | 26,489 | 86 | 0.9967638758... |
| prior_state.revision.rev_id | 3,160,644 | 3,155,922 | 4,722 | 0 | 0.9985060006... | 26,575 | 26,488 | 87 | 0.9967262464... |
| prior_state.revision.rev_parent_id | 3,160,644 | 3,155,922 | 4,722 | 0 | 0.9985060006... | 26,575 | 26,488 | 87 | 0.9967262464... |
| prior_state.revision.rev_sha1 | 3,160,644 | 3,159,868 | 776 | 0 | 0.9997544804... | 26,574 | 26,480 | 94 | 0.9964627079... |
| prior_state.revision.rev_size | 3,160,644 | 3,159,938 | 706 | 0 | 0.9997766278... | 26,574 | 26,481 | 93 | 0.9965003386... |
| all fields | 90,784,592 | 84,706,532 | 6,063,035 | 15,025 | 0.9330496523... | 16,676,085 | 16,607,180 | 68,905 | 0.9958680349... |
| all fields except revision.editor.edit_count and edit_global_count | 84,476,912 | 84,416,962 | 45,112 | 14,838 | 0.9992903386... | 16,623,087 | 16,605,719 | 17,368 | 0.9989551880... |

Mismatch classes. A mismatch is a compared (event, field) pair that differs, that LAG leaves null, or that prior_state omits although LAG shows it changed. Causes follow the rules in `reconstruct.py`; examples are wiki_id:page_id:rev_id. Per-field rows are in mismatch_classes.csv.

| Cause | Detail | (event, field) mismatches | Fields | Examples |
|---|---|---|---|---|
| delete and undelete | this event is an undelete | 967 | 27 | abstractwiki:6296:21989; abstractwiki:6297:23113; abstractwiki:6302:22004; abstractwiki:6303:22005; abstractwiki:6305:22007 |
| delete and undelete | this event is a delete | 925 | 32 | arwiki:10542489:77027090; bhwiki:101717:806991; bnwiki:1665140:9209972; bnwiki:1665365:9213583; bnwiki:1665378:9213584 |
| delete and undelete | previous event is an undelete | 56 | 4 | azwiki:1005017:9146800; commonswiki:201990638:1288789649; commonswiki:201990663:1288789991; commonswiki:201990815:1288791447; commonswiki:201990866:1288791911 |
| delete and undelete | previous event is a delete | 1 | 1 | jawiki:5309446:111461907 |
| missed event | prior revision newer than the LAG revision, not captured | 48,162 | 34 | abwiki:57617:175752; abwiki:57702:175895; afwiki:392174:3035334; afwiki:424455:3024535; arwiki:10401070:77007316 |
| missed event | prior revision older than the LAG revision, not captured | 23 | 16 | dewiki:14008465:271503045; hewiki:2562527:44077014 |
| ordering | prior revision ordered before the LAG row | 81 | 25 | cewiki:1920380:26609999; cewiki:534202:26609998; enwiki:38774416:1380755126; eswiki:11602061:175861895; eswiki:11624194:175882105 |
| other | editor record changed between the two events | 6,085,343 | 12 | abstractwiki:1191:24204; abstractwiki:1191:24258; abstractwiki:1206:24201; abstractwiki:1262:24412; abstractwiki:1262:24441 |
| other | no event kind or lineage explains it | 10,175 | 14 | abwiki:57520:175616; afwiki:124439:2986329; afwiki:14894:2999724; afwiki:474773:3035391; afwiki:479858:3035389 |
| page move | previous event is a move | 133 | 5 | arwiki:10376319:77037400; arwiki:10461831:77034671; arwiki:10525528:77035176; arwiki:10541967:77025769; arwiki:364316:77038575 |
| page move | this event is a move | 5 | 1 | arwiki:10375740:77027134; arwiki:10376321:77027128; arwiki:10523097:77027118; arwiki:10542330:77027095; dewiki:14006226:271461153 |
| visibility change | this event is a visibility_change | 1,094 | 18 | arwiki:10541479:77007485; bewiki:111679:5221227; bewiki:11597:5222550; bewiki:141600:5220727; bewiki:141600:5222558 |

Editor record: where prior_state and LAG describe the same revision, prior_state's revision.editor.edit_count is the larger in 2,992,199 of 2,996,212 differing events and the smaller in 4,013; edit_global_count 3,009,517 of 3,012,465 and 2,948. prior_state carries the previous revision's editor as that user's record stood when the later event was produced; LAG returns it as the earlier event carried it.

Per event: causes of its mismatched fields and the field groups involved.

| Causes | Field groups | Events | Examples |
|---|---|---|---|
| other | revision.editor | 3,035,754 | abstractwiki:1191:24204; abstractwiki:1191:24258; abstractwiki:1206:24201; abstractwiki:1262:24412; abstractwiki:1262:24441 |
| other | page, revision.editor | 6,472 | abwiki:57520:175616; afwiki:124439:2986329; afwiki:14894:2999724; afwiki:474773:3035391; afwiki:479858:3035389 |
| missed event | revision, revision.editor | 3,939 | abwiki:57617:175752; abwiki:57702:175895; afwiki:392174:3035334; afwiki:424455:3024535; arwiki:10401070:77007316 |
| other | page | 3,609 | anwiki:271770:2483632; arwiki:10540429:76996202; arwiki:10542045:77047108; arwiki:10542342:77024087; bewiki:817091:5219604 |
| other, visibility change | revision, revision.editor | 1,073 | arwiki:10541479:77007485; bewiki:111679:5221227; bewiki:11597:5222550; bewiki:141600:5220727; bewiki:141600:5222558 |
| missed event | revision, revision.content_slots, revision.editor | 745 | arwiki:10515844:77004177; arwiki:10531093:77015592; arwiki:10536516:77019716; arwiki:10538281:76989548; arwiki:10538303:77015473 |
| delete and undelete | page | 188 | abstractwiki:6296:21989; abstractwiki:6297:23113; abstractwiki:6302:22004; abstractwiki:6303:22005; abstractwiki:6305:22007 |
| delete and undelete, other | page, revision.editor | 137 | arwiki:10540174:77039297; arwiki:10540959:77001336; arwiki:10542489:77027090; arwiki:6979355:77042918; arwiki:9775011:76994405 |
| other, page move | page, revision.editor | 68 | arwiki:10375740:77027134; arwiki:10376319:77037400; arwiki:10376321:77027128; arwiki:10461831:77034671; arwiki:10523097:77027118 |
| delete and undelete, other | revision, revision.content_slots, revision.editor | 52 | bnwiki:1665140:9209972; bnwiki:1665365:9213583; bnwiki:1665378:9213584; commonswiki:178350381:1289738836; enwiki:74999434:1380883157 |
| delete and undelete | revision, revision.editor | 36 | bhwiki:101717:806991; commonswiki:201986183:1288846186; commonswiki:202066950:1289695913; dewiki:14004230:271476565; dewiki:14006008:271425678 |
| delete and undelete | page, revision, revision.content_slots, revision.editor | 34 | commonswiki:152249359:1246798449; commonswiki:163690183:1021864695; commonswiki:183024555:1158788944; commonswiki:201522958:1285745310; dewiki:13738836:263214064 |
| other | revision, revision.editor | 24 | commonswiki:1037956:1289961816; commonswiki:76740501:1287826130; elwiki:243424:11888130; elwiki:731199:11888161; elwiki:9216:11888508 |
| missed event | page, revision, revision.editor | 21 | dewiki:13922983:271327956; dewiki:14008465:271503045; enwiki:2175606:1381109798; enwiki:24775388:1380776782; enwiki:32274283:1380776644 |
| page move | revision, revision.content_slots | 18 | commonswiki:11194573:1288791131; commonswiki:11194637:1289039229; commonswiki:11194656:1289042335; commonswiki:11394628:1289042671; commonswiki:11424014:1289043501 |
| page move | page | 16 | commonswiki:202110445:1290103639; commonswiki:202132040:1290385837; commonswiki:202132058:1290390151; enwiki:48237951:1381085496; enwiki:66900605:1380527508 |
| delete and undelete | revision, revision.content_slots | 14 | commonswiki:201990638:1288789649; commonswiki:201990663:1288789991; commonswiki:201990796:1288401980; commonswiki:201990815:1288791447; commonswiki:201990866:1288791911 |
| delete and undelete | revision, revision.content_slots, revision.editor | 9 | commonswiki:201692496:1287371423; commonswiki:201706817:1287532728; eswiki:7266504:175892137; eswiki:7266504:88661217; jawiki:5309446:111461907 |
| missed event | page, revision, revision.content_slots, revision.editor | 8 | commonswiki:24760202:1289129735; enwiki:53744976:1380534015; enwiki:81177375:1380548883; enwiki:84502953:1380199373; fawiki:7057220:44793487 |
| delete and undelete | page, revision, revision.editor | 4 | commonswiki:160091909:1289532998; commonswiki:201986183:1288773037; eswiktionary:372317:6319715; kowiki:4289071:42670992 |
| delete and undelete, other | revision, revision.editor | 4 | metawiki:13584314:31182692; metawiki:13584315:31182693; metawiki:13586194:31193749; plwikiquote:74473:650754 |
| delete and undelete | page, revision, revision.content_slots | 3 | eswiki:2469329:171424902; eswiki:2469329:171765767; ptwiki:6435196:65151137 |
| missed event | revision | 3 | commonswiki:201915596:1288568634; commonswiki:202022139:1289089966; dewiki:14007742:271480676 |
| ordering | page, revision, revision.editor | 3 | cewiki:1920380:26609999; enwiki:38774416:1380755126; hewiki:2585305:44102519 |
| visibility change | revision | 3 | ruwiki:5110592:155688338; ruwiki:9308081:155658905; ukwiki:1967972:48996942 |
| ordering | page, revision, revision.content_slots, revision.editor | 2 | eswiki:11602061:175861895; eswiki:11624194:175882105 |
| delete and undelete | page, revision | 1 | enwiki:74999434:1380883157 |
| ordering | page, revision | 1 | cewiki:534202:26609998 |
| other | revision | 1 | enwiki:34745517:1381207584 |
| visibility change | revision, revision.content_slots, revision.editor | 1 | ruwiki:138838:155609777 |

Revision mismatches under the primary key, by cause, event kind, the predecessor's kind and whether the predecessor is in the other datacenter topic (revision_mismatches.csv):

| Cause | Detail | Kind | Previous kind | Cross-topic | Events | Examples |
|---|---|---|---|---|---|---|
| missed event | prior revision newer than the LAG revision, not captured | edit | edit | false | 3,290 | afwiki:424455:3024535; arwiki:251734:77047210; arwiki:276175:77026031; arwiki:385567:77038751; arwiki:1461813:77001900 |
| missed event | prior revision newer than the LAG revision, not captured | edit | edit | true | 1,037 | afwiki:392174:3035334; arwiki:8483:76986962; arwiki:350577:77032530; arwiki:1690920:76985130; arwiki:1870192:76986278 |
| missed event | prior revision newer than the LAG revision, not captured | edit | create | false | 279 | abwiki:57617:175752; abwiki:57702:175895; arwiki:10540293:77002721; arwiki:10545843:77047857; arzwiki:2336925:13279529 |
| missed event | prior revision newer than the LAG revision, not captured | move | edit | false | 39 | bhwiki:101774:807470; bswiki:538724:3935956; bswiki:538727:3935957; commonswiki:125334850:1288491603; commonswiki:128602692:1288491126 |
| missed event | prior revision newer than the LAG revision, not captured | edit | create | true | 34 | arwiki:10538390:77015470; arzwiki:2337391:13287932; arzwiki:2337395:13287933; arzwiki:2337406:13287942; commonswiki:201730435:1288083580 |
| missed event | prior revision newer than the LAG revision, not captured | edit | move | false | 15 | commonswiki:12147617:1290246692; commonswiki:201891782:1288899197; commonswiki:201891786:1288899236; commonswiki:201915596:1288568634; commonswiki:202022139:1289089966 |
| missed event | prior revision newer than the LAG revision, not captured | move | create | false | 7 | commonswiki:202009409:1288978034; commonswiki:202027257:1289150142; siwiki:197325:801988; siwiki:197326:802243; siwiki:197327:802241 |
| ordering | prior revision ordered before the LAG row | move | move | false | 4 | cewiki:534202:26609998; cewiki:1920380:26609999; enwiki:38774416:1380755126; hewiki:2585305:44102519 |
| missed event | prior revision newer than the LAG revision, not captured | edit | visibility_change | false | 3 | enwiki:171752:1381027168; enwiki:5367300:1380664237; ptwiki:4503116:73254678 |
| missed event | prior revision newer than the LAG revision, not captured | edit | undelete | false | 3 | bgwiki:363686:13057027; commonswiki:201986183:1288846807; eswiki:2469329:175904306 |
| missed event | prior revision newer than the LAG revision, not captured | edit | move | true | 2 | commonswiki:200517916:1289765540; fawiki:501582:44789164 |
| missed event | prior revision newer than the LAG revision, not captured | move | edit | true | 2 | enwiki:84483275:1380558914; zhwiki:9634691:95110746 |
| missed event | prior revision older than the LAG revision, not captured | move | move | false | 2 | dewiki:14008465:271503045; hewiki:2562527:44077014 |
| ordering | prior revision ordered before the LAG row | edit | delete | false | 2 | eswiki:11602061:175861895; eswiki:11624194:175882105 |
| missed event | prior revision newer than the LAG revision, not captured | edit | visibility_change | true | 2 | enwiki:1805149:1381234794; ruwiki:10855425:155638023 |
| missed event | prior revision newer than the LAG revision, not captured | edit | undelete | true | 1 | enwiki:48080939:1380965741 |

The capture has no offset gaps, so "missed event" here means the stream holds no page_change event for that revision, not an event the capture lost.

#### Checks against the MediaWiki API

On 2026-10-23T05:21:41Z, 208 requests to Wikimedia's public Action API, one at a time with the study's User-Agent (`api_checks.py`). Samples are drawn with Python's random.Random(20261113) from the sorted candidate identifiers. Revision content read to decide redirect status was discarded.

is_redirect: 50 sampled of 10,195 events where prior_state.page omits is_redirect while LAG shows a change. Each event's previous revision and own revision were read and classified as redirect or not from their content (the wiki's redirect magic word and a [[link]] for wikitext; a "redirect" key for Wikibase entities).

| Verdict | Reason | Events | Identifiers (wiki_id:page_id:rev_id) |
|---|---|---|---|
| ambiguous | revision not found | 2 | enwiktionary:12270658:93622929; frwiki:17669692:240429566 |
| confirms LAG |  | 48 | bnwikisource:887783:2004578; commonswiki:201863902:1288039720; commonswiki:202049693:1289436604; dewiki:14005144:271392986; enwiki:20471612:1381242732; enwiki:31524227:1381287834; enwiki:34743582:1381254978; enwiki:50718126:1381258800; enwiki:83889415:1381203713; enwiki:84527970:1380558436; enwiki:84577965:1381177407; enwiki:84584507:1381294283; frwiki:13703575:240594982; frwiki:17671163:240453720; jawiki:5308973:111448551; mswiki:1303312:6983912; wikidatawiki:33297279:2559449661; wikidatawiki:43925149:2557993029; wikidatawiki:55642629:2558129775; wikidatawiki:65293887:2559525255; wikidatawiki:75814219:2557686332; wikidatawiki:100508974:2559379140; wikidatawiki:103095171:2558687069; wikidatawiki:103885815:2559685022; wikidatawiki:107344546:2558085817; wikidatawiki:119182275:2558722514; wikidatawiki:127630006:2559738832; wikidatawiki:130085377:2559528212; wikidatawiki:132041874:2557646553; wikidatawiki:135076560:2559511787; wikidatawiki:135080832:2558994440; wikidatawiki:135081416:2559179452; wikidatawiki:135085112:2559465582; wikidatawiki:135330487:2557913844; wikidatawiki:135371028:2559334648; wikidatawiki:135383201:2559684262; zhwikisource:8875243:11146638; zhwikisource:8890418:11162454; zhwikisource:8918415:11191524; zhwikisource:8930324:11203889; zhwikisource:8949509:11223808; zhwikisource:8958351:11233042; zhwikisource:8980360:11255934; zhwikisource:8983526:11259347; zhwikisource:8996570:11272998; zhwikisource:9032875:11310616; zhwikisource:9050844:11329241; zhwikisource:9134709:11416938 |

Revisions no captured event carries: 50 sampled of 4,716 events under the primary key whose prior_state.revision.rev_id has no captured event. Imported means an import log entry at the revision's own timestamp, or import log entries on the page and a revision ID higher than the previous captured revision's with an older timestamp.

| Category | Created by (log entry at the revision's timestamp) | Events | Identifiers (wiki_id:page_id:rev_id > revision with no event) |
|---|---|---|---|
| exists | upload/overwrite | 27 | commonswiki:18106770:1288940815 > 1288940751; commonswiki:46205814:1290015719 > 1290015708; commonswiki:154021331:1288604802 > 1288269538; commonswiki:165746800:1288266974 > 1288266963; commonswiki:199454752:1289772176 > 1289761819; commonswiki:200688038:1288047791 > 1288046773; commonswiki:200948239:1288255600 > 1288150643; commonswiki:201245859:1287870152 > 1287870129; commonswiki:201677743:1288350534 > 1288350523; commonswiki:201679307:1288530178 > 1288530157; commonswiki:201747276:1288178018 > 1288178006; commonswiki:201774257:1289189166 > 1289187596; commonswiki:201792599:1288416132 > 1288416118; commonswiki:201862113:1288040031 > 1288035940; commonswiki:201862673:1288192948 > 1288192926; commonswiki:201864466:1289328269 > 1288160681; commonswiki:201906319:1288460471 > 1288291112; commonswiki:201946794:1289574481 > 1288494595; commonswiki:202125313:1290412130 > 1290412114; enwiki:7426803:1380948278 > 1380948275; enwiki:84527561:1380639143 > 1380639141; enwiki:84528481:1380638126 > 1380638121; enwiki:84528597:1380637991 > 1380637987; enwiki:84529545:1380637937 > 1380637934; enwiki:84559029:1381093165 > 1381093162; enwiki:84585988:1381508229 > 1381508224; enwiki:84590914:1381507880 > 1381507877 |
| exists | no log entry at its timestamp | 11 | commonswiki:202129599:1290382832 > 1290382823; dewiki:14007320:271468555 > 271468549; enwiki:291228:1381499363 > 1381495597; enwiki:1624675:1381445416 > 1381441120; enwiki:75285719:1381076601 > 1381076598; enwiki:80546130:1380992562 > 1380377282; frwiki:12647457:240528164 > 240360156; jawiki:4371391:111459236 > 111459235; kowiki:4118494:42666569 > 42664265; mlwikivoyage:615:9098 > 3559; ruwiki:11630392:155637664 > 155637369 |
| exists | protect/protect | 6 | dewiki:636245:271428924 > 271428903; enwiki:1097898:1381116732 > 1381116677; enwiki:23558504:1380642952 > 1380642948; enwiki:65278063:1381379119 > 1381379081; itwiki:4923:152934229 > 152934228; viwiki:19846325:75649867 > 75649296 |
| exists | upload/revert | 1 | commonswiki:4557893:1288439528 > 1288439387 |
| imported | import/upload | 5 | dewiki:14005553:271414536 > 271409842; mlwikivoyage:557:9061 > 2534; mlwikivoyage:815:9186 > 6608; mlwikivoyage:840:9206 > 6887; mlwikivoyage:938:9263 > 8303 |

prior_state.revision.rev_id against revision.rev_parent_id:

| page_change_kind | Events | Equal | Differ | Only prior_state | Only rev_parent_id | Neither |
|---|---|---|---|---|---|---|
| create | 1,069,941 | 0 | 0 | 0 | 1,069,941 | 0 |
| delete | 96,000 | 0 | 0 | 0 | 96,000 | 0 |
| edit | 8,187,754 | 8,187,753 | 0 | 0 | 1 | 0 |
| move | 34,612 | 34,606 | 6 | 0 | 0 | 0 |
| undelete | 611 | 0 | 0 | 0 | 611 | 0 |
| visibility_change | 1,195 | 0 | 0 | 0 | 1,195 | 0 |

### Size

Per event: json is the event as received (compact JSON; writing the parsed event back with separators (",", ":") gives the same bytes for 9,390,113 of 9,390,113 events); avro is the Avro binary datum under `study/wikimedia/schema/page_change_1.12.0.avsc`, derived by `study/wikimedia/schema.py` from https://schema.wikimedia.org/repositories/primary/jsonschema/mediawiki/page/change/1.12.0: object to record, object with only additionalProperties to map, array to array, string to string, integer to long, number to double, boolean to boolean, a property outside `required` to a union with null. The four meta fields EventStreams adds (topic, partition, offset, key) are outside the published schema, so the Avro datum omits them. Percentiles are nearest-rank.

| Population | Measure | Events | Median bytes | p95 bytes | Min | Max | Total bytes |
|---|---|---|---|---|---|---|---|
| all events | json | 9,390,113 | 2838 | 3487 | 1011 | 5581 | 26,842,531,222 |
| all events | json_prior | 9,390,113 | 874 | 1170 | 0 | 2249 | 7,664,766,623 |
| all events | avro | 9,390,113 | 1241 | 1714 | 403 | 3788 | 12,019,412,405 |
| all events | avro_prior | 9,390,113 | 371 | 568 | 0 | 1754 | 3,336,999,046 |
| events with prior_state | json | 8,223,615 | 2878 | 3504 | 1011 | 5581 | 24,435,094,347 |
| events with prior_state | json_prior | 8,223,615 | 902 | 1180 | 39 | 2249 | 7,664,766,623 |
| events with prior_state | avro | 8,223,615 | 1264 | 1734 | 403 | 3788 | 10,865,841,794 |
| events with prior_state | avro_prior | 8,223,615 | 396 | 581 | 15 | 1754 | 3,336,999,046 |

Share of each event that prior_state takes, over events that carry it:

| Encoding | Events | Median share | p95 share | Min | Max | Sum of prior_state / sum of events | Same, over all events |
|---|---|---|---|---|---|---|---|
| json | 8,223,615 | 0.3163403106... | 0.3466707391... | 0.0166240409... | 0.5610191082... | 0.3136786178... | 0.2855455977... |
| avro | 8,223,615 | 0.3100065402... | 0.3667496886... | 0.0094280326... | 0.7166178317... | 0.3071091139... | 0.2776341249... |

Parquet of the whole capture, one column per leaf field, rows sorted by entity and ordering key, DuckDB 1.4.5 default row groups:

| Columns | Compression | Column count | Rows | Row groups | Bytes |
|---|---|---|---|---|---|
| with prior_state | snappy | 137 | 9,390,113 | 77 | 3,657,288,238 |
| with prior_state | zstd | 137 | 9,390,113 | 77 | 2,233,983,082 |
| without prior_state | snappy | 95 | 9,390,113 | 77 | 2,550,155,668 |
| without prior_state | zstd | 95 | 9,390,113 | 77 | 1,531,397,463 |

### prior_state field distributions

Presence over all events; string lengths over present values in UTF-8 bytes, nearest-rank.

| Field | Present | Of events | Length p50 | Length p95 | Length max |
|---|---|---|---|---|---|
| prior_state.page.is_redirect | 0 | 9,390,113 |  |  |  |
| prior_state.page.namespace_id | 4,464 | 9,390,113 |  |  |  |
| prior_state.page.namespace_is_content | 3,221 | 9,390,113 |  |  |  |
| prior_state.page.page_id | 0 | 9,390,113 |  |  |  |
| prior_state.page.page_title | 34,612 | 9,390,113 | 35 | 102 | 241 |
| prior_state.page.revision_count | 0 | 9,390,113 |  |  |  |
| prior_state.revision.comment | 8,221,289 | 9,390,113 | 78 | 203 | 1500 |
| prior_state.revision.content_slots.main.content_body | 0 | 9,390,113 |  |  |  |
| prior_state.revision.content_slots.main.content_format | 8,222,365 | 9,390,113 | 11 | 16 | 19 |
| prior_state.revision.content_slots.main.content_model | 8,222,365 | 9,390,113 | 8 | 13 | 23 |
| prior_state.revision.content_slots.main.content_sha1 | 8,222,365 | 9,390,113 | 31 | 31 | 31 |
| prior_state.revision.content_slots.main.content_size | 8,222,365 | 9,390,113 |  |  |  |
| prior_state.revision.content_slots.main.origin_rev_id | 8,222,365 | 9,390,113 |  |  |  |
| prior_state.revision.content_slots.main.slot_role | 8,222,365 | 9,390,113 | 4 | 4 | 4 |
| prior_state.revision.content_slots.mediainfo.content_body | 0 | 9,390,113 |  |  |  |
| prior_state.revision.content_slots.mediainfo.content_format | 1,861,336 | 9,390,113 | 16 | 16 | 16 |
| prior_state.revision.content_slots.mediainfo.content_model | 1,861,336 | 9,390,113 | 18 | 18 | 18 |
| prior_state.revision.content_slots.mediainfo.content_sha1 | 1,861,336 | 9,390,113 | 31 | 31 | 31 |
| prior_state.revision.content_slots.mediainfo.content_size | 1,861,336 | 9,390,113 |  |  |  |
| prior_state.revision.content_slots.mediainfo.origin_rev_id | 1,861,336 | 9,390,113 |  |  |  |
| prior_state.revision.content_slots.mediainfo.slot_role | 1,861,336 | 9,390,113 | 9 | 9 | 9 |
| prior_state.revision.editor.edit_count | 8,194,387 | 9,390,113 |  |  |  |
| prior_state.revision.editor.edit_global_count | 8,193,668 | 9,390,113 |  |  |  |
| prior_state.revision.editor.first_edit_dt | 8,193,659 | 9,390,113 | 20 | 20 | 20 |
| prior_state.revision.editor.groups | 8,222,104 | 9,390,113 |  |  |  |
| prior_state.revision.editor.is_bot | 8,222,104 | 9,390,113 |  |  |  |
| prior_state.revision.editor.is_system | 8,222,104 | 9,390,113 |  |  |  |
| prior_state.revision.editor.is_temp | 8,222,104 | 9,390,113 |  |  |  |
| prior_state.revision.editor.registration_dt | 8,060,335 | 9,390,113 | 20 | 20 | 20 |
| prior_state.revision.editor.user_central_id | 8,193,802 | 9,390,113 |  |  |  |
| prior_state.revision.editor.user_id | 8,194,387 | 9,390,113 |  |  |  |
| prior_state.revision.editor.user_text | 8,222,104 | 9,390,113 | 10 | 18 | 85 |
| prior_state.revision.editor.wiki_id | 8,222,104 | 9,390,113 | 11 | 12 | 20 |
| prior_state.revision.is_comment_visible | 8,223,502 | 9,390,113 |  |  |  |
| prior_state.revision.is_content_visible | 8,222,420 | 9,390,113 |  |  |  |
| prior_state.revision.is_editor_visible | 8,222,592 | 9,390,113 |  |  |  |
| prior_state.revision.is_minor_edit | 8,222,365 | 9,390,113 |  |  |  |
| prior_state.revision.rev_dt | 8,222,365 | 9,390,113 | 20 | 20 | 20 |
| prior_state.revision.rev_id | 8,222,365 | 9,390,113 |  |  |  |
| prior_state.revision.rev_parent_id | 8,222,365 | 9,390,113 |  |  |  |
| prior_state.revision.rev_sha1 | 8,222,365 | 9,390,113 | 31 | 31 | 31 |
| prior_state.revision.rev_size | 8,222,365 | 9,390,113 |  |  |  |

### Query cost: stored prior_state versus LAG

On the zstd Parquet file, measured as in the resources stage: a fresh in-memory DuckDB per run, 3 warm-up runs, then one profiled run; 20 runs; p50 and p95 nearest-rank. CPU ms is the profiler's cpu_time, latency its latency; bytes scanned are the compressed column chunks the scan reads in row groups its filters do not prune, from parquet_metadata(). The previous value is rev_id, rev_dt, rev_size and rev_sha1.

| Scope | Method | Rows | Latency ms p50 | Latency ms p95 | CPU ms p50 | CPU ms p95 | Bytes scanned | Columns read | Row groups read |
|---|---|---|---|---|---|---|---|---|---|
| one entity (labswiki:7919, 2212 events) | stored prior_state | 2,212 | 25.66875 | 27.17725 | 20.790789 | 21.863874 | 4,807,215 | 11 of 137 | 1 of 77 |
| one entity (labswiki:7919, 2212 events) | LAG | 2,212 | 24.773 | 26.037584 | 19.853295 | 20.927288999999998 | 4,512,867 | 10 of 137 | 1 of 77 |
| all entities | stored prior_state | 9,390,113 | 458.178334 | 511.34275 | 3474.3551109999995 | 3845.205294000001 | 337,368,616 | 8 of 137 | 77 of 77 |
| all entities | LAG | 9,390,113 | 2174.108625 | 3206.710542 | 13758.115295999998 | 19710.07825700001 | 396,386,631 | 10 of 137 | 77 of 77 |

Files in results/wikimedia: capture.csv, capture_gaps.csv, capture_connections.csv, ordering.csv, reconstruction_counts.csv, reconstruction_fields.csv, mismatch_classes.csv, mismatch_events.csv, lineage.csv, parent_check.csv, size_events.csv, size_prior_state_share.csv, size_parquet.csv, field_distributions.csv, query_cost.csv, reconstruction.sql, environment.json.

## Sort order of the audit-trail Parquet file

Measured by `bench/sort_order.py` from `bench/config.json` on 2026-10-23T10:41:11Z: design B (audit trail), seed 20261113, N=1000, H=100, K=67, FULL and SPARSE models. Machine: Apple M3, 8 logical CPUs. DuckDB 1.4.5, threads=8. Wall time 95.9 s. File bytes reproduce exactly when the rewrite check says so; latency and CPU time vary between runs.

- Every file is written by DuckDB's Parquet writer from the same rows with ROW_GROUP_SIZE 10000; only the ORDER BY of the COPY differs (column order_by in sort_order.csv). The customer_id, event_timestamp, event_sequence file is the main benchmark's file: its sizes equal storage.csv for design B (4 checks).
- The generator emits each entity's rows in event order. "customer_id, then hash order within an entity" is ORDER BY customer_id, hash(event_timestamp, event_sequence): a customer_id sort over rows that are not in event order, added beside the two requested orders.
- Identical to full sort: the snappy file's SHA-256 equals that of the customer_id, event_timestamp, event_sequence file. Rewrite identical: the same COPY run a second time produced a byte-identical snappy file.
- In the FULL and SPARSE models the customer_id file is byte-identical to the customer_id, event_timestamp, event_sequence file, so the two read the same bytes and their latency and CPU differences below are run-to-run variation.
- Out-of-order rows: adjacent rows of one entity, in file order, whose (event_timestamp, event_sequence) is lower than the row before.
- p50 ms: 20 timed runs after 3 warm-up runs on one connection, the protocol of the latency table above. CPU ms and bytes scanned: the protocol and definitions of the resource-use section above (median of 20 runs, each on a fresh in-memory database; bytes scanned derived from parquet_metadata(), not observed). The queries read the snappy file. Every file returned the same rows for every query.

### Files

| Model | Sort order | Parquet snappy bytes | Parquet zstd bytes | Identical to full sort | Rewrite identical | Out-of-order rows | Entities with out-of-order rows | Row groups |
|---|---|---|---|---|---|---|---|---|
| full | customer_id | 8,299,637 | 5,642,494 | yes | yes | 0 | 0 | 9 x 10240 + 1 x 7840 |
| full | customer_id, event_timestamp, event_sequence | 8,299,637 | 5,642,494 | yes | yes | 0 | 0 | 9 x 10240 + 1 x 7840 |
| full | customer_id, then hash order within an entity | 13,284,164 | 10,089,011 | no | yes | 49,558 | 1,000 | 9 x 10240 + 1 x 7840 |
| sparse | customer_id | 7,593,772 | 5,027,397 | yes | yes | 0 | 0 | 9 x 10240 + 1 x 7840 |
| sparse | customer_id, event_timestamp, event_sequence | 7,593,772 | 5,027,397 | yes | yes | 0 | 0 | 9 x 10240 + 1 x 7840 |
| sparse | customer_id, then hash order within an entity | 7,676,752 | 5,260,406 | no | yes | 49,558 | 1,000 | 9 x 10240 + 1 x 7840 |

### Queries

| Model | Query | Sort order | p50 ms | CPU ms | Bytes scanned | Columns read | Row groups read | Rows returned |
|---|---|---|---|---|---|---|---|---|
| full | q1_one_hop_previous_single | customer_id | 1.335 | 0.98418849999999995 | 56,100 | 4 of 70 | 1 of 10 | 100 |
| full | q1_one_hop_previous_single | customer_id, event_timestamp, event_sequence | 1.297 | 0.83791899999999995 | 56,100 | 4 of 70 | 1 of 10 | 100 |
| full | q1_one_hop_previous_single | customer_id, then hash order within an entity | 1.316 | 0.9627095 | 72,306 | 4 of 70 | 1 of 10 | 100 |
| full | q3_current_state_all | customer_id | 55.225 | 238.042146 | 7,590,968 | 70 of 70 | 10 of 10 | 1,000 |
| full | q3_current_state_all | customer_id, event_timestamp, event_sequence | 55.234 | 241.764714000000035 | 7,590,968 | 70 of 70 | 10 of 10 | 1,000 |
| full | q3_current_state_all | customer_id, then hash order within an entity | 60.768 | 258.975099999999985 | 12,573,329 | 70 of 70 | 10 of 10 | 1,000 |
| sparse | q1_one_hop_previous_single | customer_id | 1.323 | 1.01976949999999985 | 55,559 | 4 of 70 | 1 of 10 | 5 |
| sparse | q1_one_hop_previous_single | customer_id, event_timestamp, event_sequence | 1.296 | 1.0115199999999998 | 55,559 | 4 of 70 | 1 of 10 | 5 |
| sparse | q1_one_hop_previous_single | customer_id, then hash order within an entity | 1.312 | 1.0587279999999997 | 63,803 | 4 of 70 | 1 of 10 | 5 |
| sparse | q3_current_state_all | customer_id | 183.681 | 977.41107499999995 | 6,883,681 | 70 of 70 | 10 of 10 | 1,000 |
| sparse | q3_current_state_all | customer_id, event_timestamp, event_sequence | 187.812 | 978.85621250000005 | 6,883,681 | 70 of 70 | 10 of 10 | 1,000 |
| sparse | q3_current_state_all | customer_id, then hash order within an entity | 195.299 | 1002.0860819999999 | 6,965,127 | 70 of 70 | 10 of 10 | 1,000 |

Files: `sort_order.csv`, `sort_order_environment.json`.

## Engine scan bytes in the sql-matrix setup

Measured by `ci/engine_scan_bytes.py --engines spark,trino` on 2026-10-23T10:43:03Z, in the setup of `ci/run_sql_matrix.py`: each golden case's input.csv (3 or 4 rows) is loaded as customer_profile_changes, then every sql/ file the engine claims runs against it. Engines: Spark 4.0.4, Trino 483. Every query ran 2 times and the counter was the same each time.

- Trino: `physicalInputBytes` from the client's final query statistics. The table lives in Trino's memory connector, so the counter is the size of the in-memory pages the table scan produced, not bytes read from storage.
- Spark: `inputBytes` (the task metric inputMetrics.bytesRead) summed over the stages of every job the query ran, read from the SparkContext status store. The view is a DataFrame built from Python rows, so the scan is an in-memory RDD scan and no input bytes are recorded.
- sql/duckdb/ files claim DuckDB only and are not run here; DuckDB is not part of this measurement.

| SQL file | Golden case | Engine | Counter | Input bytes | Rows returned | Matches golden | Scan operator |
|---|---|---|---|---|---|---|---|
| sql/dedup.sql | case1_full_state | Spark | inputBytes (stage sum) | 0 | 3 | yes | Scan ExistingRDD |
| sql/dedup.sql | case1_full_state | Trino | physicalInputBytes | 196 | 3 | yes | table scan, catalog memory |
| sql/dedup.sql | case2_sparse | Spark | inputBytes (stage sum) | 0 | 3 | yes | Scan ExistingRDD |
| sql/dedup.sql | case2_sparse | Trino | physicalInputBytes | 174 | 3 | yes | table scan, catalog memory |
| sql/dedup.sql | case3_duplicate | Spark | inputBytes (stage sum) | 0 | 3 | yes | Scan ExistingRDD |
| sql/dedup.sql | case3_duplicate | Trino | physicalInputBytes | 261 | 3 | yes | table scan, catalog memory |
| sql/lag_history.sql | case1_full_state | Spark | inputBytes (stage sum) | 0 | 3 | yes | Scan ExistingRDD |
| sql/lag_history.sql | case1_full_state | Trino | physicalInputBytes | 196 | 3 | yes | table scan, catalog memory |
| sql/lag_history.sql | case2_sparse | Spark | inputBytes (stage sum) | 0 | 3 | yes | Scan ExistingRDD |
| sql/lag_history.sql | case2_sparse | Trino | physicalInputBytes | 174 | 3 | yes | table scan, catalog memory |
| sql/lag_history.sql | case3_duplicate | Spark | inputBytes (stage sum) | 0 | 4 | yes | Scan ExistingRDD |
| sql/lag_history.sql | case3_duplicate | Trino | physicalInputBytes | 261 | 4 | yes | table scan, catalog memory |
| sql/latest_state.sql | case1_full_state | Spark | inputBytes (stage sum) | 0 | 1 | yes | Scan ExistingRDD |
| sql/latest_state.sql | case1_full_state | Trino | physicalInputBytes | 196 | 1 | yes | table scan, catalog memory |
| sql/latest_state.sql | case2_sparse | Spark | inputBytes (stage sum) | 0 | 1 | yes | Scan ExistingRDD |
| sql/latest_state.sql | case2_sparse | Trino | physicalInputBytes | 174 | 1 | yes | table scan, catalog memory |
| sql/latest_state.sql | case3_duplicate | Spark | inputBytes (stage sum) | 0 | 1 | yes | Scan ExistingRDD |
| sql/latest_state.sql | case3_duplicate | Trino | physicalInputBytes | 261 | 1 | yes | table scan, catalog memory |
| sql/sparse_current_value.sql | case1_full_state | Spark | inputBytes (stage sum) | 0 | 3 | yes | Scan ExistingRDD |
| sql/sparse_current_value.sql | case1_full_state | Trino | physicalInputBytes | 196 | 3 | yes | table scan, catalog memory |
| sql/sparse_current_value.sql | case2_sparse | Spark | inputBytes (stage sum) | 0 | 3 | yes | Scan ExistingRDD |
| sql/sparse_current_value.sql | case2_sparse | Trino | physicalInputBytes | 174 | 3 | yes | table scan, catalog memory |
| sql/sparse_current_value.sql | case3_duplicate | Spark | inputBytes (stage sum) | 0 | 4 | yes | Scan ExistingRDD |
| sql/sparse_current_value.sql | case3_duplicate | Trino | physicalInputBytes | 261 | 4 | yes | table scan, catalog memory |
| sql/sparse_previous_value.sql | case1_full_state | Spark | inputBytes (stage sum) | 0 | 3 | yes | Scan ExistingRDD |
| sql/sparse_previous_value.sql | case1_full_state | Trino | physicalInputBytes | 196 | 3 | yes | table scan, catalog memory |
| sql/sparse_previous_value.sql | case2_sparse | Spark | inputBytes (stage sum) | 0 | 3 | yes | Scan ExistingRDD |
| sql/sparse_previous_value.sql | case2_sparse | Trino | physicalInputBytes | 174 | 3 | yes | table scan, catalog memory |
| sql/sparse_previous_value.sql | case3_duplicate | Spark | inputBytes (stage sum) | 0 | 4 | yes | Scan ExistingRDD |
| sql/sparse_previous_value.sql | case3_duplicate | Trino | physicalInputBytes | 261 | 4 | yes | table scan, catalog memory |

Files: `engine_scan_bytes.csv`.

## Engine documentation for LAST_VALUE with IGNORE NULLS, fetched by script

Fetched by `bench/engine_docs_fetch.py` on 2026-10-23T10:43:45Z. Each syntax line is the page's own text, copied by the script; the page's SHA-256 is in engine_docs_fetched.csv. "none" means no line on the page gives a LAST_VALUE signature. The section "Engine documentation for LAST_VALUE with IGNORE NULLS" holds the hand copy of 2026-10-20 and is unchanged. Text lines are the page's text split where the HTML breaks lines, so a sentence can span two lines.

| Engine | URL as served | Documented version | Syntax line on the page |
|---|---|---|---|
| DuckDB | https://duckdb.org/docs/2.0/sql/functions/window_functions | version selector shows "2.0 LTS" | `last_value(expr[ ORDER BY ordering][ IGNORE NULLS])` |
| DuckDB | https://duckdb.org/docs/1.4/sql/functions/window_functions | page reads "This page is for DuckDB 1.4 (LTS)." | `last_value(expr[ ORDER BY ordering][ IGNORE NULLS])` |
| Spark SQL | https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html | page title reads "Window Functions - Spark 4.2.0 Documentation" | `window_function [ nulls_option ] OVER` |
| Trino | https://trino.io/docs/current/functions/window.html | page title ends "Trino 484 Documentation" | `last_value(x) → [same as input]` |
| Trino | https://trino.io/docs/current/sql/select.html | page title ends "Trino 484 Documentation" | none |
| Athena | https://docs.aws.amazon.com/athena/latest/ug/engine-versions-reference-0003.html | page title reads "Athena engine version 3 - Amazon Athena" | none |
| Athena | https://docs.aws.amazon.com/athena/latest/ug/functions-env3.html | page title reads "Athena engine version 3 functions - Amazon Athena" | none |

- DuckDB, https://duckdb.org/docs/2.0/sql/functions/window_functions: redirect: https://duckdb.org/docs/lts/sql/functions/window_functions -> https://duckdb.org/docs/2.0/sql/functions/window_functions.html (meta refresh) ; https://duckdb.org/docs/2.0/sql/functions/window_functions.html -> https://duckdb.org/docs/2.0/sql/functions/window_functions (HTTP 308); lines mentioning LAST_VALUE or IGNORE NULLS: last_value(expr[ ORDER BY ordering][ IGNORE NULLS]) || last_value(column).
- DuckDB, https://duckdb.org/docs/1.4/sql/functions/window_functions: lines mentioning LAST_VALUE or IGNORE NULLS: last_value(expr[ ORDER BY ordering][ IGNORE NULLS]) || last_value(column).
- Spark SQL, https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html: lines mentioning LAST_VALUE or IGNORE NULLS: window_function [ nulls_option ] OVER || Note: Only LAG | LEAD | NTH_VALUE | FIRST_VALUE | LAST_VALUE can be used with IGNORE NULLS. || LAST_VALUE(v) IGNORE NULLS OVER w last_value.
- Trino, https://trino.io/docs/current/functions/window.html: lines mentioning LAST_VALUE or IGNORE NULLS: By default, null values are respected. If IGNORE NULLS is specified, all rows where || x is null are excluded from the calculation. If IGNORE NULLS is specified and x || last_value(x) → [same as input].
- Trino, https://trino.io/docs/current/sql/select.html: SELECT page searched for IGNORE NULLS.
- Athena, https://docs.aws.amazon.com/athena/latest/ug/engine-versions-reference-0003.html: lines mentioning LAST_VALUE or IGNORE NULLS: IGNORE NULLS cannot be used with non-value window functions || Cause: IGNORE NULLS can now be || first_value, last_value, nth_value,; links to https://trino.io/docs/current/functions/window.html#value-functions.
- Athena, https://docs.aws.amazon.com/athena/latest/ug/functions-env3.html: links to https://trino.io/docs/current/functions/window.html.

Files: `engine_docs_fetched.csv`.

## AWS scale run

Measured by `bench/aws/run.py` from `bench/aws/config.aws.json` on AWS (us-east-1), with the infrastructure in `infra/`. Synthetic data from the bench-full generator, seed 20261113, K=67, full model, N=1,000,000 entities x H=100 events = 100,000,000 events per design. Runner m8id.2xlarge; Amazon MSK 3.9.x.kraft, 3 x kafka.m7g.large brokers, replication factor 3; Amazon Managed Service for Apache Flink FLINK-1_20 at parallelism 4; Iceberg on S3 with the Glue Data Catalog; Athena engine version 3. Kafka 3.7.x, which the local pipeline benchmark used, is past MSK end of support, so the brokers run 3.9.x.kraft.

Managed Flink rejected parallelism above 4 for this account (CreateApplication: the account needs AWS verification for higher KPU limits), so both applications run at 4 with one KPU per slot plus the orchestration KPU.

Definitions are in `bench/aws/report.py`. Raw rows: `producer_runs.csv`, `topic_log_bytes.csv`, `msk_metrics.csv`, `flink_runs.csv`, `flink_checkpoints.csv`, `flink_job_states.csv`, `iceberg_snapshots.csv`, `iceberg_stats.csv`, `iceberg_history.csv`, `athena_runs.csv`, `equivalence.csv`, `costs_daily.csv`, `costs_usage_types.csv`.

### Equivalence before timing

| Dataset | Query | Designs | Rows left | Rows right | Differing rows | Equal |
|---|---|---|---|---|---|---|
| full | q1_one_hop_previous_single | A vs B | 100 | 100 | 0 | yes |
| full | q3_current_state_all | A vs B | 1000000 | 1000000 | 0 | yes |
| full | q3_current_state_all | B vs C | 1000000 | 1000000 | 0 | yes |
| full | q4_previous_value_all | A vs B | 1000000 | 1000000 | 0 | yes |

### Producer and Kafka

Producer latency is send() to acknowledgement for every event of the run, sent back to back with no pacing (acks=all, Kafka client 3.4.0 defaults otherwise), so it includes each record's wait in the producer's buffer. Log bytes come from describeLogDirs once every replica matched its leader. Replication bytes from CloudWatch sum ReplicationBytesInPerSec (and ...OutPerSec) over the three brokers, one-minute averages times 60, from the minute the run started to the minute after the log bytes settled; the derived column is one copy times (replication factor - 1).

| Design | Compression | Events | p50 ms | p95 ms | p99 ms | Max ms | Log bytes, one copy | Bytes per event | Log bytes, all replicas | Replication bytes in (CloudWatch) | Replication bytes out (CloudWatch) | Replication bytes (derived) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A | zstd | 100000000 | 13.672902 | 47.293248 | 165.64037 | 505.175579 | 74023632171 | 740.23632171 | 222070896513 | 143490988304.20193843055714 | 143493766485.54152615055687 | 148047264342 |
| A | none | 100000000 | 20.11992 | 4598.309395 | 4996.711522 | 6263.642761 | 192248378915 | 1922.48378915 | 576745136745 | 382596771585.01432491 | 382605371304.0271506 | 384496757830 |
| B | zstd | 100000000 | 12.701734 | 29.15702 | 100.952569 | 567.711554 | 64919671514 | 649.19671514 | 194759014542 | 128210366226.79109634 | 128210772332.9552472 | 129839343028 |
| B | none | 100000000 | 22.63433 | 2551.797033 | 3320.844921 | 3828.815631 | 98225607669 | 982.25607669 | 294676823007 | 194302916461.06319778 | 194307835950.97236636 | 196451215338 |

### Flink

Each application read its zstd topic from the earliest offset after the topic held every event. End-to-end throughput is events over the time from the job's switch to RUNNING (JobManager log) to the commit of the first Iceberg snapshot holding every event; sustained throughput is the records committed between the first and the second-to-last data commit over the time between them. Checkpoint durations are the checkpointDuration of every "Completed checkpoint" line of the job up to the checkpoint that made every event visible, nearest-rank.

| Design | Events | KPU | End-to-end events/s | Sustained events/s | Data commits | Checkpoints | Checkpoint p50 ms | Checkpoint p95 ms | Checkpoint max ms |
|---|---|---|---|---|---|---|---|---|---|
| A | 100000000 | 5 | 23164.6958915558... | 23528.7642938502... | 71 | 71 | 2736 | 4199 | 4566 |
| B | 100000000 | 5 | 49544.1934205311... | 51698.0769685873... | 33 | 33 | 2640 | 3479 | 4072 |

### Iceberg on S3

Expiration is expireSnapshots with retainLast 1; compaction is the Flink rewriteDataFiles action with Iceberg's defaults, both run from the runner against the Glue catalog. Bytes and file counts are listed from the table's metadata/ and data/ prefixes on S3. History unchanged compares, in Athena, the row count, the distinct (customer_id, event_sequence) count and a checksum over LAG(email) and the previous known email at every row with the same three values after the Flink commits.

| Table | State | Snapshots | Metadata bytes on S3 | Manifest files on S3 | Manifests in snapshot | Data files in snapshot | Data files on S3 | Data bytes on S3 | Rows | History unchanged |
|---|---|---|---|---|---|---|---|---|---|---|
| a | after commits | 71 | 6012556 | 71 | 71 | 321 | 321 | 119389089551 | 100000000 | yes |
| b | after commits | 33 | 1692987 | 33 | 33 | 164 | 164 | 60197255664 | 100000000 | yes |
| c | after commits | 1 | 47997 | 1 | 1 | 9 | 9 | 599647573 | 1000000 | yes |
| a | after expiration | 1 | 5604165 | 71 | 71 | 321 | 321 | 119389089551 | 100000000 | yes |
| b | after expiration | 1 | 1536526 | 33 | 33 | 164 | 164 | 60197255664 | 100000000 | yes |
| c | after expiration | 1 | 47997 | 1 | 1 | 9 | 9 | 599647573 | 1000000 | yes |
| a | after compaction | 2 | 9305023 | 143 | 72 | 1042 | 1363 | 238780190252 | 100000000 | yes |
| b | after compaction | 2 | 2793352 | 67 | 34 | 477 | 641 | 120397213717 | 100000000 | yes |
| c | after compaction | 2 | 109238 | 3 | 2 | 7 | 16 | 1199268777 | 1000000 | yes |
| a | after compaction and expiration | 1 | 7487600 | 72 | 72 | 1042 | 1363 | 238780190252 | 100000000 | yes |
| b | after compaction and expiration | 1 | 2128734 | 34 | 34 | 477 | 641 | 120397213717 | 100000000 | yes |
| c | after compaction and expiration | 1 | 85915 | 2 | 2 | 7 | 16 | 1199268777 | 1000000 | yes |

After compaction and expiration, a keeps 321 data files on S3 that its snapshot no longer references; b keeps 164 data files on S3 that its snapshot no longer references; c keeps 9 data files on S3 that its snapshot no longer references.
The files left over are the pre-compaction data files, by count and by bytes: a 321 files and 119389089551 bytes on S3 outside the snapshot, against 321 files and 119389089551 bytes after the Flink commits; b 164 files and 60197255664 bytes on S3 outside the snapshot, against 164 files and 60197255664 bytes after the Flink commits; c 9 files and 599647573 bytes on S3 outside the snapshot, against 9 files and 599647573 bytes after the Flink commits. The second expiration removed 71 of the manifest files of a (143 to 72), 33 of the manifest files of b (67 to 34), 1 of the manifest files of c (3 to 2), and no data files. It ran with expireOlderThan(now + 1 ms) and retainLast(1), which keeps only the newest snapshot, the compaction snapshot. Iceberg 1.10.1's incremental cleanup (core IncrementalFileCleanup, used when no snapshot ID or branch is named) deletes a data file only when the snapshot recorded on its DELETED manifest entry has expired; here that is the compaction snapshot, which was kept. Orphan-file removal was not run.

### Athena engine version 3

Bytes scanned is Athena's DataScannedInBytes, the same on every run of a query. p50 and p95 are nearest-rank over the timed runs of TotalExecutionTimeInMillis; the engine columns are EngineExecutionTimeInMillis. Each SELECT writes its result rows to the workgroup's S3 result location, and the runtime includes that write. Cost per query is the bytes scanned, rounded up to the next MB with a 10 MB minimum, at the price in `infra/prices.json`.

| Query | Design | Runs | Bytes scanned | p50 ms | p95 ms | p99 ms | Engine p50 ms | Engine p95 ms | Engine p99 ms | Cost per query USD |
|---|---|---|---|---|---|---|---|---|---|---|
| q1_one_hop_previous_single | A | 10 | 491273707 | 3067 | 4553 | 4553 | 2865 | 4345 | 4345 | 0.00223636627197265625 |
| q1_one_hop_previous_single | B | 10 | 524696325 | 2769 | 3961 | 3961 | 2624 | 3750 | 3750 | 0.00238895416259765625 |
| q2_three_changes_back_single | B | 10 | 524696325 | 2529 | 3851 | 3851 | 2339 | 3628 | 3628 | 0.00238895416259765625 |
| q3_current_state_all | A | 10 | 60224568330 | 30166 | 32484 | 32484 | 29980 | 32256 | 32256 | 0.27387142181396484375 |
| q3_current_state_all | B | 10 | 60175997153 | 29817 | 40658 | 40658 | 29648 | 40489 | 40489 | 0.27365207672119140625 |
| q3_current_state_all | C | 10 | 597471193 | 16836 | 19006 | 19006 | 16672 | 18777 | 18777 | 0.0027179718017578125 |
| q4_previous_value_all | A | 10 | 2559908559 | 5558 | 11470 | 11470 | 5410 | 11291 | 11291 | 0.0116443634033203125 |
| q4_previous_value_all | B | 10 | 1545859430 | 6696 | 9266 | 9266 | 6512 | 8894 | 8894 | 0.00703334808349609375 |

With 10 timed runs, the nearest-rank p95 and p99 are both the slowest run. Q1 and Q4 have no C row and Q2 has no A or C row: C holds no history and A holds one hop, so they cannot answer those questions from their own data.

### Cost Explorer, unblended cost by service

Cost Explorer UnblendedCost over 2026-10-23 to 2026-10-25 (end exclusive). Usage is the gross charge for what the run used; the account applied credits against it, shown separately. Cost Explorer still marked these days as estimated when read.

| Filter | Service | Usage USD | Credits USD | Net USD |
|---|---|---|---|---|
| account, no filter | AWS Cost Explorer | 0.02 | -0.02 | 0 |
| account, no filter | AWS Data Transfer | 0 | -6.1748887502 | -6.1748887502 |
| account, no filter | AWS Glue | 0 | 0 | 0 |
| account, no filter | AWS Key Management Service | 0 | 0 | 0 |
| account, no filter | Amazon Athena | 7.240275 | -7.240275 | 0 |
| account, no filter | Amazon Elastic Compute Cloud - Compute | 4.2559669235 | -4.2559669235 | 0 |
| account, no filter | Amazon Kinesis Analytics | 1.2131149558 | -1.2131149558 | 0 |
| account, no filter | Amazon Managed Streaming for Apache Kafka | 8.4636653464 | -4.9933880087 | 3.4702773377 |
| account, no filter | Amazon Simple Storage Service | 0.2587558 | -0.2587558 | 0 |
| account, no filter | Amazon Virtual Private Cloud | 0.1009308189 | -0.1009308189 | 0 |
| account, no filter | AmazonCloudWatch | 0.0015 | -0.0015 | 0 |
| account, no filter | EC2 - Other | 2.7484853644 | -0.0438739536 | 2.7046114108 |
| account, no filter | Total | 24.302694209 | -24.3026942107 | -0.0000000017 |

Cost Explorer books the credit for regional (cross-AZ) data transfer under AWS Data Transfer (-6.1748887502), while the usage it offsets sits under EC2 - Other (2.7046114108 for 270.46296149 GB) and Amazon Managed Streaming for Apache Kafka (3.4702773394 for 348.02591352 GB); those usage lines sum to 6.1748887502.

The same query filtered on the run tag returned no rows: the tag was not active as a cost allocation tag when Cost Explorer was read. In us-east-1 every usage line of these days is the run's (`costs_usage_types.csv`); the only lines from another region are Glue catalog requests and one S3 request in us-east-2.

## Note on q2_three_changes_back_single

q2_three_changes_back_single computes LAG(email, 3) over the entity's events that carry
email (`WHERE customer_id = '<entity>' AND email IS NOT NULL`, ordered by event_timestamp,
event_sequence), so it returns email as it was three such events back. In the full model
every event carries every attribute, so the result is the value three events back; the AWS
scale run uses the full model, so its q2 is three events back as well. In the sparse model
an event carries an attribute only when it changes, so the same query returns the value
three changes back. The description "value of email three changes back, single entity" in
the sections above, and in the description column of `latency.csv`, `resources.csv` and
`scaling_latency.csv`, therefore applies to the sparse model only. The query ID stays as it
is because the results files refer to it.

The row counts in `latency.csv` and `scaling_latency.csv` show the difference: design B's q2
returned 100 rows for C0000500 in the full model (one per event, H=100) and 5 in the sparse
model, and in the sweep over H it returned H rows in the full model (1, 10, 100, 1000) and
1, 2, 3 and 49 rows in the sparse model.
