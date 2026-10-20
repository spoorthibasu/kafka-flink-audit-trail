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
