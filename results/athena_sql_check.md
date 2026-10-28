# sql/ files on Athena engine version 3

Run by `bench/aws/athena_sql_check.py` from 2026-10-28T04:55:23Z to 2026-10-28T04:56:33Z in us-east-1. Athena workgroup `athena-sql-check` with engine version selected "Athena engine version 3" (effective "Athena engine version 3"); every query reported EffectiveEngineVersion "Athena engine version 3". Glue Data Catalog database `athena_sql_check`, external table `customer_profile_changes`; one temporary S3 bucket held the inputs and the query results. Read back before the queries, each of the three carried the tags run=athena-sql-check, project=audit-trail-bench. All three were deleted after the run: `athena_sql_check_teardown.txt`, leftovers: none.

Inputs: the four CI golden cases, `ci/golden/<case>/input.csv`, each written to one Parquet file by DuckDB 1.4.5 with the column types of `ci/run_sql_matrix.py` (customer_id, email, phone and address string; event_timestamp and event_sequence bigint) and read through `customer_profile_changes`, which pointed at one case at a time. Before each case's queries, `SELECT * FROM customer_profile_changes` returned the input rows exactly: 4 of 4 cases.

Queries: the 6 standard-form files under `sql/`; the two files under `sql/duckdb/` were not run. Each query string was the file's full text, byte for byte, with its comments and trailing semicolon: the SHA-256 of the query string equals that of the file in 24 of 24 queries. `athena_sql_check` was each query's default database, so the files' unqualified `customer_profile_changes` resolves as written. Result reuse was off. The comparison is `ci/run_sql_matrix.py`'s: lower-cased column names, then the rows as sorted row sets, against `ci/golden/<case>/expected/<file>.csv`. Rows matched counts the returned rows that are expected rows, as a multiset.

Athena accepted every query string as sent. The query text it records for an execution (GetQueryExecution, field Query): for the file queries, the text sent without its final semicolon and newline (24 of 24); for the input checks, which end without a semicolon, identical (4 of 4).

Result: 6 of 6 files returned exactly the expected rows in all four cases, 24 of 24 queries.

## Per file, as written

| SQL file | Query SHA-256 (first 16 hex) | Cases passed | Rows matched of expected | Query text Athena recorded | Differences |
|---|---|---|---|---|---|
| `sql/change_aware_previous_value.sql` | `e570f88f0b3aea43` | 4 of 4 | 28 of 28 | 4 of 4 text sent without its final semicolon and newline | none |
| `sql/dedup.sql` | `c4e851a72d1879d0` | 4 of 4 | 27 of 27 | 4 of 4 text sent without its final semicolon and newline | none |
| `sql/lag_history.sql` | `1ae8d1da29d19737` | 4 of 4 | 20 of 20 | 4 of 4 text sent without its final semicolon and newline | none |
| `sql/latest_state.sql` | `2fedc5f13c1ec690` | 4 of 4 | 5 of 5 | 4 of 4 text sent without its final semicolon and newline | none |
| `sql/sparse_current_value.sql` | `3ce2dc24b2f08a63` | 4 of 4 | 20 of 20 | 4 of 4 text sent without its final semicolon and newline | none |
| `sql/sparse_previous_value.sql` | `0be61324a529bbdd` | 4 of 4 | 20 of 20 | 4 of 4 text sent without its final semicolon and newline | none |

## Per query

| Query | Case | Form | Engine version | Rows expected | Rows returned | Rows matched | Result | Bytes scanned | Difference |
|---|---|---|---|---|---|---|---|---|---|
| `SELECT * FROM customer_profile_changes` | case1_full_state | input | Athena engine version 3 | 3 | 3 | 3 | pass | 267 | none |
| sql/change_aware_previous_value.sql | case1_full_state | file as written | Athena engine version 3 | 3 | 3 | 3 | pass | 199 | none |
| sql/dedup.sql | case1_full_state | file as written | Athena engine version 3 | 3 | 3 | 3 | pass | 267 | none |
| sql/lag_history.sql | case1_full_state | file as written | Athena engine version 3 | 3 | 3 | 3 | pass | 267 | none |
| sql/latest_state.sql | case1_full_state | file as written | Athena engine version 3 | 1 | 1 | 1 | pass | 267 | none |
| sql/sparse_current_value.sql | case1_full_state | file as written | Athena engine version 3 | 3 | 3 | 3 | pass | 267 | none |
| sql/sparse_previous_value.sql | case1_full_state | file as written | Athena engine version 3 | 3 | 3 | 3 | pass | 267 | none |
| `SELECT * FROM customer_profile_changes` | case2_sparse | input | Athena engine version 3 | 3 | 3 | 3 | pass | 269 | none |
| sql/change_aware_previous_value.sql | case2_sparse | file as written | Athena engine version 3 | 3 | 3 | 3 | pass | 192 | none |
| sql/dedup.sql | case2_sparse | file as written | Athena engine version 3 | 3 | 3 | 3 | pass | 269 | none |
| sql/lag_history.sql | case2_sparse | file as written | Athena engine version 3 | 3 | 3 | 3 | pass | 269 | none |
| sql/latest_state.sql | case2_sparse | file as written | Athena engine version 3 | 1 | 1 | 1 | pass | 269 | none |
| sql/sparse_current_value.sql | case2_sparse | file as written | Athena engine version 3 | 3 | 3 | 3 | pass | 269 | none |
| sql/sparse_previous_value.sql | case2_sparse | file as written | Athena engine version 3 | 3 | 3 | 3 | pass | 269 | none |
| `SELECT * FROM customer_profile_changes` | case3_duplicate | input | Athena engine version 3 | 4 | 4 | 4 | pass | 267 | none |
| sql/change_aware_previous_value.sql | case3_duplicate | file as written | Athena engine version 3 | 4 | 4 | 4 | pass | 199 | none |
| sql/dedup.sql | case3_duplicate | file as written | Athena engine version 3 | 3 | 3 | 3 | pass | 267 | none |
| sql/lag_history.sql | case3_duplicate | file as written | Athena engine version 3 | 4 | 4 | 4 | pass | 267 | none |
| sql/latest_state.sql | case3_duplicate | file as written | Athena engine version 3 | 1 | 1 | 1 | pass | 267 | none |
| sql/sparse_current_value.sql | case3_duplicate | file as written | Athena engine version 3 | 4 | 4 | 4 | pass | 267 | none |
| sql/sparse_previous_value.sql | case3_duplicate | file as written | Athena engine version 3 | 4 | 4 | 4 | pass | 267 | none |
| `SELECT * FROM customer_profile_changes` | case4_full_state_nullable | input | Athena engine version 3 | 18 | 18 | 18 | pass | 534 | none |
| sql/change_aware_previous_value.sql | case4_full_state_nullable | file as written | Athena engine version 3 | 18 | 18 | 18 | pass | 425 | none |
| sql/dedup.sql | case4_full_state_nullable | file as written | Athena engine version 3 | 18 | 18 | 18 | pass | 534 | none |
| sql/lag_history.sql | case4_full_state_nullable | file as written | Athena engine version 3 | 10 | 10 | 10 | pass | 663 | none |
| sql/latest_state.sql | case4_full_state_nullable | file as written | Athena engine version 3 | 2 | 2 | 2 | pass | 534 | none |
| sql/sparse_current_value.sql | case4_full_state_nullable | file as written | Athena engine version 3 | 10 | 10 | 10 | pass | 663 | none |
| sql/sparse_previous_value.sql | case4_full_state_nullable | file as written | Athena engine version 3 | 10 | 10 | 10 | pass | 663 | none |

## Cost

Athena bills each query's bytes scanned rounded up to the next MB, with a 10 MB minimum, at 5 USD per TB (`infra/prices.json`; MB = 2^20 and TB = 2^40 bytes, as in `bench/aws/athena.py`). The 28 queries scanned 9424 bytes in all and were billed 293601280 bytes: 0.00133514404296875 USD. S3 and Glue request charges are not in this figure; they were not measured.

Files: `athena_sql_check.csv` (one row per query, with its query execution ID), `athena_sql_check_environment.json`, `athena_sql_check_teardown.txt`.
