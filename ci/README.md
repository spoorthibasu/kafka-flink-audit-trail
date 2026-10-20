# Cross-engine SQL validation

`run_sql_matrix.py` runs every file under `sql/` on DuckDB, Spark (local pyspark) and
Trino (a Docker container) against the golden cases under `golden/`, and compares the
rows each engine returns with the expected CSV for that query, as sorted row sets.

Each SQL file names the engines it is written for in a `-- engines:` header. The
script exits non-zero when a file fails or returns different rows on an engine it
claims. On an engine it does not claim, the outcome is recorded in the matrix and does
not fail the run. Athena is listed in the headers but has no local runtime, so it is
not part of the matrix.

Golden cases, all for customer 98765 with address null throughout:

1. `case1_full_state`: every event carries email and phone.
   Rows (ts, seq, email, phone): (1000, 1, alice@gmail.com, 555-397),
   (2000, 2, alice@yahoo.com, 555-397), (3000, 3, alice@newjob.com, 555-493).
   Expected previous_email: null, alice@gmail.com, alice@yahoo.com.
   Expected previous_phone: null, 555-397, 555-397.
2. `case2_sparse`: row 2 is phone-only, so email is null there. The expected files
   cover both the CURRENT ROW frame (current value) and the 1 PRECEDING frame
   (previous known value).
3. `case3_duplicate`: event 2 appears twice. `dedup.sql` returns it once; the window
   queries without dedup return four rows, which the expected files record.

```bash
pip install -r ci/requirements.txt
docker run -d --name kfat-trino -p 8080:8080 trinodb/trino:483
python3 ci/run_sql_matrix.py --engines duckdb,spark,trino --out ci/matrix.md
```

`make sql-matrix` runs the same command. The matrix the last run produced is written
to `ci/matrix.md`.
