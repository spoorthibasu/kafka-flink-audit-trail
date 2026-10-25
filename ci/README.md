# Cross-engine SQL validation

`run_sql_matrix.py` runs every file under `sql/` on DuckDB, Spark (local pyspark) and
Trino (a Docker container) against the golden cases under `golden/`, and compares the
rows each engine returns with the expected CSV for that query, as sorted row sets.

Each SQL file names the engines it is written for in a `-- engines:` header. The
script exits non-zero when a file fails or returns different rows on an engine it
claims. On an engine it does not claim, the outcome is recorded in the matrix and does
not fail the run. Athena is listed in the headers but has no local runtime, so it is
not part of the matrix.

Golden cases, with address null throughout. Cases 1 to 3 are for customer 98765:

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
4. `case4_full_state_nullable`: full-state events for two customers, interleaved in
   time, for `change_aware_previous_value.sql`. Customer 98765 starts with an email:
   alice@gmail.com for three events, null for two, alice@newjob.com for two,
   alice@yahoo.com for two, then alice@gmail.com again. Customer 24680 starts with a
   null email for two events, then bob@gmail.com for two, bob@work.com for two, null,
   and bob@home.com.
   Expected email_before_last_change for 98765: null for rows 1 to 3, alice@gmail.com
   for rows 4 and 5, null for rows 6 and 7 (the history alice@gmail.com, null,
   alice@newjob.com), alice@newjob.com for rows 8 and 9, alice@yahoo.com for row 10.
   For 24680: null for rows 1 to 4, bob@gmail.com for rows 5 and 6, bob@work.com for
   row 7, null for row 8. `lag_history.sql` and the sparse queries read customer 98765
   only; `latest_state.sql` and `dedup.sql` return both customers.

```bash
pip install -r ci/requirements.txt
docker run -d --name kfat-trino -p 8080:8080 trinodb/trino:483
python3 ci/run_sql_matrix.py --engines duckdb,spark,trino --out ci/matrix.md
```

`make sql-matrix` runs the same command. The matrix the last run produced is written
to `ci/matrix.md`.
