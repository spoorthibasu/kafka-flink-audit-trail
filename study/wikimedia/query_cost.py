"""Step 7: cost of the previous value read from stored prior_state versus computed with LAG.

Four queries over the canonical Parquet file, each returning the entity, meta.dt, the
current revision ID and the previous rev_id, rev_dt, rev_size and rev_sha1:

  stored, one entity    prior_state__revision__* columns, WHERE on the entity
  LAG, one entity       LAG of the current columns in the article's form, same WHERE
  stored, all entities  prior_state__revision__* columns, every row
  LAG, all entities     LAG of the current columns, every row

The one-entity queries use the entity with the most events. Measurement follows
bench/resources.py: each timed run uses a fresh in-memory DuckDB, runs the query as
CREATE OR REPLACE TEMP TABLE ... AS <query> for the warm-up runs without the profiler,
then once with it. CPU time is the profiler's cpu_time, latency its latency, peak memory
system_peak_buffer_memory; bytes scanned are derived from parquet_metadata() by
resources.bytes_scanned. p50 and p95 are nearest-rank over the timed runs.
"""
import json
import os
import sys
from decimal import Decimal

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "..", "bench"))
import load  # noqa: E402
import resources  # noqa: E402

FIELDS = ("revision__rev_id", "revision__rev_dt", "revision__rev_size", "revision__rev_sha1")


def queries(entity):
    wiki, page = entity
    where = "wiki_id = '%s' AND page__page_id = %d" % (wiki.replace("'", "''"), page)
    order = ", ".join(load.ORDER_BY)
    w = "OVER (PARTITION BY %s ORDER BY %s)" % (", ".join(load.ENTITY), order)
    head = "wiki_id, page__page_id, meta__dt, revision__rev_id"
    stored = ", ".join("prior_state__%s AS previous_%s" % (f, f) for f in FIELDS)
    lag = ", ".join("LAG(%s) %s AS previous_%s" % (f, w, f) for f in FIELDS)
    return [
        ("one entity", "stored prior_state", "SELECT %s, %s FROM events WHERE %s ORDER BY %s" % (head, stored, where, order)),
        ("one entity", "LAG", "SELECT %s, %s FROM events WHERE %s ORDER BY %s" % (head, lag, where, order)),
        ("all entities", "stored prior_state", "SELECT %s, %s FROM events" % (head, stored)),
        ("all entities", "LAG", "SELECT %s, %s FROM events" % (head, lag)),
    ]


def pct(values, p):
    s = sorted(values)
    k = max(1, -(-p * len(s) // 100))
    return s[k - 1]


def measure(parquet, sql, runs, warmup, work):
    profile_path = os.path.join(work, "profile.json")
    ctas = "CREATE OR REPLACE TEMP TABLE bench_result AS " + sql
    samples = {"cpu_ms": [], "latency_ms": [], "peak_buffer_bytes": []}
    rows = None
    for _ in range(runs):
        con = duckdb.connect()
        con.execute("CREATE VIEW events AS SELECT * FROM read_parquet('%s')" % parquet)
        for _ in range(warmup):
            con.execute(ctas)
        con.execute("DROP TABLE IF EXISTS bench_result")
        con.execute("SET enable_profiling = 'json'")
        con.execute("SET profiling_output = '%s'" % profile_path)
        con.execute(ctas)
        con.execute("RESET enable_profiling")
        con.execute("RESET profiling_output")
        rows = con.execute("SELECT count(*) FROM bench_result").fetchone()[0]
        con.close()
        with open(profile_path) as fh:
            prof = json.load(fh, parse_float=Decimal)
        samples["cpu_ms"].append(Decimal(prof["cpu_time"]) * 1000)
        samples["latency_ms"].append(Decimal(prof["latency"]) * 1000)
        samples["peak_buffer_bytes"].append(int(prof["system_peak_buffer_memory"]))
    return samples, prof, rows


def query_cost(parquet, runs, warmup, work, log=print):
    con = duckdb.connect()
    entity = con.execute("SELECT wiki_id, page__page_id, count(*) AS n FROM read_parquet('%s') GROUP BY ALL "
                         "ORDER BY n DESC, wiki_id, page__page_id LIMIT 1" % parquet).fetchone()
    layout = {"events": resources.parquet_layout(con, parquet)}
    con.close()
    out = []
    for scope, method, sql in queries(entity[:2]):
        samples, prof, rows = measure(parquet, sql, runs, warmup, work)
        scan = resources.bytes_scanned(prof, layout, "events")
        out.append({"scope": scope, "method": method, "entity": "%s:%d" % entity[:2] if scope == "one entity" else "",
                    "entity_events": entity[2] if scope == "one entity" else "", "rows_returned": rows,
                    "runs": runs, "warmup_runs_per_run": warmup,
                    "latency_ms_p50": pct(samples["latency_ms"], 50), "latency_ms_p95": pct(samples["latency_ms"], 95),
                    "cpu_ms_p50": pct(samples["cpu_ms"], 50), "cpu_ms_p95": pct(samples["cpu_ms"], 95),
                    "peak_buffer_bytes_p50": pct(samples["peak_buffer_bytes"], 50),
                    "peak_buffer_bytes_p95": pct(samples["peak_buffer_bytes"], 95),
                    "bytes_scanned": scan["bytes_scanned"], "columns_read": scan["columns_read"],
                    "columns_in_file": scan["columns_in_file"], "row_groups_read": scan["row_groups_read"],
                    "row_groups_in_file": scan["row_groups_in_file"], "pushed_filters": scan["pushed_filters"],
                    "sql": sql})
        log("  %-12s %-18s cpu_ms p50=%s p95=%s latency_ms p50=%s scanned=%d" % (
            scope, method, out[-1]["cpu_ms_p50"], out[-1]["cpu_ms_p95"], out[-1]["latency_ms_p50"], scan["bytes_scanned"]))
    return out
