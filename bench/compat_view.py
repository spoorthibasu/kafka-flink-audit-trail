#!/usr/bin/env python3
"""Migration with a compatibility view: the previous-value table's columns computed with LAG.

Regenerates the main FULL dataset of bench/benchmark.py from the same config and seed and
writes designs A and B to Parquet as it does (the first compression, the same row-group
size); the sizes must equal storage.csv. Over design B (the audit trail) it defines a view
named `a` that exposes every column of the previous-value table, in the same order and
with the same names and types: each attribute x as stored, and previous_x as LAG(x) OVER
(PARTITION BY customer_id ORDER BY event_timestamp, event_sequence).

Checks, before timing:
  schema   DESCRIBE of the view equals DESCRIBE of the stored table: names, types, order
  table    every row of the view equals a row of the stored table and the reverse
           (EXCEPT ALL both ways, so repeated rows count)
  queries  every design A query of build_queries() for the FULL model, unchanged, returns
           the same rows from the view as from the stored table (sorted row sets)

Latency: bench/benchmark.py's time_query(), the protocol of latency.csv: CREATE OR
REPLACE TEMP TABLE AS <query>, the config's warm-up runs and runs, nearest-rank p50 and
p95, once with `a` the stored Parquet file and once with `a` the view.

Writes compat_view.csv, compat_view_environment.json and the "Compatibility view" section
of SUMMARY.md. No other results file is touched.

  python3 bench/compat_view.py --config bench/config.json --out results
"""
import argparse
import csv
import json
import os
import shutil
import sys
import tempfile
import time

import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import benchmark as b  # noqa: E402  same generator, Parquet writer, queries and timing
from summary_sections import replace_section  # noqa: E402

SECTION_HEADING = "## Compatibility view: previous-value columns from LAG"
MODEL = "full"


def view_sql(attrs):
    cols = []
    for x, _ in attrs:
        cols.append(x)
        cols.append("LAG(%s) OVER w AS previous_%s" % (x, x))
    return ("CREATE VIEW a AS SELECT customer_id, event_timestamp, event_sequence, %s FROM b "
            "WINDOW w AS (PARTITION BY customer_id ORDER BY event_timestamp, event_sequence)" % ", ".join(cols))


def connect(files, attrs, compat):
    con = duckdb.connect()
    con.execute("CREATE VIEW b AS SELECT * FROM read_parquet('%s')" % files["b"])
    if compat:
        con.execute(view_sql(attrs))
    else:
        con.execute("CREATE VIEW a AS SELECT * FROM read_parquet('%s')" % files["a"])
    return con


def rows_of(con, sql):
    return sorted(con.execute(sql).fetchall(), key=lambda r: tuple("\x00" if v is None else str(v) for v in r))


def section(env, recs):
    cfg = env["config"]
    lines = [SECTION_HEADING, ""]
    w = lines.append
    w("Measured by `bench/compat_view.py` from `%s` on %s: the main FULL dataset of `bench/benchmark.py`, synthetic, "
      "seed %d, N=%d entities x H=%d events, K=%d, regenerated and written to the same Parquet files (sizes equal "
      "`storage.csv`). Machine: %s, %s logical CPUs. DuckDB %s, threads=%s." % (
          env["config_file"], env["started_utc"], cfg["seed"], cfg["n_entities"], cfg["h_events"], cfg["k_attributes"],
          env["machine"].get("cpu", env["machine"]["platform"]), env["machine"]["cpu_count"], env["duckdb"],
          env["duckdb_threads"]))
    w("")
    w("A view named `a` over the audit trail (design B) exposes every column of the previous-value table in the same "
      "order, with the same names and types: each attribute x as stored and previous_x as LAG(x) OVER (PARTITION BY "
      "customer_id ORDER BY event_timestamp, event_sequence). The benchmark's design A queries run against it with "
      "their SQL unchanged.")
    w("")
    w("- Schema: the view's DESCRIBE equals the stored table's (%d columns, names, types and order): %s." % (
        env["columns"], env["schema_equal"]))
    w("- Table: rows of the stored table not in the view %d, rows of the view not in the stored table %d, of %s rows "
      "each (EXCEPT ALL both ways)." % (env["stored_minus_view"], env["view_minus_stored"], b.fmt_bytes(env["rows"])))
    w("- Queries: every design A query returned the same rows from the view as from the stored table (column "
      "Same rows below).")
    w("- p50 and p95 ms: `bench/benchmark.py` time_query(), the protocol of `latency.csv`: CREATE OR REPLACE TEMP "
      "TABLE AS the query, %d warm-up runs and %d timed runs on one connection, nearest-rank." % (
          cfg["queries"]["warmup_runs"], cfg["queries"]["runs"]))
    w("")
    w(b.md_table(["Query", "Reads previous_ columns", "Rows", "Same rows", "Stored table p50 ms", "View p50 ms",
                  "Stored table p95 ms", "View p95 ms"],
                 [[r["query"], r["reads_previous_columns"], b.fmt_bytes(r["rows"]), r["same_rows"], r["stored_p50_ms"],
                   r["view_p50_ms"], r["stored_p95_ms"], r["view_p95_ms"]] for r in recs]))
    w("")
    w("Files: `compat_view.csv`, `compat_view_environment.json`.")
    w("")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="bench/config.json")
    ap.add_argument("--out", default="results")
    args = ap.parse_args()
    cfg = json.load(open(args.config))
    t_start = time.perf_counter()

    def log(msg):
        print("[%7.1fs] %s" % (time.perf_counter() - t_start, msg), flush=True)

    env = {"started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "config_file": args.config, "config": cfg,
           "machine": b.machine_info(), "duckdb": duckdb.__version__,
           "duckdb_threads": duckdb.connect().execute("SELECT current_setting('threads')").fetchone()[0]}
    attrs = b.attributes(cfg["k_attributes"])
    n, h = cfg["n_entities"], cfg["h_events"]
    comp = cfg["storage"]["parquet_compressions"][0]
    rg = cfg["storage"]["parquet_row_group_size"]
    storage = {r["design"]: int(r["parquet_bytes"]) for r in csv.DictReader(open(os.path.join(args.out, "storage.csv")))
               if r["model"] == MODEL and int(r["n_entities"]) == n and int(r["h_events"]) == h and r["parquet_compression"] == comp}
    work = tempfile.mkdtemp(prefix="audit-compat-view-")
    recs = []
    try:
        b.generate(cfg, MODEL, n, h, attrs, work, None)
        con = duckdb.connect()
        files = {}
        for design, table, with_prev in (("A", "a", True), ("B", "b", False)):
            b.load_csv(con, table, os.path.join(work, "%s.csv" % table), b.columns_sql(attrs, with_prev))
            files[table] = os.path.join(work, "%s_%s.parquet" % (table, comp))
            size = b.write_parquet(con, table, files[table], comp, rg)
            if size != storage[design]:
                raise RuntimeError("%s: %d bytes, storage.csv has %d" % (design, size, storage[design]))
        con.close()
        log("Parquet A and B written; sizes equal storage.csv")

        stored, view = connect(files, attrs, False), connect(files, attrs, True)
        d_stored = stored.execute("DESCRIBE a").fetchall()
        d_view = view.execute("DESCRIBE a").fetchall()
        env["columns"] = len(d_stored)
        env["schema_equal"] = "yes" if [r[:2] for r in d_stored] == [r[:2] for r in d_view] else "no"
        both = duckdb.connect()
        both.execute("CREATE VIEW b AS SELECT * FROM read_parquet('%s')" % files["b"])
        both.execute(view_sql(attrs).replace("CREATE VIEW a AS", "CREATE VIEW a_view AS"))
        both.execute("CREATE VIEW a_stored AS SELECT * FROM read_parquet('%s')" % files["a"])
        env["rows"] = both.execute("SELECT count(*) FROM a_stored").fetchone()[0]
        env["stored_minus_view"] = both.execute("SELECT count(*) FROM (SELECT * FROM a_stored EXCEPT ALL SELECT * FROM a_view)").fetchone()[0]
        env["view_minus_stored"] = both.execute("SELECT count(*) FROM (SELECT * FROM a_view EXCEPT ALL SELECT * FROM a_stored)").fetchone()[0]
        both.close()
        log("schema equal %s; table differences %d and %d" % (env["schema_equal"], env["stored_minus_view"], env["view_minus_stored"]))
        if env["schema_equal"] != "yes" or env["stored_minus_view"] or env["view_minus_stored"]:
            raise RuntimeError("the view does not reproduce the stored table")

        entity = "C%07d" % (n // 2)
        queries = [(qid, sql) for qid, _, _, d, sql, _ in b.build_queries(attrs, MODEL, entity) if d == "A" and sql]
        for qid, sql in queries:
            same = rows_of(stored, sql) == rows_of(view, sql)
            if not same:
                raise RuntimeError("%s returns different rows from the view" % qid)
            m_s = b.time_query(stored, sql, cfg["queries"]["runs"], cfg["queries"]["warmup_runs"],
                               os.path.join(work, "prof.json"), os.path.join(work, "explain.txt"))
            m_v = b.time_query(view, sql, cfg["queries"]["runs"], cfg["queries"]["warmup_runs"],
                               os.path.join(work, "prof.json"), os.path.join(work, "explain.txt"))
            recs.append({"model": MODEL, "query": qid, "design": "A", "entity": entity if qid.endswith("_single") else "",
                         "reads_previous_columns": "yes" if "previous_" in sql else "no", "rows": m_s["rows_returned"],
                         "same_rows": "yes", "runs": m_s["runs"], "warmup_runs": m_s["warmup_runs"],
                         "stored_p50_ms": m_s["p50_ms"], "view_p50_ms": m_v["p50_ms"],
                         "stored_p95_ms": m_s["p95_ms"], "view_p95_ms": m_v["p95_ms"],
                         "stored_p99_ms": m_s["p99_ms"], "view_p99_ms": m_v["p99_ms"],
                         "stored_scan_rows_out": m_s["scan_rows_out"], "view_scan_rows_out": m_v["scan_rows_out"]})
            log("%-28s same rows; stored p50 %8.3f ms, view p50 %8.3f ms" % (qid, m_s["p50_ms"], m_v["p50_ms"]))
        stored.close()
        view.close()
    finally:
        shutil.rmtree(work, ignore_errors=True)

    env["view_sql"] = view_sql([(x, t) for x, t in attrs[:2]]) + " ... (same pattern for all %d attributes)" % len(attrs)
    env["wall_seconds"] = round(time.perf_counter() - t_start, 1)
    env["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    b.write_csv(os.path.join(args.out, "compat_view.csv"), recs, list(recs[0].keys()))
    with open(os.path.join(args.out, "compat_view_environment.json"), "w") as fh:
        json.dump(env, fh, indent=2)
    replace_section(os.path.join(args.out, "SUMMARY.md"), SECTION_HEADING, section(env, recs))
    log("compat_view.csv and SUMMARY.md section written")


if __name__ == "__main__":
    main()
