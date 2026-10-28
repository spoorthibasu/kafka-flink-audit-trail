#!/usr/bin/env python3
"""A narrow compatibility view: only the columns q1 and q4 read, with previous_email from LAG.

Follow-up to bench/compat_view.py, whose view exposes all 137 columns of the
previous-value table and computes LAG for all 67 attributes. This narrow view exposes
customer_id, event_timestamp, event_sequence, email and previous_email, with
previous_email as LAG(email) OVER (PARTITION BY customer_id ORDER BY event_timestamp,
event_sequence), named and typed as in the previous-value table.

Regenerates the main FULL dataset of bench/benchmark.py and writes designs A and B to
Parquet as it does; the sizes must equal storage.csv. For q1 and q4, the design A queries
of build_queries() that read previous_email, it checks that the narrow view and the
67-attribute view each return the same rows as the stored table, then times all three
in one run with bench/benchmark.py's time_query(), the protocol of latency.csv (the
config's warm-up runs and runs, nearest-rank p50 and p95).

Writes compat_view_narrow.csv, compat_view_narrow_environment.json and the "Compatibility
view, narrow" section of SUMMARY.md. No other results file is touched.

  python3 bench/compat_view_narrow.py --config bench/config.json --out results
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
import compat_view as cv  # noqa: E402  the 67-attribute view
from summary_sections import replace_section  # noqa: E402

SECTION_HEADING = "## Compatibility view, narrow: previous_email only"
MODEL = "full"
QUERIES = ("q1_one_hop_previous_single", "q4_previous_value_all")
NARROW_COLUMNS = ("customer_id", "event_timestamp", "event_sequence", "email", "previous_email")
NARROW_VIEW = ("CREATE VIEW a AS SELECT customer_id, event_timestamp, event_sequence, email, "
               "LAG(email) OVER (PARTITION BY customer_id ORDER BY event_timestamp, event_sequence) AS previous_email "
               "FROM b")


def connect(files, attrs, kind):
    con = duckdb.connect()
    con.execute("CREATE VIEW b AS SELECT * FROM read_parquet('%s')" % files["b"])
    if kind == "stored":
        con.execute("CREATE VIEW a AS SELECT * FROM read_parquet('%s')" % files["a"])
    elif kind == "wide":
        con.execute(cv.view_sql(attrs))
    else:
        con.execute(NARROW_VIEW)
    return con


def section(env, recs):
    cfg = env["config"]
    lines = [SECTION_HEADING, ""]
    w = lines.append
    w("Measured by `bench/compat_view_narrow.py` from `%s` on %s: the main FULL dataset of `bench/benchmark.py`, "
      "synthetic, seed %d, N=%d x H=%d, K=%d, regenerated and written to the same Parquet files (sizes equal "
      "`storage.csv`). Machine: %s, %s logical CPUs. DuckDB %s, threads=%s." % (
          env["config_file"], env["started_utc"], cfg["seed"], cfg["n_entities"], cfg["h_events"], cfg["k_attributes"],
          env["machine"].get("cpu", env["machine"]["platform"]), env["machine"]["cpu_count"], env["duckdb"],
          env["duckdb_threads"]))
    w("")
    w("The narrow view, named `a` over the audit trail (design B), exposes only the columns q1 and q4 read: %s, with "
      "previous_email as LAG(email) OVER (PARTITION BY customer_id ORDER BY event_timestamp, event_sequence). Its "
      "column names and types equal the previous-value table's for those columns: %s. The 67-attribute view is the "
      "one in the section above, measured again in this run so that all three columns of times come from one session."
      % (", ".join(NARROW_COLUMNS), env["narrow_types_equal"]))
    w("")
    w("- Same rows: q1 and q4, SQL unchanged, return the same rows from the narrow view and from the 67-attribute "
      "view as from the stored table (sorted row sets).")
    w("- p50 and p95 ms: `bench/benchmark.py` time_query(), the protocol of `latency.csv`: CREATE OR REPLACE TEMP "
      "TABLE AS the query, %d warm-up runs and %d timed runs on one connection, nearest-rank." % (
          cfg["queries"]["warmup_runs"], cfg["queries"]["runs"]))
    w("")
    w(b.md_table(["Query", "Rows", "Same rows", "Stored table p50 ms", "67-attribute view p50 ms", "Narrow view p50 ms",
                  "Stored table p95 ms", "67-attribute view p95 ms", "Narrow view p95 ms"],
                 [[r["query"], b.fmt_bytes(r["rows"]), r["same_rows"], r["stored_p50_ms"], r["wide_view_p50_ms"],
                   r["narrow_view_p50_ms"], r["stored_p95_ms"], r["wide_view_p95_ms"], r["narrow_view_p95_ms"]]
                  for r in recs]))
    w("")
    w("Files: `compat_view_narrow.csv`, `compat_view_narrow_environment.json`.")
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
           "duckdb_threads": duckdb.connect().execute("SELECT current_setting('threads')").fetchone()[0],
           "narrow_view_sql": NARROW_VIEW}
    attrs = b.attributes(cfg["k_attributes"])
    n, h = cfg["n_entities"], cfg["h_events"]
    comp = cfg["storage"]["parquet_compressions"][0]
    rg = cfg["storage"]["parquet_row_group_size"]
    storage = {r["design"]: int(r["parquet_bytes"]) for r in csv.DictReader(open(os.path.join(args.out, "storage.csv")))
               if r["model"] == MODEL and int(r["n_entities"]) == n and int(r["h_events"]) == h and r["parquet_compression"] == comp}
    work = tempfile.mkdtemp(prefix="audit-compat-narrow-")
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

        cons = {k: connect(files, attrs, k) for k in ("stored", "wide", "narrow")}
        stored_types = {r[0]: r[1] for r in cons["stored"].execute("DESCRIBE a").fetchall()}
        narrow_desc = [(r[0], r[1]) for r in cons["narrow"].execute("DESCRIBE a").fetchall()]
        env["narrow_types_equal"] = "yes" if (tuple(c for c, _ in narrow_desc) == NARROW_COLUMNS
                                              and all(stored_types[c] == t for c, t in narrow_desc)) else "no"
        if env["narrow_types_equal"] != "yes":
            raise RuntimeError("narrow view columns differ from the stored table: %s" % narrow_desc)

        entity = "C%07d" % (n // 2)
        sql_by = {qid: sql for qid, _, _, d, sql, _ in b.build_queries(attrs, MODEL, entity) if d == "A" and sql}
        for qid in QUERIES:
            sql = sql_by[qid]
            want = cv.rows_of(cons["stored"], sql)
            for k in ("wide", "narrow"):
                if cv.rows_of(cons[k], sql) != want:
                    raise RuntimeError("%s returns different rows from the %s view" % (qid, k))
            m = {k: b.time_query(cons[k], sql, cfg["queries"]["runs"], cfg["queries"]["warmup_runs"],
                                 os.path.join(work, "prof.json"), os.path.join(work, "explain.txt"))
                 for k in ("stored", "wide", "narrow")}
            recs.append({"model": MODEL, "query": qid, "design": "A", "entity": entity if qid.endswith("_single") else "",
                         "rows": m["stored"]["rows_returned"], "same_rows": "yes",
                         "runs": m["stored"]["runs"], "warmup_runs": m["stored"]["warmup_runs"],
                         "stored_p50_ms": m["stored"]["p50_ms"], "wide_view_p50_ms": m["wide"]["p50_ms"],
                         "narrow_view_p50_ms": m["narrow"]["p50_ms"],
                         "stored_p95_ms": m["stored"]["p95_ms"], "wide_view_p95_ms": m["wide"]["p95_ms"],
                         "narrow_view_p95_ms": m["narrow"]["p95_ms"],
                         "stored_p99_ms": m["stored"]["p99_ms"], "wide_view_p99_ms": m["wide"]["p99_ms"],
                         "narrow_view_p99_ms": m["narrow"]["p99_ms"]})
            log("%-28s same rows; p50 stored %8.3f, 67-attribute view %8.3f, narrow view %8.3f ms" % (
                qid, m["stored"]["p50_ms"], m["wide"]["p50_ms"], m["narrow"]["p50_ms"]))
        for c in cons.values():
            c.close()
    finally:
        shutil.rmtree(work, ignore_errors=True)

    env["wall_seconds"] = round(time.perf_counter() - t_start, 1)
    env["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    b.write_csv(os.path.join(args.out, "compat_view_narrow.csv"), recs, list(recs[0].keys()))
    with open(os.path.join(args.out, "compat_view_narrow_environment.json"), "w") as fh:
        json.dump(env, fh, indent=2)
    replace_section(os.path.join(args.out, "SUMMARY.md"), SECTION_HEADING, section(env, recs))
    log("compat_view_narrow.csv and SUMMARY.md section written")


if __name__ == "__main__":
    main()
