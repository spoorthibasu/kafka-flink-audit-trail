#!/usr/bin/env python3
"""Each engine's own input-byte counter for the sql/ files, in the sql-matrix setup.

The setup is ci/run_sql_matrix.py's: each golden case's input.csv becomes the table
customer_profile_changes, then every sql/ file an engine claims runs against it. Spark
holds the table as a temporary view over a DataFrame built from Python rows
(SparkSession local[1]); Trino holds it in the memory connector. Neither reads a file.

Per (engine, file, case) this records the rows returned, whether they match the golden
expected file, and the engine's own counter:

  Trino   physicalInputBytes from the client's final query statistics (the value
          Trino's coordinator reports for the query; the sum over its stages)
  Spark   inputBytes and inputRecords summed over the stages of every job the query
          ran, from the SparkContext's status store (the task metric
          inputMetrics.bytesRead aggregated per stage)

Each query runs twice; the script stops if the two counters differ. Writes
engine_scan_bytes.csv and the "Engine scan bytes" section of SUMMARY.md.

  python3 ci/engine_scan_bytes.py --engines spark,trino --out results
"""
import argparse
import csv
import os
import re
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "ci"))
sys.path.insert(0, os.path.join(ROOT, "bench"))
import run_sql_matrix as matrix  # noqa: E402  same tables, files and golden cases
from benchmark import md_table  # noqa: E402
from summary_sections import replace_section  # noqa: E402

SECTION_HEADING = "## Engine scan bytes in the sql-matrix setup"
REPEATS = 2


def trino_run(engine, sql):
    cur = engine.conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    header = [d[0] for d in cur.description]
    st = cur.stats
    return header, rows, {"counter": st["physicalInputBytes"], "query_id": st["queryId"],
                          "processed_rows": st["processedRows"], "detail": ""}


def spark_run(engine, sql, group):
    sc = engine.spark.sparkContext
    sc.setJobGroup(group, group)
    df = engine.spark.sql(sql)
    rows = [tuple(r) for r in df.collect()]
    store = sc._jsc.sc().statusStore()
    in_bytes = in_records = 0
    stages = skipped = 0
    for job_id in sc.statusTracker().getJobIdsForGroup(group):
        for stage_id in sc.statusTracker().getJobInfo(job_id).stageIds:
            try:
                sd = store.lastStageAttempt(stage_id)
            except Exception:  # a stage the job skipped has no attempt
                skipped += 1
                continue
            stages += 1
            in_bytes += sd.inputBytes()
            in_records += sd.inputRecords()
    plan = df._jdf.queryExecution().executedPlan().toString()
    scans = sorted(set(m.group(1).strip() for m in re.finditer(r"(?:\+- |^)(?:\*\(\d+\) )?(\w*Scan[\w ]*?)(?:\[|$)", plan, re.M)))
    return df.columns, rows, {"counter": in_bytes, "input_records": in_records, "stages": stages,
                              "skipped_stages": skipped, "detail": "; ".join(scans)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engines", default="spark,trino")
    ap.add_argument("--trino-host", default="localhost")
    ap.add_argument("--trino-port", type=int, default=8080)
    ap.add_argument("--out", default="results")
    args = ap.parse_args()
    engines = [e.strip().lower() for e in args.engines.split(",") if e.strip()]
    files = matrix.sql_files()
    cases = matrix.golden_cases()
    started = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    rows_out, versions = [], {}
    for name in engines:
        engine = matrix.make_engine(name, args)
        versions[name] = engine.version
        print("== %s %s" % (matrix.ENGINE_LABEL[name], engine.version), flush=True)
        for case in cases:
            engine.load(case["rows"])
            for f in files:
                if name not in f["claimed"]:
                    continue
                counters = []
                for rep in range(REPEATS):
                    if name == "trino":
                        header, rows, stats = trino_run(engine, f["sql"])
                    else:
                        header, rows, stats = spark_run(engine, f["sql"], "%s-%s-%d" % (f["name"], case["name"], rep))
                    counters.append(stats["counter"])
                if len(set(counters)) != 1:
                    raise RuntimeError("%s %s %s: counter changed between runs %s" % (name, f["path"], case["name"], counters))
                exp = case["expected"].get(f["name"])
                match = "no expected file" if exp is None else (
                    "yes" if matrix.normalize(header, rows) == matrix.normalize(*exp) else "no")
                rec = {"engine": matrix.ENGINE_LABEL[name], "engine_version": engine.version, "sql_file": f["path"],
                       "golden_case": case["name"], "input_rows": len(case["rows"]), "rows_returned": len(rows),
                       "matches_golden": match,
                       "counter_name": "physicalInputBytes" if name == "trino" else "inputBytes (stage sum)",
                       "input_bytes": stats["counter"], "runs": REPEATS,
                       "spark_input_records": stats.get("input_records", ""),
                       "spark_stages": stats.get("stages", ""), "spark_skipped_stages": stats.get("skipped_stages", ""),
                       "spark_scan_operators": stats["detail"],
                       "trino_processed_rows": stats.get("processed_rows", "")}
                rows_out.append(rec)
                print("  %-34s %-18s %s=%s rows=%d golden=%s %s" % (
                    f["path"], case["name"], rec["counter_name"], rec["input_bytes"], len(rows), match, stats["detail"]),
                    flush=True)
        engine.close()
    fields = list(rows_out[0].keys())
    with open(os.path.join(args.out, "engine_scan_bytes.csv"), "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(rows_out)
    replace_section(os.path.join(args.out, "SUMMARY.md"), SECTION_HEADING, section(started, versions, rows_out, files))
    print("engine_scan_bytes.csv written; SUMMARY.md section written")


def section(started, versions, rows, files):
    lines = [SECTION_HEADING, ""]
    w = lines.append
    w("Measured by `ci/engine_scan_bytes.py --engines spark,trino` on %s, in the setup of `ci/run_sql_matrix.py`: "
      "each golden case's input.csv (3 or 4 rows) is loaded as customer_profile_changes, then every sql/ file the "
      "engine claims runs against it. Engines: %s. Every query ran %d times and the counter was the same each time." % (
          started, ", ".join("%s %s" % (matrix.ENGINE_LABEL[e], v) for e, v in versions.items()), REPEATS))
    w("")
    w("- Trino: `physicalInputBytes` from the client's final query statistics. The table lives in Trino's memory "
      "connector, so the counter is the size of the in-memory pages the table scan produced, not bytes read from "
      "storage.")
    w("- Spark: `inputBytes` (the task metric inputMetrics.bytesRead) summed over the stages of every job the query "
      "ran, read from the SparkContext status store. The view is a DataFrame built from Python rows, so the scan is "
      "an in-memory RDD scan and no input bytes are recorded.")
    w("- sql/duckdb/ files claim DuckDB only and are not run here; DuckDB is not part of this measurement.")
    w("")
    w(md_table(["SQL file", "Golden case", "Engine", "Counter", "Input bytes", "Rows returned", "Matches golden",
                "Scan operator"],
               [[r["sql_file"], r["golden_case"], r["engine"], r["counter_name"], r["input_bytes"], r["rows_returned"],
                 r["matches_golden"], r["spark_scan_operators"] or "table scan, catalog memory"]
                for r in sorted(rows, key=lambda r: (r["sql_file"], r["golden_case"], r["engine"]))]))
    w("")
    w("Files: `engine_scan_bytes.csv`.")
    w("")
    return "\n".join(lines)


if __name__ == "__main__":
    main()
