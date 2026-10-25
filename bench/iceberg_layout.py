#!/usr/bin/env python3
"""Iceberg data layout of the audit-trail table (design B) in three versions: (a) as the
Flink sink writes it, (b) after the repository's bin-pack compaction, (c) after a Spark
rewrite_data_files with strategy 'sort'.

Per model (pipeline.models in the config) and run (pipeline.runs):

1. Events: bench/pipeline.py write_events, the same generator, config and seed as
   bench/benchmark.py, design B, in event-time order.
2. com.example.audit.bench.IcebergLayout writes them through the Iceberg FlinkSink in
   pipeline.iceberg_commits bounded jobs at pipeline.parallelism, as PipelineBenchmark's
   iceberg mode does, expires snapshots (retainLast 1) and runs Actions.rewriteDataFiles
   with Iceberg's defaults, as that mode does. Snapshot (a) is the table as written and
   (b) the table after that compaction. Nothing is expired afterwards.
3. Spark (local pyspark with the Iceberg Spark runtime given by --spark-jar) rolls the
   table back to (a) and runs CALL system.rewrite_data_files with strategy 'sort',
   sort_order customer_id, event_timestamp, event_sequence (ascending, nulls first) and
   rewrite-all true. Its snapshot is (c). Both (b) and (c) start from (a).
4. For each version, the data files of its snapshot come from Spark's files metadata
   table at that snapshot ID, and their sizes are checked against the files on disk.
   Recorded: data files and bytes, bytes per event, the Parquet footers as DuckDB's
   parquet_metadata() and parquet_file_metadata() read them (row groups, rows per row
   group, codec, encodings, compressed and uncompressed bytes per column), and for
   q3_current_state_all, design B, from bench/benchmark.py, the bytes scanned:
   bench/resources.py's derivation (compressed bytes of the column chunks the scan reads,
   in row groups its pushed-down filters do not prune) over the snapshot's files. The
   three versions must return the same rows.
5. The table as written is checked against results/pipeline_iceberg_runs.csv: its data
   bytes must equal data_bytes_on_disk after_commits for design B in every committed run.

Writes iceberg_layout_runs.csv, iceberg_layout_columns.csv (run 1 of each model),
iceberg_layout_environment.json and the "Iceberg data layout" section of SUMMARY.md. No
other results file is touched.

  python3 bench/iceberg_layout.py --config bench/config.json --out results \\
      --spark-jar iceberg-spark-runtime-4.0_2.13-1.10.1.jar
"""
import argparse
import csv
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from decimal import Decimal
from fractions import Fraction

import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import benchmark as b  # noqa: E402  same generator, attributes and queries
import pipeline as p  # noqa: E402  same events, classpath and JVM flags
import resources as r  # noqa: E402  same bytes-scanned derivation
from summary_sections import replace_section, exact_or_prefix  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SECTION_HEADING = "## Iceberg data layout: as written, bin-pack compaction, sort compaction"
RUNS_CSV = "iceberg_layout_runs.csv"
COLUMNS_CSV = "iceberg_layout_columns.csv"
ENV_JSON = "iceberg_layout_environment.json"
QUERY = "q3_current_state_all"
DESIGN = "B"
SORT_ORDER = "customer_id ASC NULLS FIRST, event_timestamp ASC NULLS FIRST, event_sequence ASC NULLS FIRST"
VERSIONS = [("as_written", "(a) as the Flink sink wrote it"),
            ("binpack", "(b) after Actions.rewriteDataFiles (bin-pack)"),
            ("sort", "(c) after Spark rewrite_data_files, strategy sort")]


def sha1(path):
    h = hashlib.sha1()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def fmt(value):
    """Exact decimal when it terminates, else exact_or_prefix."""
    try:
        return r.exact(value)
    except ValueError:
        return exact_or_prefix(value)


def sql_list(files):
    return "[" + ", ".join("'%s'" % f.replace("'", "''") for f in files) + "]"


# ------------------------------------------------------------------------ steps


def write_table(cp, events, wh, pcfg, out_json, log):
    cmd = ["java", "-XX:+IgnoreUnrecognizedVMOptions"] + ["--add-opens=java.base/%s=ALL-UNNAMED" % m for m in p.ADD_OPENS]
    cmd += ["-cp", cp, "com.example.audit.bench.IcebergLayout", "--events", events, "--warehouse", wh,
            "--commits", str(pcfg["iceberg_commits"]), "--parallelism", str(pcfg["parallelism"]), "--out", out_json]
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if proc.returncode != 0:
        raise RuntimeError("IcebergLayout failed (exit %d):\n%s" % (proc.returncode, "\n".join(proc.stdout.splitlines()[-40:])))
    log("    Flink write and bin-pack compaction: %.1fs" % (time.perf_counter() - t0))
    with open(out_json) as fh:
        return json.load(fh)


def spark_session(jar, parallelism):
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[%d]" % parallelism).appName("iceberg-layout")
             .config("spark.ui.enabled", "false")
             .config("spark.jars", jar)
             .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def sort_rewrite(spark, cat, wh, snapshot_a, log):
    spark.conf.set("spark.sql.catalog.%s" % cat, "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set("spark.sql.catalog.%s.type" % cat, "hadoop")
    spark.conf.set("spark.sql.catalog.%s.warehouse" % cat, "file://" + wh)
    t0 = time.perf_counter()
    spark.sql("CALL %s.system.rollback_to_snapshot('audit.events', %d)" % (cat, snapshot_a)).collect()
    res = spark.sql("CALL %s.system.rewrite_data_files(table => 'audit.events', strategy => 'sort', "
                    "sort_order => '%s', options => map('rewrite-all', 'true'))" % (cat, SORT_ORDER)).collect()[0].asDict()
    main = spark.sql("SELECT snapshot_id FROM %s.audit.events.refs WHERE name = 'main'" % cat).collect()[0][0]
    parent = spark.sql("SELECT parent_id FROM %s.audit.events.snapshots WHERE snapshot_id = %d" % (cat, main)).collect()[0][0]
    if parent != snapshot_a:
        raise RuntimeError("sort snapshot %d has parent %s, expected %d" % (main, parent, snapshot_a))
    log("    Spark sort rewrite: %.1fs, %s" % (time.perf_counter() - t0, res))
    return main, res


def snapshot_files(spark, cat, snapshot):
    rows = spark.sql("SELECT content, file_path, file_size_in_bytes, record_count FROM %s.audit.events.files "
                     "VERSION AS OF %d" % (cat, snapshot)).collect()
    if any(x["content"] != 0 for x in rows):
        raise RuntimeError("snapshot %d has delete files" % snapshot)
    files = []
    for x in rows:
        path = x["file_path"][len("file:"):] if x["file_path"].startswith("file:") else x["file_path"]
        if os.path.getsize(path) != x["file_size_in_bytes"]:
            raise RuntimeError("%s: %d bytes on disk, %d in metadata" % (path, os.path.getsize(path), x["file_size_in_bytes"]))
        files.append({"path": path, "bytes": x["file_size_in_bytes"], "records": x["record_count"]})
    return sorted(files, key=lambda f: f["path"])


def measure(files, sql, work):
    """Footers, per-column sizes, and the current-state query's bytes scanned and answer."""
    con = duckdb.connect()
    lst = sql_list([f["path"] for f in files])
    meta = con.execute(
        "SELECT file_name, row_group_id, row_group_num_rows, path_in_schema, compression, encodings, "
        "total_compressed_size, total_uncompressed_size, stats_min_value, stats_max_value, stats_null_count "
        "FROM parquet_metadata(%s)" % lst).fetchall()
    created_by = sorted({x[0] for x in con.execute("SELECT created_by FROM parquet_file_metadata(%s)" % lst).fetchall()})
    layout, columns, row_groups = {}, {}, {}
    for fname, rg, nrows, col, codec, enc, comp, uncomp, lo, hi, nulls in meta:
        g = layout.setdefault((fname, rg), {"num_rows": nrows, "columns": {}})
        g["columns"][col] = {"compressed": comp, "min": lo, "max": hi, "null_count": nulls}
        row_groups[(fname, rg)] = nrows
        c = columns.setdefault(col, {"compressed": 0, "uncompressed": 0, "codecs": set(), "encodings": set()})
        c["compressed"] += comp
        c["uncompressed"] += uncomp
        c["codecs"].add(codec)
        c["encodings"].update(e.strip() for e in enc.split(","))
    out_of_order, runs = con.execute(
        "SELECT count(*) FILTER (WHERE pc IS NOT NULL AND (customer_id < pc OR (customer_id = pc AND "
        "(event_timestamp < pt OR (event_timestamp = pt AND event_sequence < ps))))), "
        "count(*) FILTER (WHERE pc IS NULL OR customer_id <> pc) "
        "FROM (SELECT customer_id, event_timestamp, event_sequence, "
        "LAG(customer_id) OVER w AS pc, LAG(event_timestamp) OVER w AS pt, LAG(event_sequence) OVER w AS ps "
        "FROM read_parquet(%s, filename = true, file_row_number = true) "
        "WINDOW w AS (PARTITION BY filename ORDER BY file_row_number))" % lst).fetchone()
    con.execute("CREATE VIEW b AS SELECT * FROM read_parquet(%s)" % lst)
    profile_path = os.path.join(work, "profile.json")
    con.execute("SET enable_profiling = 'json'")
    con.execute("SET profiling_output = '%s'" % profile_path)
    con.execute("CREATE OR REPLACE TEMP TABLE bench_result AS " + sql)
    con.execute("RESET enable_profiling")
    con.execute("RESET profiling_output")
    with open(profile_path) as fh:
        prof = json.load(fh, parse_float=Decimal)
    scan = r.bytes_scanned(prof, {"b": layout}, "b")
    answer = con.execute("SELECT * FROM bench_result ORDER BY ALL").fetchall()
    con.close()
    return {"row_groups": row_groups, "columns": columns, "created_by": created_by, "scan": scan,
            "out_of_order": out_of_order, "customer_id_runs": runs,
            "answer_rows": len(answer), "answer_sha256": hashlib.sha256(repr(answer).encode()).hexdigest()}


# ------------------------------------------------------------------------ report


def committed_as_written(out_dir):
    """data_bytes_on_disk after_commits, design B, every committed run, by model."""
    path = os.path.join(out_dir, "pipeline_iceberg_runs.csv")
    found = {}
    for row in csv.DictReader(open(path)):
        if row["schema"] == DESIGN and row["state"] == "after_commits":
            found.setdefault(row["model"], set()).add(int(row["data_bytes_on_disk"]))
    return found


def med(values):
    m = p.med(values)
    return b.fmt_bytes(m) if m.denominator == 1 else fmt(m)


def span(values):
    values = sorted(values)
    return "%s in every run" % b.fmt_bytes(values[0]) if values[0] == values[-1] else \
        "median %s, min %s, max %s" % (med(values), b.fmt_bytes(values[0]), b.fmt_bytes(values[-1]))


def section(env, runs, columns):
    lines = []
    w = lines.append
    cfg, pcfg = env["config"], env["config"]["pipeline"]
    n = cfg["n_entities"] * cfg["h_events"]
    w(SECTION_HEADING)
    w("")
    w("Measured by `bench/iceberg_layout.py` from `%s` on %s: design B (audit trail), seed %d, N=%d, H=%d, K=%d, "
      "%s events per table, %d runs per model. Machine: %s, %s logical CPUs. Wall time %s s. Flink %s with Iceberg %s "
      "(FlinkSink, Actions.rewriteDataFiles); Spark %s with %s (SHA-1 %s); DuckDB %s for footers and the query." % (
          env["config_file"], env["started_utc"], cfg["seed"], cfg["n_entities"], cfg["h_events"], cfg["k_attributes"],
          b.fmt_bytes(n), pcfg["runs"], env["machine"].get("cpu", env["machine"]["machine"]), env["machine"]["cpu_count"],
          env["wall_seconds"], env["flink_version"], env["iceberg_flink_version"], env["spark_version"],
          env["spark_jar"]["name"], env["spark_jar"]["sha1"], env["duckdb"]))
    w("")
    w("- (a) The events go through the Iceberg FlinkSink in %d bounded jobs at parallelism %d, as the pipeline "
      "benchmark's Iceberg mode writes them (`com.example.audit.bench.IcebergLayout`), into a table partitioned by "
      "day(event_timestamp) with declared sort order %s." % (pcfg["iceberg_commits"], pcfg["parallelism"],
                                                             env["declared_sort_order"]))
    w("- (b) Snapshot expiration (retainLast 1), then `Actions.rewriteDataFiles` with Iceberg's defaults, as in that "
      "mode: the repository's compaction, which bin-packs files and does not sort.")
    w("- (c) From (a): Spark `CALL system.rewrite_data_files` with `strategy => 'sort'`, `sort_order => '%s'` and "
      "`rewrite-all` true, on local[%d]." % (SORT_ORDER, pcfg["parallelism"]))
    w("- Data bytes are the sizes of the snapshot's data files, from Spark's files metadata table at that snapshot ID "
      "and equal to the sizes on disk. Bytes scanned are for %s, design B, from bench/benchmark.py, over the "
      "snapshot's files: bench/resources.py's derivation from parquet_metadata(), compressed bytes of the column "
      "chunks the scan reads, in row groups its pushed-down filters do not prune. All three versions return the same "
      "rows in every run. Within each data file in storage order: customer_id runs counts the maximal runs of rows "
      "with one customer_id, summed over the files (%d entities, so %d when each entity's rows are contiguous in one "
      "file); adjacent rows out of sort order counts the rows whose (customer_id, event_timestamp, event_sequence) is "
      "lower than the row before it." % (QUERY, cfg["n_entities"], cfg["n_entities"]))
    for model in pcfg["models"]:
        rows = [x for x in runs if x["model"] == model]
        w("")
        w("### %s model" % model.upper())
        w("")
        table = []
        for v, label in VERSIONS:
            vr = [x for x in rows if x["version"] == v]
            data = [int(x["data_bytes"]) for x in vr]
            scanned = [int(x["scan_bytes"]) for x in vr]
            per_event = sorted({x["bytes_per_event"] for x in vr}, key=Fraction)
            table.append([label, " / ".join(sorted({x["data_files"] for x in vr}, key=int)), span(data),
                          per_event[0] if len(per_event) == 1 else "%s to %s" % (per_event[0], per_event[-1]),
                          span(scanned), " / ".join(sorted({x["row_groups"] for x in vr}, key=int)),
                          " / ".join(sorted({"%s to %s" % (x["rows_per_row_group_min"], x["rows_per_row_group_max"])
                                             for x in vr})),
                          " / ".join(sorted({x["codecs"] for x in vr})),
                          span([int(x["customer_id_runs"]) for x in vr]),
                          span([int(x["adjacent_rows_out_of_sort_order"]) for x in vr])])
        w(b.md_table(["Version", "Data files", "Data bytes", "Bytes per event", "Bytes scanned by %s" % QUERY,
                      "Row groups", "Rows per row group", "Codec", "customer_id runs",
                      "Adjacent rows out of sort order"], table))
        w("")
        aw = [x for x in rows if x["version"] == "as_written"]
        w("The table as written matches results/pipeline_iceberg_runs.csv (design B, after_commits, data bytes on "
          "disk) in %d of %d runs." % (sum(x["matches_committed_as_written"] == "yes" for x in aw), len(aw)))
        w("")
        w("Column chunk bytes, run 1, compressed (uncompressed), summed over the snapshot's files:")
        w("")
        cols = [c for c in columns if c["model"] == model]
        keys = ["customer_id", "event_timestamp", "event_sequence"]
        crow = []
        for v, label in VERSIONS:
            vc = {c["column"]: c for c in cols if c["version"] == v}
            attrs = [c for name, c in vc.items() if name not in keys]
            crow.append([label] + ["%s (%s)" % (b.fmt_bytes(vc[k]["compressed_bytes"]), b.fmt_bytes(vc[k]["uncompressed_bytes"]))
                                   for k in keys] +
                        ["%s (%s)" % (b.fmt_bytes(sum(int(c["compressed_bytes"]) for c in attrs)),
                                      b.fmt_bytes(sum(int(c["uncompressed_bytes"]) for c in attrs))),
                         " / ".join(sorted({c["encodings"] for c in vc.values()}))])
        w(b.md_table(["Version"] + keys + ["%d attribute columns" % cfg["k_attributes"], "Encodings seen"], crow))
    w("")
    w("### Parquet settings")
    w("")
    props = env["table_properties"]
    w("Table properties in the metadata: %s. Iceberg %s defaults for properties not set: %s. Every version's footers "
      "name the codec in the tables above; created_by: %s." % (
          ", ".join("`%s` = `%s`" % kv for kv in sorted(props.items())),
          env["iceberg_flink_version"],
          ", ".join("`%s` = `%s`" % kv for kv in sorted(env["iceberg_parquet_defaults"].items())),
          "; ".join("%s: %s" % (label, ", ".join(env["created_by"][v])) for v, label in VERSIONS)))
    w("")
    w("Files: `%s` (every run and version), `%s` (every column, run 1), `%s`." % (RUNS_CSV, COLUMNS_CSV, ENV_JSON))
    w("")
    return "\n".join(lines)


# ------------------------------------------------------------------------ main


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="bench/config.json")
    ap.add_argument("--out", default="results")
    ap.add_argument("--spark-jar", required=True, help="iceberg-spark-runtime jar for the installed pyspark")
    args = ap.parse_args()
    cfg = json.load(open(args.config))
    pcfg = cfg["pipeline"]
    n = cfg["n_entities"] * cfg["h_events"]
    t_start = time.perf_counter()

    def log(msg):
        print("[%7.1fs] %s" % (time.perf_counter() - t_start, msg), flush=True)

    import pyspark
    jar = os.path.abspath(args.spark_jar)
    env = {"started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "config_file": args.config,
           "config": cfg, "machine": b.machine_info(), "duckdb": duckdb.__version__,
           "pyspark": pyspark.__version__,
           "spark_jar": {"name": os.path.basename(jar), "bytes": os.path.getsize(jar), "sha1": sha1(jar)},
           "java": subprocess.run(["java", "-version"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                  text=True).stdout.splitlines()[0],
           "sort_order": SORT_ORDER, "rewrite_options": {"rewrite-all": "true"}}
    pom = open(os.path.join(ROOT, "pom.xml")).read()
    for key, tag in (("flink_version", "flink.version"), ("iceberg_flink_version", "iceberg.version")):
        env[key] = pom.split("<%s>" % tag)[1].split("</%s>" % tag)[0]
    committed = committed_as_written(args.out)
    attrs = b.attributes(cfg["k_attributes"])
    entity = "C%07d" % (cfg["n_entities"] // 2)
    cp = p.classpath(log)
    spark = spark_session(jar, pcfg["parallelism"])
    env["spark_version"] = spark.version
    runs, columns, created_by = [], [], {}
    work = tempfile.mkdtemp(prefix="audit-iceberg-layout-")
    try:
        for model in pcfg["models"]:
            events = p.write_events(cfg, model, work, log)[DESIGN]
            sql = next(q[4] for q in b.build_queries(attrs, model, entity) if q[0] == QUERY and q[3] == DESIGN)
            for run in range(1, pcfg["runs"] + 1):
                log("  %s run %d" % (model, run))
                wh = os.path.join(work, "wh-%s-%d" % (model, run))
                snaps = write_table(cp, events, wh, pcfg, os.path.join(work, "snaps.json"), log)
                if snaps["events"] != n:
                    raise RuntimeError("IcebergLayout read %d events, expected %d" % (snaps["events"], n))
                env["table_properties"] = snaps["table_properties"]
                env["declared_sort_order"] = snaps["declared_sort_order"]
                env["iceberg_parquet_defaults"] = snaps["iceberg_parquet_defaults"]
                cat = "layout_%s_%d" % (model, run)
                sc, res = sort_rewrite(spark, cat, wh, snaps["snapshot_as_written"], log)
                ids = {"as_written": snaps["snapshot_as_written"], "binpack": snaps["snapshot_after_binpack"], "sort": sc}
                answers = set()
                for v, _ in VERSIONS:
                    files = snapshot_files(spark, cat, ids[v])
                    records = sum(f["records"] for f in files)
                    if records != n:
                        raise RuntimeError("%s %s: %d records, expected %d" % (model, v, records, n))
                    m = measure(files, sql, work)
                    answers.add(m["answer_sha256"])
                    data = sum(f["bytes"] for f in files)
                    rg_rows = list(m["row_groups"].values())
                    created_by.setdefault(v, set()).update(m["created_by"])
                    row = {"model": model, "run": run, "version": v, "snapshot_id": ids[v], "data_files": len(files),
                           "data_bytes": data, "rows": records, "bytes_per_event": fmt(Fraction(data, n)),
                           "row_groups": len(rg_rows), "rows_per_row_group_min": min(rg_rows),
                           "rows_per_row_group_max": max(rg_rows),
                           "codecs": " ".join(sorted(set().union(*(c["codecs"] for c in m["columns"].values())))),
                           "adjacent_rows_out_of_sort_order": m["out_of_order"],
                           "customer_id_runs": m["customer_id_runs"],
                           "rows_per_customer_id_run": fmt(Fraction(records, m["customer_id_runs"])),
                           "scan_bytes": m["scan"]["bytes_scanned"], "scan_columns_read": m["scan"]["columns_read"],
                           "scan_row_groups_read": m["scan"]["row_groups_read"],
                           "scan_pushed_filters": m["scan"]["pushed_filters"],
                           "answer_rows": m["answer_rows"], "answer_sha256": m["answer_sha256"],
                           "matches_committed_as_written": ("yes" if committed.get(model) == {data} else "no")
                           if v == "as_written" else "",
                           "sort_rewritten_files": res["rewritten_data_files_count"] if v == "sort" else "",
                           "sort_added_files": res["added_data_files_count"] if v == "sort" else "",
                           "sort_rewritten_bytes": res["rewritten_bytes_count"] if v == "sort" else ""}
                    runs.append(row)
                    log("    %-10s files %d, bytes %d (%s per event), scanned %d, row groups %d" % (
                        v, len(files), data, row["bytes_per_event"], row["scan_bytes"], len(rg_rows)))
                    if run == 1:
                        for col, c in sorted(m["columns"].items()):
                            columns.append({"model": model, "version": v, "column": col,
                                            "compressed_bytes": c["compressed"], "uncompressed_bytes": c["uncompressed"],
                                            "codecs": " ".join(sorted(c["codecs"])),
                                            "encodings": " ".join(sorted(c["encodings"]))})
                if len(answers) != 1:
                    raise RuntimeError("%s run %d: the versions return different rows for %s" % (model, run, QUERY))
                shutil.rmtree(wh, ignore_errors=True)
    finally:
        spark.stop()
        shutil.rmtree(work, ignore_errors=True)
    env["created_by"] = {v: sorted(s) for v, s in created_by.items()}
    env["wall_seconds"] = round(time.perf_counter() - t_start, 1)
    env["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    b.write_csv(os.path.join(args.out, RUNS_CSV), runs, list(runs[0].keys()))
    b.write_csv(os.path.join(args.out, COLUMNS_CSV), columns, list(columns[0].keys()))
    with open(os.path.join(args.out, ENV_JSON), "w") as fh:
        json.dump(env, fh, indent=2)
    runs_read = list(csv.DictReader(open(os.path.join(args.out, RUNS_CSV))))
    cols_read = list(csv.DictReader(open(os.path.join(args.out, COLUMNS_CSV))))
    replace_section(os.path.join(args.out, "SUMMARY.md"), SECTION_HEADING, section(env, runs_read, cols_read))
    log("%s, %s, %s and the SUMMARY.md section written; wall time %.1fs" % (RUNS_CSV, COLUMNS_CSV, ENV_JSON,
                                                                           env["wall_seconds"]))


if __name__ == "__main__":
    main()
