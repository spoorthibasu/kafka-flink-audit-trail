#!/usr/bin/env python3
"""Sort order of the audit-trail Parquet file: customer_id only, or customer_id,
event_timestamp, event_sequence.

Regenerates the main design B (audit trail) datasets of bench/benchmark.py, FULL and
SPARSE, from the same config and seed, and writes each one with the DuckDB Parquet
writer, the same compression and row-group setting as the main benchmark, in three
orders:

  customer_id                                       ORDER BY customer_id
  customer_id, event_timestamp, event_sequence      ORDER BY customer_id, event_timestamp, event_sequence
  customer_id, then hash order within an entity     ORDER BY customer_id, hash(event_timestamp, event_sequence)

The second file is the one bench/benchmark.py writes; its sizes are checked against
storage.csv in the output directory. The generator emits each entity's rows in event
order, so a sort on customer_id alone can leave them in that order; the third file
puts each entity's rows in the order of DuckDB's hash() instead, a customer_id sort
over rows that are not in event order. For each file it records the bytes on disk, its
SHA-256, whether it is byte-identical to the fully sorted file, whether a second write
is byte-identical, and how many adjacent rows of one entity are out of
(event_timestamp, event_sequence) order as stored.

For q1 (one-hop previous value, single entity) and q3 (current state of every
attribute, all entities), design B queries from bench/benchmark.py, over the snappy
file, it checks that both files return the same rows and measures:

  p50 latency     bench/benchmark.py's protocol: CREATE OR REPLACE TEMP TABLE ... AS
                  <query>, runs and warm-ups from the config, nearest-rank percentiles
  CPU time        bench/resources.py's protocol: DuckDB profiler metric cpu_time, a fresh
                  in-memory database per run, median over the runs
  bytes scanned   bench/resources.py's derivation from parquet_metadata(): compressed
                  bytes of the column chunks read, in row groups the filters do not prune

Writes sort_order.csv, sort_order_environment.json and the "Sort order" section of
SUMMARY.md. No other results file is touched.

  python3 bench/sort_order.py --config bench/config.json --out results
"""
import argparse
import csv
import hashlib
import json
import os
import shutil
import sys
import tempfile
import time

import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import benchmark as b  # noqa: E402  same generator, queries and latency protocol
import resources as r  # noqa: E402  same CPU-time and bytes-scanned protocol
from summary_sections import replace_section  # noqa: E402

SECTION_HEADING = "## Sort order of the audit-trail Parquet file"
FULL_SORT = "customer_id, event_timestamp, event_sequence"
ORDERS = [  # (label, file name part, ORDER BY)
    ("customer_id", "cid", "customer_id"),
    (FULL_SORT, "cid_ts_seq", FULL_SORT),
    ("customer_id, then hash order within an entity", "cid_hash", "customer_id, hash(event_timestamp, event_sequence)")]
QUERIES = ("q1_one_hop_previous_single", "q3_current_state_all")
DESIGN = "B"


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def write(con, path, order_by, compression, row_group_size):
    con.execute("COPY (SELECT * FROM b ORDER BY %s) TO '%s' (FORMAT PARQUET, COMPRESSION %s, ROW_GROUP_SIZE %d)"
                % (order_by, path, compression, row_group_size))
    return os.path.getsize(path)


def out_of_order(con, path):
    """Adjacent rows of one entity, in file order, whose (event_timestamp, event_sequence)
    is lower than the row before; and the number of entities with at least one."""
    return con.execute(
        "WITH f AS (SELECT customer_id, event_timestamp, event_sequence, file_row_number FROM "
        "read_parquet('%s', file_row_number = true)), "
        "p AS (SELECT customer_id, event_timestamp, event_sequence, "
        "LAG(customer_id) OVER (ORDER BY file_row_number) AS pc, "
        "LAG(event_timestamp) OVER (ORDER BY file_row_number) AS pt, "
        "LAG(event_sequence) OVER (ORDER BY file_row_number) AS ps FROM f) "
        "SELECT count(*), count(DISTINCT customer_id) FROM p WHERE pc = customer_id AND "
        "(event_timestamp < pt OR (event_timestamp = pt AND event_sequence < ps))" % path).fetchone()


def rows_of(con, sql):
    return sorted(con.execute(sql).fetchall(), key=lambda row: tuple("\x00" if v is None else str(v) for v in row))


def run_model(cfg, model, attrs, work, out_dir, log):
    n, h = cfg["n_entities"], cfg["h_events"]
    rg = cfg["storage"]["parquet_row_group_size"]
    comps = cfg["storage"]["parquet_compressions"]
    data_dir = os.path.join(work, model)
    os.makedirs(data_dir)
    b.generate(cfg, model, n, h, attrs, data_dir, None)
    con = duckdb.connect()
    b.load_csv(con, "b", os.path.join(data_dir, "b.csv"), b.columns_sql(attrs, False))

    files, layout = {}, {}
    rows = []
    for label, slug, order_by in ORDERS:
        rec = {"model": model, "n_entities": n, "h_events": h, "design": DESIGN, "sort_order": label,
               "order_by": order_by}
        for comp in comps:
            path = os.path.join(data_dir, "b_%s_%s.parquet" % (comp, slug))
            rec["parquet_bytes_%s" % comp] = write(con, path, order_by, comp, rg)
            if comp == comps[0]:
                files[label] = path
                again = path + ".again"
                write(con, again, order_by, comp, rg)
                rec["rewrite_identical_%s" % comp] = "yes" if sha256(again) == sha256(path) else "no"
                rec["sha256_%s" % comp] = sha256(path)
                os.remove(again)
        rec["out_of_order_adjacent_rows"], rec["entities_out_of_order"] = out_of_order(con, files[label])
        layout[label] = r.parquet_layout(con, files[label])
        rec["row_group_rows"] = r.describe_row_groups([g["num_rows"] for _, g in sorted(layout[label].items())])
        rows.append(rec)
        log("  %s %-46s snappy %d zstd %d, rewrite identical %s, out-of-order rows %d in %d entities" % (
            model, label, rec["parquet_bytes_snappy"], rec["parquet_bytes_zstd"], rec["rewrite_identical_snappy"],
            rec["out_of_order_adjacent_rows"], rec["entities_out_of_order"]))
    con.close()
    full = next(f for f in rows if f["sort_order"] == FULL_SORT)
    for f in rows:
        f["identical_to_full_sort_%s" % comps[0]] = "yes" if f["sha256_%s" % comps[0]] == full["sha256_%s" % comps[0]] else "no"
    check_storage(out_dir, model, n, h, {("B", c): full["parquet_bytes_%s" % c] for c in comps})

    entity = "C%07d" % (n // 2)
    queries = {qid: sql for qid, _, _, d, sql, _ in b.build_queries(attrs, model, entity) if d == DESIGN and sql}
    results = []
    for qid in QUERIES:
        sql = queries[qid]
        answers = {}
        for label, _, _ in ORDERS:
            con = duckdb.connect()
            con.execute("CREATE VIEW b AS SELECT * FROM read_parquet('%s')" % files[label])
            answers[label] = rows_of(con, sql)
            con.close()
        if len(set(map(tuple, answers.values()))) != 1:
            raise RuntimeError("%s %s: the sort orders return different rows" % (model, qid))
        for label, _, _ in ORDERS:
            con = duckdb.connect()
            con.execute("CREATE VIEW b AS SELECT * FROM read_parquet('%s')" % files[label])
            m = b.time_query(con, sql, cfg["queries"]["runs"], cfg["queries"]["warmup_runs"],
                             os.path.join(data_dir, "prof.json"), os.path.join(data_dir, "explain.txt"))
            con.close()
            samples, scan = r.measure({"b": files[label]}, sql, "b", cfg["queries"]["runs"], cfg["queries"]["warmup_runs"],
                                      data_dir, {"b": layout[label]})
            rec = {"model": model, "query": qid, "design": DESIGN, "sort_order": label,
                   "entity": entity if qid.endswith("_single") else "", "rows_returned": m["rows_returned"],
                   "same_rows_all_orders": "yes", "runs": m["runs"], "warmup_runs": m["warmup_runs"],
                   "p50_ms": m["p50_ms"], "p95_ms": m["p95_ms"], "p99_ms": m["p99_ms"],
                   "cpu_ms_median": r.exact(r.median(samples["cpu_ms"])),
                   "cpu_ms_min": r.exact(min(samples["cpu_ms"])), "cpu_ms_max": r.exact(max(samples["cpu_ms"])),
                   "bytes_scanned": scan["bytes_scanned"], "columns_read": scan["columns_read"],
                   "columns_in_file": scan["columns_in_file"], "row_groups_read": scan["row_groups_read"],
                   "row_groups_in_file": scan["row_groups_in_file"], "pushed_filters": scan["pushed_filters"]}
            results.append(rec)
            log("  %s %-28s %-46s p50 %s ms, cpu %s ms, scanned %d" % (
                model, qid, label, rec["p50_ms"], rec["cpu_ms_median"], rec["bytes_scanned"]))
    return rows, results


def check_storage(out_dir, model, n, h, sizes):
    """The fully sorted file must equal storage.csv's design B sizes for this model."""
    path = os.path.join(out_dir, "storage.csv")
    if not os.path.exists(path):
        raise RuntimeError("%s not found; run bench/benchmark.py with the same config first" % path)
    found = 0
    for row in csv.DictReader(open(path)):
        key = (row["design"], row["parquet_compression"])
        if row["model"] == model and int(row["n_entities"]) == n and int(row["h_events"]) == h and key in sizes:
            found += 1
            if int(row["parquet_bytes"]) != sizes[key]:
                raise RuntimeError("%s %s: %d differs from storage.csv %s" % (model, key, sizes[key], row["parquet_bytes"]))
    if found != len(sizes):
        raise RuntimeError("storage.csv has %d of %d expected rows for %s" % (found, len(sizes), model))


def section(env, files, results):
    cfg = env["config"]
    lines = [SECTION_HEADING, ""]
    w = lines.append
    w("Measured by `bench/sort_order.py` from `%s` on %s: design B (audit trail), seed %d, N=%d, H=%d, K=%d, FULL "
      "and SPARSE models. Machine: %s, %s logical CPUs. DuckDB %s, threads=%s. Wall time %s s. File bytes reproduce "
      "exactly when the rewrite check says so; latency and CPU time vary between runs." % (
          env["config_file"], env["started_utc"], cfg["seed"], cfg["n_entities"], cfg["h_events"], cfg["k_attributes"],
          env["machine"].get("cpu", env["machine"]["platform"]), env["machine"]["cpu_count"], env["duckdb"],
          env["duckdb_threads"], env["wall_seconds"]))
    w("")
    w("- Every file is written by DuckDB's Parquet writer from the same rows with ROW_GROUP_SIZE %d; only the "
      "ORDER BY of the COPY differs (column order_by in sort_order.csv). The %s file is the main benchmark's file: "
      "its sizes equal storage.csv for design B (%d checks)." % (cfg["storage"]["parquet_row_group_size"], FULL_SORT,
                                                                  env["storage_checks"]))
    w("- The generator emits each entity's rows in event order. \"customer_id, then hash order within an entity\" "
      "is ORDER BY customer_id, hash(event_timestamp, event_sequence): a customer_id sort over rows that are not in "
      "event order, added beside the two requested orders.")
    w("- Identical to full sort: the snappy file's SHA-256 equals that of the %s file. Rewrite identical: the same "
      "COPY run a second time produced a byte-identical snappy file." % FULL_SORT)
    same = [f for f in files if f["sort_order"] == "customer_id" and f["identical_to_full_sort_snappy"] == "yes"]
    if same:
        w("- In the %s model%s the customer_id file is byte-identical to the %s file, so the two read the same bytes "
          "and their latency and CPU differences below are run-to-run variation." % (
              " and ".join(sorted(set(f["model"].upper() for f in same))), "s" if len(same) > 1 else "", FULL_SORT))
    w("- Out-of-order rows: adjacent rows of one entity, in file order, whose (event_timestamp, event_sequence) is "
      "lower than the row before.")
    w("- p50 ms: %d timed runs after %d warm-up runs on one connection, the protocol of the latency table above. "
      "CPU ms and bytes scanned: the protocol and definitions of the resource-use section above (median of %d runs, "
      "each on a fresh in-memory database; bytes scanned derived from parquet_metadata(), not observed). The "
      "queries read the snappy file. Every file returned the same rows for every query." % (
          cfg["queries"]["runs"], cfg["queries"]["warmup_runs"], cfg["queries"]["runs"]))
    w("")
    w("### Files")
    w("")
    w(b.md_table(["Model", "Sort order", "Parquet snappy bytes", "Parquet zstd bytes", "Identical to full sort",
                  "Rewrite identical", "Out-of-order rows", "Entities with out-of-order rows", "Row groups"],
                 [[f["model"], f["sort_order"], b.fmt_bytes(f["parquet_bytes_snappy"]), b.fmt_bytes(f["parquet_bytes_zstd"]),
                   f["identical_to_full_sort_snappy"], f["rewrite_identical_snappy"],
                   b.fmt_bytes(f["out_of_order_adjacent_rows"]), b.fmt_bytes(f["entities_out_of_order"]),
                   f["row_group_rows"]] for f in files]))
    w("")
    w("### Queries")
    w("")
    w(b.md_table(["Model", "Query", "Sort order", "p50 ms", "CPU ms", "Bytes scanned", "Columns read",
                  "Row groups read", "Rows returned"],
                 [[q["model"], q["query"], q["sort_order"], q["p50_ms"], q["cpu_ms_median"], b.fmt_bytes(q["bytes_scanned"]),
                   "%d of %d" % (q["columns_read"], q["columns_in_file"]),
                   "%d of %d" % (q["row_groups_read"], q["row_groups_in_file"]), b.fmt_bytes(q["rows_returned"])]
                  for q in results]))
    w("")
    w("Files: `sort_order.csv`, `sort_order_environment.json`.")
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

    env = {"started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "config_file": args.config,
           "config": cfg, "machine": b.machine_info(), "duckdb": duckdb.__version__,
           "duckdb_threads": duckdb.connect().execute("SELECT current_setting('threads')").fetchone()[0]}
    attrs = b.attributes(cfg["k_attributes"])
    work = tempfile.mkdtemp(prefix="audit-sort-order-")
    files, results = [], []
    try:
        for model in cfg["event_models"]:
            f, q = run_model(cfg, model, attrs, work, args.out, log)
            files.extend(f)
            results.extend(q)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    env["storage_checks"] = len(cfg["event_models"]) * len(cfg["storage"]["parquet_compressions"])
    rows = []
    for q in results:
        f = next(x for x in files if x["model"] == q["model"] and x["sort_order"] == q["sort_order"])
        rows.append(dict(f, **q))
    b.write_csv(os.path.join(args.out, "sort_order.csv"), rows, list(rows[0].keys()))
    env["wall_seconds"] = round(time.perf_counter() - t_start, 1)
    env["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    with open(os.path.join(args.out, "sort_order_environment.json"), "w") as fh:
        json.dump(env, fh, indent=2)
    replace_section(os.path.join(args.out, "SUMMARY.md"), SECTION_HEADING, section(env, files, results))
    log("sort_order.csv written; SUMMARY.md section written")


if __name__ == "__main__":
    main()
