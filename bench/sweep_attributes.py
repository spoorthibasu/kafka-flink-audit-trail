#!/usr/bin/env python3
"""Generality sweep over the number of attributes K, full-state events.

For each K in config["attribute_sweep"]["k_values"], at the same N, H and seed as
bench/benchmark.py, this measures for design A (previous-value schema) and design B
(audit trail):

  field count               fields in the Avro schema, keys included
  Avro bytes per event      fastavro schemaless datum, no framing; mean and median over
                            all N x H events; uncompressed and raw DEFLATE (zlib level
                            from config), each event compressed on its own
  Parquet bytes             DuckDB Parquet writer, snappy and zstd, same row-group setting
  p50 latency               q1 one-hop previous value, single entity, and q3 current
                            state of every attribute, all entities; the same
                            CREATE TABLE AS protocol, runs and warm-ups as the latency table

and the A to B ratio of each. Design C appears for q3 only, as in the main benchmark.
At K equal to config["k_attributes"] the data is the main FULL dataset, so Avro totals
and Parquet sizes are checked against payload.csv and storage.csv in the output
directory when those files exist.

  python3 bench/sweep_attributes.py --config bench/config.json --out results
"""
import argparse
import csv
import json
import os
import shutil
import statistics
import sys
import tempfile
import time
from fractions import Fraction

import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import benchmark as b  # noqa: E402
from summary_sections import replace_section, exact_or_prefix  # noqa: E402

SECTION_HEADING = "## Generality sweep over attribute count"
MODEL = "full"


def ratio(a, bb):
    return exact_or_prefix(Fraction(a) / Fraction(bb))


def check_against_main(out_dir, cfg, k, totals, sizes):
    """At the main K, the sweep regenerates the main FULL dataset: totals must match."""
    checks = []
    pay = os.path.join(out_dir, "payload.csv")
    if os.path.exists(pay):
        for r in csv.DictReader(open(pay)):
            if r["model"] == MODEL and r["compression"] in ("none", "deflate"):
                got = totals[(r["design"], r["compression"])]
                if got != int(r["total_bytes"]):
                    raise RuntimeError("K=%d %s %s Avro total %d differs from payload.csv %s"
                                       % (k, r["design"], r["compression"], got, r["total_bytes"]))
                checks.append("payload.csv %s %s" % (r["design"], r["compression"]))
    sto = os.path.join(out_dir, "storage.csv")
    if os.path.exists(sto):
        for r in csv.DictReader(open(sto)):
            if (r["model"] == MODEL and int(r["n_entities"]) == cfg["n_entities"]
                    and int(r["h_events"]) == cfg["h_events"]):
                key = (r["design"], r["parquet_compression"])
                if key in sizes:
                    if sizes[key] != int(r["parquet_bytes"]):
                        raise RuntimeError("K=%d %s Parquet %d differs from storage.csv %s"
                                           % (k, key, sizes[key], r["parquet_bytes"]))
                    checks.append("storage.csv %s %s" % key)
    return checks


def run_k(cfg, k, work, log):
    n, h = cfg["n_entities"], cfg["h_events"]
    attrs = b.attributes(k)
    data_dir = os.path.join(work, "k%d" % k)
    os.makedirs(data_dir)
    sizes = {"A": {"none": [], "deflate": [], "snappy": []}, "B": {"none": [], "deflate": [], "snappy": []}}
    t0 = time.perf_counter()
    b.generate(cfg, MODEL, n, h, attrs, data_dir, sizes)
    log("K=%d generated (%.1fs)" % (k, time.perf_counter() - t0))
    fields = {"A": len(b.avro_schema("PreviousValue", attrs, True)["fields"]),
              "B": len(b.avro_schema("AuditTrail", attrs, False)["fields"]),
              "C": len(b.avro_schema("CurrentState", attrs, False)["fields"])}

    con = duckdb.connect()
    b.load_csv(con, "a", os.path.join(data_dir, "a.csv"), b.columns_sql(attrs, True))
    b.load_csv(con, "b", os.path.join(data_dir, "b.csv"), b.columns_sql(attrs, False))
    b.load_csv(con, "c", os.path.join(data_dir, "c.csv"), b.columns_sql(attrs, False))
    pq = {}
    snappy_paths = {}
    for comp in ("snappy", "zstd"):
        for design, table in (("A", "a"), ("B", "b"), ("C", "c")):
            path = os.path.join(data_dir, "%s_%s.parquet" % (table, comp))
            pq[(design, comp)] = b.write_parquet(con, table, path, comp, cfg["storage"]["parquet_row_group_size"])
            if comp == "snappy":
                snappy_paths[table] = path
    for table in ("a", "b", "c"):
        con.execute("DROP TABLE %s" % table)
        con.execute("CREATE VIEW %s AS SELECT * FROM read_parquet('%s')" % (table, snappy_paths[table]))

    entity = "C%07d" % (n // 2)
    queries = {(q, d): sql for q, _, _, d, sql, _ in b.build_queries(attrs, MODEL, entity) if sql}
    lat = {}
    for key in (("q1_one_hop_previous_single", "A"), ("q1_one_hop_previous_single", "B"),
                ("q3_current_state_all", "A"), ("q3_current_state_all", "B"), ("q3_current_state_all", "C")):
        m = b.time_query(con, queries[key], cfg["queries"]["runs"], cfg["queries"]["warmup_runs"],
                         os.path.join(data_dir, "prof.json"), os.path.join(data_dir, "explain.txt"))
        lat[key] = m["p50_ms"]
    con.close()

    totals = {(d, c): sum(sizes[d][c]) for d in ("A", "B") for c in ("none", "deflate")}
    row = {"k_attributes": k, "model": MODEL, "n_entities": n, "h_events": h, "events": n * h,
           "fields_A": fields["A"], "fields_B": fields["B"], "fields_C": fields["C"],
           "fields_ratio_A_to_B": ratio(fields["A"], fields["B"])}
    for comp, label in (("none", "uncompressed"), ("deflate", "deflate")):
        for stat in ("mean", "median"):
            vals = {}
            for d in ("A", "B"):
                v = sizes[d][comp]
                vals[d] = Fraction(sum(v), len(v)) if stat == "mean" else statistics.median([Fraction(x) for x in v])
                row["avro_%s_%s_bytes_%s" % (label, stat, d)] = exact_or_prefix(vals[d])
            row["avro_%s_%s_ratio_A_to_B" % (label, stat)] = ratio(vals["A"], vals["B"])
        row["avro_%s_total_bytes_A" % label] = totals[("A", comp)]
        row["avro_%s_total_bytes_B" % label] = totals[("B", comp)]
    for comp in ("snappy", "zstd"):
        for d in ("A", "B", "C"):
            row["parquet_%s_bytes_%s" % (comp, d)] = pq[(d, comp)]
        row["parquet_%s_ratio_A_to_B" % comp] = ratio(pq[("A", comp)], pq[("B", comp)])
    for q, label in (("q1_one_hop_previous_single", "q1"), ("q3_current_state_all", "q3")):
        for d in ("A", "B", "C"):
            if (q, d) in lat:
                row["p50_ms_%s_%s" % (label, d)] = lat[(q, d)]
        row["p50_ms_%s_ratio_A_to_B" % label] = ratio(Fraction(str(lat[(q, "A")])), Fraction(str(lat[(q, "B")])))
    row["latency_runs"] = cfg["queries"]["runs"]
    row["latency_warmup_runs"] = cfg["queries"]["warmup_runs"]
    log("K=%d fields %d/%d, Avro mean %s/%s, zstd %d/%d, q1 p50 %s/%s ms, q3 p50 %s/%s/%s ms" % (
        k, fields["A"], fields["B"], row["avro_uncompressed_mean_bytes_A"], row["avro_uncompressed_mean_bytes_B"],
        pq[("A", "zstd")], pq[("B", "zstd")], lat[("q1_one_hop_previous_single", "A")],
        lat[("q1_one_hop_previous_single", "B")], lat[("q3_current_state_all", "A")],
        lat[("q3_current_state_all", "B")], lat[("q3_current_state_all", "C")]))
    pq_flat = {(d, c): pq[(d, c)] for d in ("A", "B", "C") for c in ("snappy", "zstd")}
    return row, totals, pq_flat


def section(env, rows):
    cfg = env["config"]
    lines = []
    w = lines.append
    w(SECTION_HEADING)
    w("")
    w("Measured by `bench/sweep_attributes.py` from `%s` on %s: FULL model (every event carries every attribute), "
      "seed %d, N=%d, H=%d, K in %s. Machine: %s, %s logical CPUs. DuckDB %s, threads=%s. Wall time %s s. "
      "Field counts, Avro bytes and Parquet bytes reproduce exactly; latencies vary between runs." % (
          env["config_file"], env["started_utc"], cfg["seed"], cfg["n_entities"], cfg["h_events"],
          cfg["attribute_sweep"]["k_values"], env["machine"].get("cpu", env["machine"]["platform"]),
          env["machine"]["cpu_count"], env["duckdb"], env["duckdb_threads"], env["wall_seconds"]))
    w("")
    w("- Attributes beyond the %d in `bench/generate_schemas.py` are string columns named attribute_<i>." %
      len(b.MUTABLE_ATTRIBUTES))
    w("- Avro bytes: fastavro schemaless datum per event, no Confluent framing; deflate is raw DEFLATE at zlib "
      "level %d per event. Mean is the exact mean over all events." % cfg["payload"]["deflate_level"])
    w("- Parquet bytes: DuckDB writer, ROW_GROUP_SIZE %d, sorted by customer_id, event_timestamp, event_sequence." %
      cfg["storage"]["parquet_row_group_size"])
    w("- Latency: p50 (nearest rank) of %d timed `CREATE OR REPLACE TEMP TABLE ... AS <query>` runs after %d warm-up runs "
      "over the snappy file, the protocol of the latency table above." % (cfg["queries"]["runs"], cfg["queries"]["warmup_runs"]))
    w("- Ratio is A divided by B. A ratio that does not terminate shows its first ten decimals followed by \"...\"; "
      "the digits shown are exact, not rounded.")
    if env.get("main_checks"):
        w("- At K=%d the Avro totals and Parquet sizes equal the committed payload.csv and storage.csv values (%d checks)."
          % (cfg["k_attributes"], len(env["main_checks"])))
    w("")
    w("### Schema and payload")
    w("")
    w(b.md_table(["K", "Fields A", "Fields B", "Ratio", "Avro mean A", "Avro mean B", "Ratio",
                  "Avro deflate mean A", "Avro deflate mean B", "Ratio"],
                 [[r["k_attributes"], r["fields_A"], r["fields_B"], r["fields_ratio_A_to_B"],
                   r["avro_uncompressed_mean_bytes_A"], r["avro_uncompressed_mean_bytes_B"], r["avro_uncompressed_mean_ratio_A_to_B"],
                   r["avro_deflate_mean_bytes_A"], r["avro_deflate_mean_bytes_B"], r["avro_deflate_mean_ratio_A_to_B"]]
                  for r in rows]))
    w("")
    w("Medians, per event: " + "; ".join(
        "K=%d uncompressed %s / %s, deflate %s / %s" % (
            r["k_attributes"], r["avro_uncompressed_median_bytes_A"], r["avro_uncompressed_median_bytes_B"],
            r["avro_deflate_median_bytes_A"], r["avro_deflate_median_bytes_B"]) for r in rows) + " (A / B).")
    w("")
    w("### Storage")
    w("")
    w(b.md_table(["K", "Parquet snappy A", "Parquet snappy B", "Ratio", "Parquet zstd A", "Parquet zstd B", "Ratio",
                  "zstd C"],
                 [[r["k_attributes"], b.fmt_bytes(r["parquet_snappy_bytes_A"]), b.fmt_bytes(r["parquet_snappy_bytes_B"]),
                   r["parquet_snappy_ratio_A_to_B"], b.fmt_bytes(r["parquet_zstd_bytes_A"]),
                   b.fmt_bytes(r["parquet_zstd_bytes_B"]), r["parquet_zstd_ratio_A_to_B"],
                   b.fmt_bytes(r["parquet_zstd_bytes_C"])] for r in rows]))
    w("")
    w("### Latency, p50 ms")
    w("")
    w(b.md_table(["K", "q1 one hop A", "q1 one hop B", "Ratio", "q3 current state A", "q3 current state B", "Ratio",
                  "q3 current state C"],
                 [[r["k_attributes"], r["p50_ms_q1_A"], r["p50_ms_q1_B"], r["p50_ms_q1_ratio_A_to_B"],
                   r["p50_ms_q3_A"], r["p50_ms_q3_B"], r["p50_ms_q3_ratio_A_to_B"], r["p50_ms_q3_C"]] for r in rows]))
    w("")
    w("Files: `sweep_attributes.csv`, `sweep_attributes_environment.json`.")
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
    work = tempfile.mkdtemp(prefix="audit-sweep-")
    rows = []
    try:
        for k in cfg["attribute_sweep"]["k_values"]:
            row, totals, pq = run_k(cfg, k, work, log)
            if k == cfg["k_attributes"]:
                env["main_checks"] = check_against_main(args.out, cfg, k, totals, pq)
                log("K=%d matches committed main results: %s" % (k, env["main_checks"]))
            rows.append(row)
            shutil.rmtree(os.path.join(work, "k%d" % k), ignore_errors=True)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    b.write_csv(os.path.join(args.out, "sweep_attributes.csv"), rows, list(rows[0].keys()))
    env["wall_seconds"] = round(time.perf_counter() - t_start, 1)
    env["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    with open(os.path.join(args.out, "sweep_attributes_environment.json"), "w") as fh:
        json.dump(env, fh, indent=2)
    replace_section(os.path.join(args.out, "SUMMARY.md"), SECTION_HEADING, section(env, rows))
    log("sweep_attributes.csv written; SUMMARY.md section written")


if __name__ == "__main__":
    main()
