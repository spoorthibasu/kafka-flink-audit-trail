#!/usr/bin/env python3
"""LAG against every previous-value field, at every event of the main FULL dataset.

Regenerates the main FULL dataset of bench/benchmark.py from the same config and seed,
writes designs A, B and C to Parquet with the main benchmark's first compression and
row-group setting, and runs bench/benchmark.py check_equivalence() on those files: the
four comparisons equivalence.csv records for main_full, and the wider comparison
previous_field_differences(), which checks every previous_x design A stores against
LAG(x) over design B, for all K attributes and every event. Nothing is timed.

Checks that the regenerated files are the benchmark's files (their sizes equal
storage.csv for main FULL) and that the four existing comparisons give the rows
equivalence.csv records. Two control runs then plant one change each in design A, a
stored previous value set to null and an event key moved, and confirm the wider
comparison reports exactly that change.

Writes lag_previous_fields.csv, lag_previous_fields_environment.json and the "LAG
against every previous-value field" section of SUMMARY.md. No other results file is
touched.

  python3 bench/lag_previous_fields.py --config bench/config.json --out results
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
import benchmark as b  # noqa: E402  same generator, Parquet writer and checks
from summary_sections import replace_section  # noqa: E402

SECTION_HEADING = "## LAG against every previous-value field"
MODEL = "full"
TAG = "main_full"


def check_storage(out_dir, n, h, sizes):
    """The regenerated files must equal storage.csv's main FULL sizes."""
    found = 0
    for row in csv.DictReader(open(os.path.join(out_dir, "storage.csv"))):
        key = (row["design"], row["parquet_compression"])
        if row["model"] == MODEL and int(row["n_entities"]) == n and int(row["h_events"]) == h and key in sizes:
            found += 1
            if int(row["parquet_bytes"]) != sizes[key]:
                raise RuntimeError("%s: %d bytes differs from storage.csv %s" % (key, sizes[key], row["parquet_bytes"]))
    if found != len(sizes):
        raise RuntimeError("storage.csv has %d of %d expected rows" % (found, len(sizes)))
    return found


def check_committed(out_dir, records):
    """The four existing comparisons must match equivalence.csv's main_full rows."""
    committed = [r for r in csv.DictReader(open(os.path.join(out_dir, "equivalence.csv"))) if r["dataset"] == TAG]
    key = lambda r: (r["query"], r["designs"], r["columns"], str(r["rows_compared"]), r["equal"])  # noqa: E731
    again = [r for r in records if r["query"] != "lag_every_previous_field"]
    if sorted(map(key, committed)) != sorted(map(key, again)):
        raise RuntimeError("the existing comparisons differ from equivalence.csv: %s vs %s" % (again, committed))
    return len(committed)


def control(parquet, attrs, replace):
    """previous_field_differences() over design A with one column replaced, B unchanged.
    Returns (rows in A, joined rows, unmatched rows, {attribute: differences} for the
    attributes with any)."""
    con = duckdb.connect()
    con.execute("CREATE VIEW b AS SELECT * FROM read_parquet('%s')" % parquet["b"])
    con.execute("CREATE VIEW a AS SELECT * REPLACE (%s) FROM read_parquet('%s')" % (replace, parquet["a"]))
    rows_a, _, joined, unmatched, fields = b.previous_field_differences(con, attrs)
    con.close()
    return rows_a, joined, unmatched, {a: d for a, _, d in fields if d}


def controls(parquet, attrs):
    """Two planted changes to design A that the comparison must report exactly:
    previous_x set to null on one event (one difference, in that field only), and one
    event's event_sequence moved up by one (that A row and its B row unmatched)."""
    x = attrs[-1][0]
    con = duckdb.connect()
    cid, seq = con.execute("SELECT customer_id, event_sequence FROM read_parquet('%s') WHERE previous_%s IS NOT NULL "
                           "ORDER BY customer_id, event_sequence LIMIT 1" % (parquet["a"], x)).fetchone()
    last_cid, last_seq = con.execute("SELECT customer_id, max(event_sequence) FROM read_parquet('%s') "
                                     "GROUP BY customer_id ORDER BY customer_id DESC LIMIT 1" % parquet["a"]).fetchone()
    con.close()
    rows_a, joined, unmatched, differing = control(
        parquet, attrs, "CASE WHEN customer_id = '%s' AND event_sequence = %d THEN NULL ELSE previous_%s END AS previous_%s"
        % (cid, seq, x, x))
    value_caught = joined == rows_a and unmatched == 0 and differing == {x: 1}
    rows_a2, joined2, unmatched2, _ = control(
        parquet, attrs, "CASE WHEN customer_id = '%s' AND event_sequence = %d THEN event_sequence + 1 "
        "ELSE event_sequence END AS event_sequence" % (last_cid, last_seq))
    key_caught = joined2 == rows_a2 + 1 and unmatched2 == 2
    return {"control_value_field": x, "control_value_event": "%s seq %d" % (cid, seq),
            "control_value_differences": differing, "control_value_unmatched": unmatched,
            "control_value_caught": value_caught,
            "control_key_event": "%s seq %d" % (last_cid, last_seq), "control_key_unmatched": unmatched2,
            "control_key_joined_rows": joined2, "control_key_caught": key_caught}


def section(env, fields):
    cfg = env["config"]
    events = env["rows_a"]
    lines = [SECTION_HEADING, ""]
    w = lines.append
    w("Measured by `bench/lag_previous_fields.py` from `%s` on %s: the main FULL dataset of `bench/benchmark.py`, "
      "synthetic, seed %d, N=%d entities x H=%d events, K=%d. DuckDB %s. Nothing is timed; no timing, size or cost "
      "result above changes." % (env["config_file"], env["started_utc"], cfg["seed"], cfg["n_entities"],
                                 cfg["h_events"], cfg["k_attributes"], env["duckdb"]))
    w("")
    w("The equivalence checks above compare the previous value of the first attribute, %s, only (q1 for one entity, "
      "q4 at each entity's newest event). `check_equivalence()` in `bench/benchmark.py` now also compares, before "
      "timing, every previous-value field design A stores with LAG over design B: for each of the %d attributes x, "
      "previous_x on every A row against LAG(x) OVER (PARTITION BY customer_id ORDER BY event_timestamp, "
      "event_sequence) on the B row with the same customer_id, event_timestamp and event_sequence, joined with a "
      "full outer join, compared with IS DISTINCT FROM. This section ran that check on the same Parquet files the "
      "benchmark reads." % (fields[0]["attribute"], len(fields)))
    w("")
    differing = sum(f["differing_events"] for f in fields)
    w(b.md_table(["Fields", "Rows in A", "Rows in B", "Joined rows", "Unmatched rows", "Field values compared",
                  "Field values set in A", "Differences", "Equal"],
                 [[len(fields), b.fmt_bytes(env["rows_a"]), b.fmt_bytes(env["rows_b"]), b.fmt_bytes(env["joined_rows"]),
                   env["unmatched_rows"], b.fmt_bytes(events * len(fields)),
                   b.fmt_bytes(sum(f["previous_set_in_a"] for f in fields)), differing,
                   "yes" if differing == 0 and env["unmatched_rows"] == 0 else "no"]]))
    w("")
    w("Each field was compared at all %s events. Per-field rows, with the differences and the events where A stores "
      "a non-null previous value, are in `lag_previous_fields.csv`." % b.fmt_bytes(events))
    w("")
    w("Same files and same answers as the benchmark: the regenerated Parquet files equal `storage.csv` for main "
      "FULL (%d checks, designs A, B and C, %s), and the four existing comparisons gave the rows `equivalence.csv` "
      "records for main_full (%d of %d)." % (env["storage_checks"], cfg["storage"]["parquet_compressions"][0],
                                             env["committed_matches"], env["committed_matches"]))
    w("")
    w("Controls, each on a copy of design A with one planted change and B unchanged: previous_%s set to null on one "
      "event (%s) gave %s and %d unmatched rows; the event_sequence of one event (%s) moved up by one gave %d "
      "unmatched rows, that A row and the B row it no longer matches." % (
          env["control_value_field"], env["control_value_event"],
          ", ".join("%d difference%s in previous_%s" % (d, "" if d == 1 else "s", a)
                    for a, d in sorted(env["control_value_differences"].items())),
          env["control_value_unmatched"], env["control_key_event"], env["control_key_unmatched"]))
    w("")
    w("Files: `lag_previous_fields.csv`, `lag_previous_fields_environment.json`.")
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
           "config": cfg, "machine": b.machine_info(), "duckdb": duckdb.__version__}
    attrs = b.attributes(cfg["k_attributes"])
    n, h = cfg["n_entities"], cfg["h_events"]
    comp = cfg["storage"]["parquet_compressions"][0]
    rg = cfg["storage"]["parquet_row_group_size"]
    work = tempfile.mkdtemp(prefix="audit-lag-fields-")
    try:
        # The steps of bench/benchmark.py run_dataset() up to its equivalence checks.
        b.generate(cfg, MODEL, n, h, attrs, work, None)
        con = duckdb.connect()
        b.load_csv(con, "a", os.path.join(work, "a.csv"), b.columns_sql(attrs, True))
        b.load_csv(con, "b", os.path.join(work, "b.csv"), b.columns_sql(attrs, False))
        b.load_csv(con, "c", os.path.join(work, "c.csv"), b.columns_sql(attrs, False))
        parquet, sizes = {}, {}
        for design, table in (("A", "a"), ("B", "b"), ("C", "c")):
            parquet[table] = os.path.join(work, "%s_%s.parquet" % (table, comp))
            sizes[(design, comp)] = b.write_parquet(con, table, parquet[table], comp, rg)
        env["storage_checks"] = check_storage(args.out, n, h, sizes)
        log("generated and wrote %s; Parquet sizes equal storage.csv" % TAG)
        for table in ("a", "b", "c"):
            con.execute("DROP TABLE %s" % table)
            con.execute("CREATE VIEW %s AS SELECT * FROM read_parquet('%s')" % (table, parquet[table]))
        records = b.check_equivalence(con, attrs, MODEL, b.build_queries(attrs, MODEL, "C%07d" % (n // 2)))
        env["committed_matches"] = check_committed(args.out, records)
        rows_a, rows_b, joined, unmatched, diffs = b.previous_field_differences(con, attrs)
        con.close()
        log("check_equivalence passed: %s" % ["%s %s %s" % (r["query"], r["designs"], r["rows_compared"]) for r in records])
        env.update(controls(parquet, attrs))
        log("controls: %s" % {k: v for k, v in env.items() if k.startswith("control_")})
        if not (env["control_value_caught"] and env["control_key_caught"]):
            raise RuntimeError("a planted change was not reported as expected")
    finally:
        shutil.rmtree(work, ignore_errors=True)

    types = dict(attrs)
    fields = [{"dataset": TAG, "model": MODEL, "attribute": a, "type": types[a], "events": joined,
               "previous_set_in_a": stored, "differing_events": d} for a, stored, d in diffs]
    env.update({"rows_a": rows_a, "rows_b": rows_b, "joined_rows": joined, "unmatched_rows": unmatched,
                "fields": len(fields), "differing_field_values": sum(f["differing_events"] for f in fields),
                "wall_seconds": round(time.perf_counter() - t_start, 1),
                "finished_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())})
    b.write_csv(os.path.join(args.out, "lag_previous_fields.csv"), fields, list(fields[0].keys()))
    with open(os.path.join(args.out, "lag_previous_fields_environment.json"), "w") as fh:
        json.dump(env, fh, indent=2)
    replace_section(os.path.join(args.out, "SUMMARY.md"), SECTION_HEADING, section(env, fields))
    log("lag_previous_fields.csv written; SUMMARY.md section written")


if __name__ == "__main__":
    main()
