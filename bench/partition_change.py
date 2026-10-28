#!/usr/bin/env python3
"""LAG across partition changes: entities whose events land in more than one Kafka partition.

For each event model in the config, writes the benchmark's design B (audit trail) events
with bench/pipeline.py's writer (same generator, config and seed as bench/benchmark.py, in
event-time order) and runs com.example.audit.bench.PartitionChange in two scenarios, each
with a fresh topic and table:

  partition-increase   the topic goes from --partitions-before to --partitions-after
                       partitions halfway through; every event is keyed by customer_id and
                       sent with the default partitioner
  partitioner-change   the topic keeps --partitions-before partitions; halfway through, the
                       producer switches from the default partitioner to RoundRobinPartitioner

A Flink job lands the events in Iceberg while they are produced. From the producer's
acknowledgements the script counts the entities whose events ended up in more than one
partition. Over the data files of the Iceberg table's current snapshot it runs, in DuckDB:

  the landed rows               every column of every row, against the generated event
  LAG of every attribute        LAG(x) OVER (PARTITION BY customer_id ORDER BY
                                event_timestamp, event_sequence) at every event, against x
                                on the entity's previous generated event
  the benchmark's LAG queries   q1, q2 and q4 for design B from bench/benchmark.py
                                build_queries(), against answers computed from the events

The expected answers are computed in Python from the generated events, not with SQL. The
script exits non-zero if no entity has events in more than one partition in a run, or if
any row or answer differs.

Needs Kafka: `make up` starts one broker at localhost:9092.

  python3 bench/partition_change.py --config bench/config.json --out results
"""
import argparse
import calendar
import collections
import csv
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

import duckdb
import fastavro

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import benchmark as b  # noqa: E402  same generator and queries
import pipeline as p  # noqa: E402  same event files, classpath and JVM flags
from summary_sections import replace_section  # noqa: E402

SECTION_HEADING = "## Partition changes and LAG"
SCENARIOS = ("partition-increase", "partitioner-change")
LAG_QUERIES = ("q1_one_hop_previous_single", "q2_three_changes_back_single", "q4_previous_value_all")


def ms(ts):
    """Epoch milliseconds of a datetime, exactly (no float arithmetic)."""
    return calendar.timegm(ts.utctimetuple()) * 1000 + ts.microsecond // 1000


def sort_key(row):
    return tuple("\x00" if v is None else str(v) for v in row)


# ------------------------------------------------------------------------ events


def read_events(path, names):
    """Generated events per customer_id, each entity's in (event_timestamp, event_sequence) order."""
    by_entity = collections.defaultdict(list)
    with open(path, "rb") as fh:
        for r in fastavro.reader(fh):
            ev = {"customer_id": r["customer_id"], "ts": ms(r["event_timestamp"]), "seq": r["event_sequence"]}
            for a in names:
                ev[a] = r[a]
            by_entity[ev["customer_id"]].append(ev)
    for evs in by_entity.values():
        evs.sort(key=lambda e: (e["ts"], e["seq"]))
    return dict(sorted(by_entity.items()))


def expected_rows(events, names):
    """Every event's columns, then x on the entity's previous event for every attribute x."""
    for cid, evs in events.items():
        for i, e in enumerate(evs):
            prev = evs[i - 1] if i > 0 else None
            yield ((cid, e["ts"], e["seq"]) + tuple(e[a] for a in names)
                   + tuple(prev[a] if prev else None for a in names))


def expected_queries(events, x, entity):
    """q1, q2 and q4 for design B, computed from the events: LAG over the rows where x is set."""
    def setx(evs):
        return [e for e in evs if e[x] is not None]
    single = setx(events.get(entity, []))
    q1 = [(e["ts"], e["seq"], e[x], single[i - 1][x] if i >= 1 else None) for i, e in enumerate(single)]
    q2 = [(e["ts"], e["seq"], e[x], single[i - 3][x] if i >= 3 else None) for i, e in enumerate(single)]
    q4 = []
    for cid, evs in events.items():
        s = setx(evs)
        if s:
            q4.append((cid, s[-1][x], s[-2][x] if len(s) >= 2 else None))
    return {LAG_QUERIES[0]: q1, LAG_QUERIES[1]: q2, LAG_QUERIES[2]: q4}


# ------------------------------------------------------------------------- java


def run_java(cp, args, log):
    cmd = ["java", "-XX:+IgnoreUnrecognizedVMOptions"] + ["--add-opens=java.base/%s=ALL-UNNAMED" % x for x in p.ADD_OPENS]
    cmd += ["-cp", cp, "com.example.audit.bench.PartitionChange"]
    for k, v in args.items():
        cmd += ["--" + k, str(v)]
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, cwd=p.ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if proc.returncode != 0:
        raise RuntimeError("PartitionChange failed (exit %d):\n%s" % (proc.returncode, "\n".join(proc.stdout.splitlines()[-40:])))
    for line in proc.stdout.splitlines():
        if line.startswith(SCENARIOS):
            log("    " + line)
    log("    java finished in %.1fs" % (time.perf_counter() - t0))


# ----------------------------------------------------------------------- checks


def partition_stats(path, before):
    partitions = collections.defaultdict(set)
    later_new = set()
    per_partition = collections.Counter()
    with open(path, newline="") as fh:
        for r in csv.DictReader(fh):
            part = int(r["partition"])
            partitions[r["customer_id"]].add(part)
            per_partition[part] += 1
            if r["phase"] == "2" and part >= before:
                later_new.add(r["customer_id"])
    return {"entities_in_more_than_one_partition": sum(1 for s in partitions.values() if len(s) > 1),
            "max_partitions_per_entity": max(len(s) for s in partitions.values()),
            "entities_with_later_events_in_an_added_partition": len(later_new),
            "records_per_partition": " ".join("%d:%d" % kv for kv in sorted(per_partition.items()))}


def check_table(files, events, attrs, model):
    names = [a for a, _ in attrs]
    con = duckdb.connect()
    con.execute("CREATE VIEW b AS SELECT * FROM read_parquet([%s])" % ", ".join("'%s'" % f for f in files))
    out = {"iceberg_rows": con.execute("SELECT count(*) FROM b").fetchone()[0],
           "iceberg_distinct_keys": con.execute("SELECT count(DISTINCT (customer_id, event_sequence)) FROM b").fetchone()[0]}

    cur = con.execute("SELECT customer_id, epoch_ms(event_timestamp), event_sequence, %s, %s FROM b "
                      "WINDOW w AS (PARTITION BY customer_id ORDER BY event_timestamp, event_sequence) "
                      "ORDER BY customer_id, event_timestamp, event_sequence"
                      % (", ".join(names), ", ".join("LAG(%s) OVER w" % a for a in names)))
    k = len(names)
    rows = row_diffs = lag_values = 0
    lag_diffs = collections.Counter()
    expected = expected_rows(events, names)
    while True:
        chunk = cur.fetchmany(10_000)
        if not chunk:
            break
        for got in chunk:
            want = next(expected, None)
            rows += 1
            if want is None or tuple(got[:3 + k]) != want[:3 + k]:
                row_diffs += 1
            if want is None:
                continue
            for i, a in enumerate(names):
                lag_values += 1
                if got[3 + k + i] != want[3 + k + i]:
                    lag_diffs[a] += 1
    missing = sum(1 for _ in expected)
    out.update({"rows_compared": rows, "rows_differing_from_events": row_diffs + missing,
                "lag_fields": k, "lag_values_compared": lag_values,
                "lag_differences": sum(lag_diffs.values()),
                "lag_fields_with_differences": " ".join("%s:%d" % kv for kv in sorted(lag_diffs.items()))})

    x = names[0]
    entity = sorted(events)[len(events) // 2]
    want = expected_queries(events, x, entity)
    sql = {qid: s for qid, _, _, d, s, _ in b.build_queries(attrs, model, entity) if d == "B" and qid in LAG_QUERIES}
    for qid in LAG_QUERIES:
        got = [tuple(ms(v) if hasattr(v, "utctimetuple") else v for v in r) for r in con.execute(sql[qid]).fetchall()]
        equal = sorted(got, key=sort_key) == sorted(want[qid], key=sort_key)
        out["%s_rows" % qid.split("_")[0]] = len(got)
        out["%s_equal" % qid.split("_")[0]] = "yes" if equal else "no"
    out["benchmark_entity"] = entity
    con.close()
    return out


# ---------------------------------------------------------------------- summary


def section(env, rows):
    cfg = env["config"]
    first = env["runs"][0]["run"]
    lines = [SECTION_HEADING, ""]
    w = lines.append
    w("Measured by `bench/partition_change.py` from `%s` on %s: the benchmark's design B (audit trail) events, "
      "synthetic, seed %d, N=%d entities x H=%d events, K=%d, per event model, sent to a local Kafka broker (%s) keyed "
      "by customer_id while a Flink %s job appended them to an Iceberg %s table (parallelism %d, checkpoints every %d "
      "ms, Kafka partition discovery every %d ms, Kafka client %s). Halfway through, in event-time order, the "
      "assignment of records to partitions changed:" % (
          env["config_file"], env["started_utc"], cfg["seed"], cfg["n_entities"], cfg["h_events"],
          cfg["k_attributes"], env["kafka_broker_image"], first["flink"], first["iceberg"], first["flink_parallelism"],
          first["checkpoint_interval_ms"], first["partition_discovery_interval_ms"], first["kafka_client"]))
    w("")
    w("- partition-increase: the topic went from %d to %d partitions while the job ran; every event was keyed by "
      "customer_id and sent with the producer's default partitioner (murmur2 of the key modulo the partition count)."
      % (env["args"]["partitions_before"], env["args"]["partitions_after"]))
    w("- partitioner-change: %d partitions throughout; the first half was sent with the default partitioner, the "
      "second half with RoundRobinPartitioner, which ignores the key." % env["args"]["partitions_before"])
    w("")
    w("Partitions come from the producer's acknowledgements. DuckDB %s ran the checks over the data files of the "
      "table's current snapshot; the expected answers were computed in Python from the generated events, not with "
      "SQL. Rows equal to the events compares every column of every row. LAG values compares LAG(x) OVER "
      "(PARTITION BY customer_id ORDER BY event_timestamp, event_sequence) for each of the %d attributes at every "
      "event with x on the entity's previous generated event. q1, q2 and q4 are the benchmark's design B LAG "
      "queries from `bench/benchmark.py`." % (env["duckdb"], cfg["k_attributes"]))
    w("")
    w(b.md_table(["Model", "Scenario", "Partitions", "Events", "Entities", "Entities with events in more than one "
                  "partition", "Iceberg rows", "Rows equal to the events", "LAG values compared", "LAG differences",
                  "q1, q2, q4 equal to expected"],
                 [[r["model"], r["scenario"], str(r["partitions_before"]) if r["partitions_before"] == r["partitions_after"]
                   else "%s to %s" % (r["partitions_before"], r["partitions_after"]),
                   b.fmt_bytes(r["events"]), b.fmt_bytes(r["entities"]),
                   b.fmt_bytes(r["entities_in_more_than_one_partition"]), b.fmt_bytes(r["iceberg_rows"]),
                   "%s of %s" % (b.fmt_bytes(r["rows_compared"] - r["rows_differing_from_events"]),
                                 b.fmt_bytes(r["events"])),
                   b.fmt_bytes(r["lag_values_compared"]), r["lag_differences"],
                   "%s, %s, %s" % (r["q1_equal"], r["q2_equal"], r["q4_equal"])] for r in rows]))
    w("")
    ok = all(r["result"] == "pass" for r in rows)
    w("%s Records per partition, entities with later events in an added partition and each run's settings are in "
      "`partition_change.csv` and `partition_change_environment.json`." % (
          "In every run some entities had events in more than one partition, and every row and every LAG answer "
          "equalled the expected one." if ok else "At least one run failed a check; see `partition_change.csv`."))
    w("")
    return "\n".join(lines)


def kafka_image():
    try:
        out = subprocess.run(["docker", "ps", "--filter", "publish=9092", "--format", "{{.Image}}"],
                             stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, timeout=20).stdout.split()
        return out[0] if out else "unknown"
    except Exception:
        return "unknown"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="bench/config.json")
    ap.add_argument("--out", default="results")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--partitions-before", type=int, default=3)
    ap.add_argument("--partitions-after", type=int, default=6)
    ap.add_argument("--parallelism", type=int, default=3)
    ap.add_argument("--checkpoint-interval-ms", type=int, default=1000)
    ap.add_argument("--discovery-interval-ms", type=int, default=1000)
    args = ap.parse_args()
    cfg = json.load(open(args.config))
    t_start = time.perf_counter()

    def log(msg):
        print("[%7.1fs] %s" % (time.perf_counter() - t_start, msg), flush=True)

    os.makedirs(args.out, exist_ok=True)
    env = {"started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "config_file": args.config,
           "config": cfg, "args": {k: v for k, v in vars(args).items() if k not in ("config", "out")},
           "machine": b.machine_info(), "duckdb": duckdb.__version__, "kafka_broker_image": kafka_image(),
           "java": subprocess.run(["java", "-version"], stderr=subprocess.PIPE, text=True).stderr.splitlines()[0],
           "runs": []}
    attrs = b.attributes(cfg["k_attributes"])
    names = [a for a, _ in attrs]
    cp = p.classpath(log)
    work = tempfile.mkdtemp(prefix="audit-partition-change-")
    rows = []
    try:
        for model in cfg["event_models"]:
            events_file = p.write_events(cfg, model, work, log)["B"]
            events = read_events(events_file, names)
            for scenario in SCENARIOS:
                run_dir = os.path.join(work, "%s-%s" % (model, scenario))
                log("%s %s" % (model, scenario))
                run_java(cp, {"events": events_file, "scenario": scenario, "bootstrap": args.bootstrap,
                              "partitions-before": args.partitions_before, "partitions-after": args.partitions_after,
                              "parallelism": args.parallelism, "checkpoint-interval-ms": args.checkpoint_interval_ms,
                              "discovery-interval-ms": args.discovery_interval_ms, "work": run_dir}, log)
                run = json.load(open(os.path.join(run_dir, "run.json")))
                files = open(os.path.join(run_dir, "files.txt")).read().split()
                row = {"model": model, "scenario": scenario, "events": run["events"], "entities": len(events),
                       "partitions_before": run["partitions_before"], "partitions_after": run["partitions_after"],
                       "phase1_partitioner": run["phase1_partitioner"], "phase2_partitioner": run["phase2_partitioner"],
                       "records_in_topic": run["records_in_topic"]}
                row.update(partition_stats(os.path.join(run_dir, "records.csv"), run["partitions_before"]))
                row["iceberg_data_files"] = len(files)
                row.update(check_table(files, events, attrs, model))
                passed = (row["entities_in_more_than_one_partition"] > 0 and row["iceberg_rows"] == row["events"]
                          and row["iceberg_distinct_keys"] == row["events"] and row["rows_differing_from_events"] == 0
                          and row["lag_differences"] == 0 and all(row["%s_equal" % q] == "yes" for q in ("q1", "q2", "q4")))
                row["result"] = "pass" if passed else "fail"
                rows.append(row)
                env["runs"].append({"model": model, "scenario": scenario, "run": run})
                log("    %d of %d entities in more than one partition; rows differing %d; LAG differences %d of %d; "
                    "q1 %s q2 %s q4 %s; %s" % (row["entities_in_more_than_one_partition"], row["entities"],
                                               row["rows_differing_from_events"], row["lag_differences"],
                                               row["lag_values_compared"], row["q1_equal"], row["q2_equal"],
                                               row["q4_equal"], row["result"]))
    finally:
        shutil.rmtree(work, ignore_errors=True)

    env["wall_seconds"] = round(time.perf_counter() - t_start, 1)
    env["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    b.write_csv(os.path.join(args.out, "partition_change.csv"), rows, list(rows[0].keys()))
    with open(os.path.join(args.out, "partition_change_environment.json"), "w") as fh:
        json.dump(env, fh, indent=2)
    summary = os.path.join(args.out, "SUMMARY.md")
    if not os.path.exists(summary):
        open(summary, "w").close()
    replace_section(summary, SECTION_HEADING, section(env, rows))
    log("partition_change.csv and SUMMARY.md section written")
    if any(r["result"] != "pass" for r in rows):
        sys.exit(1)


if __name__ == "__main__":
    main()
