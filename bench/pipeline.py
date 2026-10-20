#!/usr/bin/env python3
"""Pipeline-level measurements for designs A (previous-value) and B (audit trail).

1. Writes the benchmark's events (same generator, config and seed as bench/benchmark.py)
   to Avro container files, one per model and design, in event-time order.
2. Runs com.example.audit.bench.PipelineBenchmark in "kafka" mode for each model, design
   and producer compression, and in "iceberg" mode for each model and design. Each run
   appends a row to pipeline_kafka_runs.csv or pipeline_iceberg_runs.csv.
3. Writes the medians over runs to pipeline_kafka.csv and pipeline_iceberg.csv and the
   "Pipeline measurements" section of SUMMARY.md.

Kafka must be running: `make kafka3-up` starts three brokers for the full configuration;
the smoke configuration uses one broker at localhost:9092.

  python3 bench/pipeline.py --config bench/config.json --out results
"""
import argparse
import csv
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from fractions import Fraction

import fastavro

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import benchmark as b  # noqa: E402
from summary_sections import replace_section, exact_or_prefix  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SECTION_HEADING = "## Pipeline measurements"
KAFKA_RUNS = "pipeline_kafka_runs.csv"
ICEBERG_RUNS = "pipeline_iceberg_runs.csv"
ADD_OPENS = ["java.lang", "java.net", "java.io", "java.nio", "sun.nio.ch", "java.lang.reflect", "java.util",
             "java.util.concurrent", "java.util.concurrent.atomic", "java.time", "java.text"]
DESIGN_NAMES = {"A": "A previous-value schema", "B": "B audit trail"}


# ---------------------------------------------------------------------- events


def write_events(cfg, model, work, log):
    """Avro container files of the model's events for A and B, sorted by event time."""
    n, h = cfg["n_entities"], cfg["h_events"]
    attrs = b.attributes(cfg["k_attributes"])
    data_dir = os.path.join(work, "csv-" + model)
    os.makedirs(data_dir, exist_ok=True)
    b.generate(cfg, model, n, h, attrs, data_dir, None)
    types = dict(attrs)
    paths = {}
    for design, csv_name, with_prev in (("A", "a.csv", True), ("B", "b.csv", False)):
        schema = fastavro.parse_schema(b.avro_schema("PreviousValue" if with_prev else "AuditTrail", attrs, with_prev))
        records = []
        with open(os.path.join(data_dir, csv_name), newline="") as fh:
            for row in csv.DictReader(fh):
                rec = {"customer_id": row["customer_id"], "event_timestamp": int(row["event_timestamp"]),
                       "event_sequence": int(row["event_sequence"])}
                for col, val in row.items():
                    if col in rec:
                        continue
                    typ = types[col[len("previous_"):] if col.startswith("previous_") else col]
                    rec[col] = None if val == "" else (val if typ == "string" else int(val) if typ == "long"
                                                       else float(val) if typ == "double" else val == "true")
                records.append(rec)
        records.sort(key=lambda r: (r["event_timestamp"], r["customer_id"]))
        path = os.path.join(work, "events_%s_%s.avro" % (model, design))
        with open(path, "wb") as fh:
            fastavro.writer(fh, schema, records)
        paths[design] = path
        log("  events %s %s: %d records -> %s" % (model, design, len(records), path))
    shutil.rmtree(data_dir, ignore_errors=True)
    return paths


# ------------------------------------------------------------------------ java


def classpath(log):
    cp_file = os.path.join(ROOT, "target", "pipeline-classpath.txt")
    subprocess.run(["mvn", "-q", "-B", "compile", "dependency:build-classpath",
                    "-Dmdep.outputFile=" + cp_file, "-Dmdep.includeScope=compile"], cwd=ROOT, check=True)
    with open(cp_file) as fh:
        cp = fh.read().strip()
    log("compiled; classpath has %d entries" % len(cp.split(os.pathsep)))
    return os.path.join(ROOT, "target", "classes") + os.pathsep + cp


def run_java(cp, mode, args, log):
    cmd = ["java", "-XX:+IgnoreUnrecognizedVMOptions"] + ["--add-opens=java.base/%s=ALL-UNNAMED" % p for p in ADD_OPENS]
    cmd += ["-cp", cp, "com.example.audit.bench.PipelineBenchmark", mode]
    for k, v in args.items():
        cmd += ["--" + k, str(v)]
    log("  java %s %s" % (mode, " ".join("--%s %s" % kv for kv in args.items() if kv[0] not in ("events", "work", "out"))))
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    for line in proc.stdout.splitlines():
        if line.startswith(("full ", "sparse ")) or "Exception" in line or "Error" in line:
            log("    " + line[:300])
    if proc.returncode != 0:
        tail = "\n".join(proc.stdout.splitlines()[-40:])
        raise RuntimeError("PipelineBenchmark %s failed (exit %d):\n%s" % (mode, proc.returncode, tail))
    log("  java %s finished in %.1fs" % (mode, time.perf_counter() - t0))


# --------------------------------------------------------------------- summary


def med(values):
    return statistics.median([Fraction(v) for v in values])


def summarize(out_dir, cfg, pcfg, env):
    k_rows = list(csv.DictReader(open(os.path.join(out_dir, KAFKA_RUNS))))
    i_rows = list(csv.DictReader(open(os.path.join(out_dir, ICEBERG_RUNS))))
    kafka = []
    for model in pcfg["models"]:
        for design in ("A", "B"):
            for comp in pcfg["compressions"]:
                rs = [r for r in k_rows if r["model"] == model and r["schema"] == design and r["compression"] == comp]
                if not rs:
                    continue
                n = int(rs[0]["events"])
                rf = int(rs[0]["replication_factor"])
                one = med([r["topic_log_bytes_one_copy"] for r in rs])
                thr = med([Fraction(n * 1000, int(r["flink_all_records_seen_ts_ms"]) - int(r["flink_running_ts_ms"]))
                           for r in rs])
                kafka.append({
                    "model": model, "schema": design, "compression": comp, "runs": len(rs), "events": n,
                    "partitions": rs[0]["partitions"], "replication_factor": rf,
                    "min_insync_replicas": rs[0]["min_insync_replicas"], "acks": rs[0]["acks"],
                    "topic_log_bytes_one_copy_median": exact_or_prefix(one),
                    "topic_log_bytes_all_replicas_median": exact_or_prefix(med([r["topic_log_bytes_all_replicas"] for r in rs])),
                    "topic_log_bytes_per_event_one_copy": exact_or_prefix(one / n),
                    "replication_traffic_bytes_derived": exact_or_prefix(one * (rf - 1)),
                    "producer_latency_p50_ms_median": exact_or_prefix(med([r["producer_latency_p50_ns"] for r in rs]) / 1_000_000),
                    "producer_latency_p95_ms_median": exact_or_prefix(med([r["producer_latency_p95_ns"] for r in rs]) / 1_000_000),
                    "producer_latency_p99_ms_median": exact_or_prefix(med([r["producer_latency_p99_ns"] for r in rs]) / 1_000_000),
                    "flink_throughput_events_per_s_median": exact_or_prefix(thr),
                    "flink_checkpoint_end_to_end_ms_median": exact_or_prefix(med([r["flink_checkpoint_end_to_end_ms"] for r in rs])),
                    "flink_checkpoint_checkpointed_size_median": exact_or_prefix(med([r["flink_checkpoint_checkpointed_size"] for r in rs])),
                    "flink_checkpoints_completed_median": exact_or_prefix(med([r["flink_checkpoints_completed"] for r in rs])),
                    "flink_parallelism": rs[0]["flink_parallelism"],
                    "flink_checkpoint_interval_ms": rs[0]["flink_checkpoint_interval_ms"],
                    "flink_min_pause_ms": rs[0]["flink_min_pause_ms"],
                })
    iceberg = []
    metrics = ["snapshots", "metadata_total_bytes", "metadata_json_files", "metadata_json_bytes", "manifest_list_files",
               "manifest_list_bytes", "manifest_files", "manifest_bytes", "manifests_in_current_snapshot",
               "data_files_in_current_snapshot", "data_files_on_disk", "data_bytes_on_disk", "rows",
               "distinct_customer_sequence"]
    for model in pcfg["models"]:
        for design in ("A", "B"):
            for state in ("after_commits", "after_expiration", "after_compaction", "after_compaction_and_expiration"):
                rs = [r for r in i_rows if r["model"] == model and r["schema"] == design and r["state"] == state]
                if not rs:
                    continue
                row = {"model": model, "schema": design, "state": state, "runs": len(rs), "commits": rs[0]["commits"]}
                for m in metrics:
                    row[m + "_median"] = exact_or_prefix(med([r[m] for r in rs]))
                row["history_digest_matches_in_all_runs"] = "yes" if all(
                    r["history_digest_matches_after_commits"] == "yes" for r in rs) else "no"
                iceberg.append(row)
    b.write_csv(os.path.join(out_dir, "pipeline_kafka.csv"), kafka, list(kafka[0].keys()))
    b.write_csv(os.path.join(out_dir, "pipeline_iceberg.csv"), iceberg, list(iceberg[0].keys()))
    summary_path = os.path.join(out_dir, "SUMMARY.md")
    if not os.path.exists(summary_path):
        open(summary_path, "w").write("# Benchmark summary\n")
    replace_section(summary_path, SECTION_HEADING, section(cfg, pcfg, env, kafka, iceberg))


def section(cfg, pcfg, env, kafka, iceberg):
    lines = []
    w = lines.append
    w(SECTION_HEADING)
    w("")
    w("Measured by `bench/pipeline.py` from `%s` on %s: designs A and B through the Kafka to Flink to Iceberg "
      "topology of CustomerProfileAuditJob, seed %d, N=%d, H=%d (%s events per run), K=%d, models %s. Medians over "
      "%d runs. Machine: %s, %s logical CPUs. Kafka %s brokers (apache/kafka:3.7.0 in Docker on the same machine), "
      "Flink 1.20.3 embedded, Iceberg 1.10.1 on the local filesystem. Wall time %s s. Synthetic data at "
      "demonstration scale." % (
          env["config_file"], env["started_utc"], cfg["seed"], cfg["n_entities"], cfg["h_events"],
          "{:,}".format(cfg["n_entities"] * cfg["h_events"]), cfg["k_attributes"], ", ".join(pcfg["models"]),
          pcfg["runs"], env["machine"].get("cpu", env["machine"]["platform"]), env["machine"]["cpu_count"],
          pcfg["replication_factor"] if pcfg["replication_factor"] > 1 else "1", env["wall_seconds"]))
    w("")
    w("- Events: written by this script from the benchmark generator, sent in event-time "
      "order as Avro binary datums with no Confluent framing, keyed by customer_id, to a fresh topic per run with %d "
      "partitions, replication factor %d and min.insync.replicas %d. The job deserializes them with the same Avro "
      "schema into Iceberg rows." % (pcfg["partitions"], pcfg["replication_factor"], min(2, pcfg["replication_factor"])))
    w("- Broker storage: the topic's log bytes per replica from the brokers' log directories (AdminClient "
      "describeLogDirs), read once every replica matches its leader. One copy is the sum of the leader replicas; all "
      "replicas is the sum over brokers. Replication traffic is derived, not measured: one-copy bytes x "
      "(replication factor - 1).")
    w("- Producer latency: time from each send() call to its acknowledgement callback, acks=all, Kafka client 3.4.0 "
      "defaults otherwise; nearest-rank p50, p95 and p99 over all events of a run, then the median over runs. Events are "
      "sent back to back with no pacing, so the time includes each record's wait in the producer's buffer behind earlier "
      "records; it is latency at the producer's maximum send rate, not for an isolated event.")
    w("- Flink: parallelism %d, checkpoint every %d ms, minimum pause %d ms, exactly-once, noWatermarks, the "
      "reference job's settings. Throughput = events / (time the source operator's numRecordsOut, summed over "
      "subtasks, first equals the event count - the job's RUNNING timestamp), both from the Flink REST API, polled "
      "every %d ms with the metric fetcher set to 100 ms. Checkpoint duration and size are the REST checkpoint "
      "statistics (end_to_end_duration, checkpointed_size) of the checkpoint whose Iceberg commit made every event "
      "visible." % (pcfg["parallelism"], pcfg["checkpoint_interval_ms"], pcfg["min_pause_ms"], pcfg["poll_ms"]))
    w("")
    for model in pcfg["models"]:
        rows = [r for r in kafka if r["model"] == model]
        w("### %s model: Kafka broker storage" % model.upper())
        w("")
        w(b.md_table(["Design", "Compression", "Log bytes, one copy", "Bytes per event, one copy", "Log bytes, all replicas",
                      "Replication traffic bytes (derived)"],
                     [[DESIGN_NAMES[r["schema"]], r["compression"], r["topic_log_bytes_one_copy_median"],
                       r["topic_log_bytes_per_event_one_copy"], r["topic_log_bytes_all_replicas_median"],
                       r["replication_traffic_bytes_derived"]] for r in rows]))
        w("")
        w("### %s model: producer latency, send to acknowledgement, ms" % model.upper())
        w("")
        w(b.md_table(["Design", "Compression", "p50", "p95", "p99"],
                     [[DESIGN_NAMES[r["schema"]], r["compression"], r["producer_latency_p50_ms_median"],
                       r["producer_latency_p95_ms_median"], r["producer_latency_p99_ms_median"]] for r in rows]))
        w("")
        w("### %s model: Flink throughput and checkpoints" % model.upper())
        w("")
        w(b.md_table(["Design", "Topic compression", "Events per second", "Checkpoint end-to-end ms",
                      "Checkpointed size bytes", "Checkpoints completed"],
                     [[DESIGN_NAMES[r["schema"]], r["compression"], r["flink_throughput_events_per_s_median"],
                       r["flink_checkpoint_end_to_end_ms_median"], r["flink_checkpoint_checkpointed_size_median"],
                       r["flink_checkpoints_completed_median"]] for r in rows]))
        w("")
    w("### Iceberg metadata growth")
    w("")
    w("Each run appends the same events to a fresh table in %s bounded Flink jobs through the same Iceberg sink, one "
      "commit each, commit k holding event_sequence values of slice k for every customer. Then: snapshot expiration "
      "(expireSnapshots, retainLast 1), compaction (Actions.rewriteDataFiles, default target file size), and expiration "
      "again. Metadata bytes are the files under the table's metadata/ directory (metadata JSON, manifest lists, "
      "manifests, version hint); Iceberg's defaults keep old metadata JSON files. History check: DuckDB over the current "
      "snapshot's data files computes the row count, distinct (customer_id, event_sequence), and a hash over LAG(email) "
      "and the previous known email (LAST_VALUE IGNORE NULLS, frame ending at 1 PRECEDING) for every row; the hash must "
      "equal the one taken after the commits." % pcfg["iceberg_commits"])
    w("")
    for model in pcfg["models"]:
        rows = [r for r in iceberg if r["model"] == model]
        w("#### %s model" % model.upper())
        w("")
        w(b.md_table(["Design", "State", "Snapshots", "Metadata bytes", "Metadata JSON files", "Manifest lists",
                      "Manifest files", "Manifests in current snapshot", "Data files in current snapshot",
                      "Data files on disk", "Rows", "History unchanged"],
                     [[DESIGN_NAMES[r["schema"]], r["state"].replace("_", " "), r["snapshots_median"],
                       r["metadata_total_bytes_median"], r["metadata_json_files_median"], r["manifest_list_files_median"],
                       r["manifest_files_median"], r["manifests_in_current_snapshot_median"],
                       r["data_files_in_current_snapshot_median"], r["data_files_on_disk_median"], r["rows_median"],
                       r["history_digest_matches_in_all_runs"]] for r in rows]))
        w("")
    w("Files: `pipeline_kafka.csv`, `pipeline_iceberg.csv` (medians), `%s`, `%s` (every run), "
      "`pipeline_environment.json`." % (KAFKA_RUNS, ICEBERG_RUNS))
    w("")
    return "\n".join(lines)


# ------------------------------------------------------------------------ main


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="bench/config.json")
    ap.add_argument("--out", default="results")
    ap.add_argument("--bootstrap", default=None, help="override config pipeline.bootstrap")
    ap.add_argument("--summarize-only", action="store_true", help="recompute medians and the section from the run CSVs")
    args = ap.parse_args()
    cfg = json.load(open(args.config))
    pcfg = cfg["pipeline"]
    bootstrap = args.bootstrap or pcfg["bootstrap"]
    os.makedirs(args.out, exist_ok=True)
    t_start = time.perf_counter()

    def log(msg):
        print("[%7.1fs] %s" % (time.perf_counter() - t_start, msg), flush=True)

    env_path = os.path.join(args.out, "pipeline_environment.json")
    if args.summarize_only:
        summarize(args.out, cfg, pcfg, json.load(open(env_path)))
        log("summary rewritten")
        return
    env = {"started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "config_file": args.config,
           "config": cfg, "bootstrap": bootstrap, "machine": b.machine_info()}
    for f in (KAFKA_RUNS, ICEBERG_RUNS):
        if os.path.exists(os.path.join(args.out, f)):
            os.remove(os.path.join(args.out, f))
    cp = classpath(log)
    work = tempfile.mkdtemp(prefix="audit-pipeline-")
    try:
        for model in pcfg["models"]:
            events = write_events(cfg, model, work, log)
            for design in ("A", "B"):
                common = {"events": events[design], "label": design, "model": model, "work": work,
                          "parallelism": pcfg["parallelism"]}
                for comp in pcfg["compressions"]:
                    run_java(cp, "kafka", dict(common, out=os.path.join(args.out, KAFKA_RUNS), bootstrap=bootstrap,
                                               compression=comp, runs=pcfg["runs"],
                                               **{"replication-factor": pcfg["replication_factor"],
                                                  "partitions": pcfg["partitions"],
                                                  "checkpoint-interval-ms": pcfg["checkpoint_interval_ms"],
                                                  "min-pause-ms": pcfg["min_pause_ms"],
                                                  "rest-port": pcfg["rest_port"], "poll-ms": pcfg["poll_ms"]}), log)
                run_java(cp, "iceberg", dict(common, out=os.path.join(args.out, ICEBERG_RUNS), runs=pcfg["runs"],
                                             commits=pcfg["iceberg_commits"]), log)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    env["wall_seconds"] = round(time.perf_counter() - t_start, 1)
    env["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    with open(env_path, "w") as fh:
        json.dump(env, fh, indent=2)
    summarize(args.out, cfg, pcfg, env)
    log("pipeline CSVs and SUMMARY.md section written; wall time %.1fs" % env["wall_seconds"])


if __name__ == "__main__":
    main()
