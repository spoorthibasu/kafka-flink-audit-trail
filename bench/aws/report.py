#!/usr/bin/env python3
"""Summaries of the AWS scale run, from the raw rows bench/aws/run.py wrote.

  python3 bench/aws/report.py --results results/aws costs --start 2026-10-23 --end 2026-10-27
  python3 bench/aws/report.py --results results/aws summary --summary results/SUMMARY.md

Every value is computed from measured rows; nothing is rounded. Exact decimals are written
out in full; a quotient that does not terminate is written as its first 10 decimals followed
by "...", every digit shown exact (summary_sections.exact_or_prefix). Percentiles are
nearest-rank, as in bench/benchmark.py.

Definitions:
- Producer latency: send() to acknowledgement callback for every event of the run
  (RunnerTools produce), nearest-rank p50, p95, p99, max.
- Replication bytes: the sum over the three brokers of ReplicationBytesInPerSec (and
  ...OutPerSec), each one-minute average multiplied by 60, over the minutes from the start
  of the produce run to the minute describeLogDirs found every replica settled.
- Flink end-to-end throughput: events / (commit time of the first Iceberg snapshot holding
  every event - time the job switched to RUNNING in the JobManager log).
- Flink sustained throughput: records committed between the first and the second-to-last
  data commit / the time between those commits; it leaves out start-up and the last,
  partial checkpoint interval.
- Checkpoint duration: checkpointDuration of every "Completed checkpoint" line of the job,
  up to and including the checkpoint whose commit made every event visible.
- Athena runtime: TotalExecutionTimeInMillis (and EngineExecutionTimeInMillis) of the timed
  runs; cost per query: DataScannedInBytes rounded up to the next MB, 10 MB minimum, at the
  price per TB in infra/prices.json (MB = 2^20 bytes, TB = 2^40 bytes).
"""
import argparse
import csv
import json
import math
import os
import sys
from fractions import Fraction

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, ".."))
from summary_sections import replace_section, exact_or_prefix as ex  # noqa: E402
import athena as ath  # noqa: E402

HEADING = "## AWS scale run"


def rows(results, name):
    path = os.path.join(results, name)
    if not os.path.exists(path):
        return []
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh))


def write(results, name, out_rows):
    if not out_rows:
        return
    with open(os.path.join(results, name), "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(out_rows[0].keys()))
        w.writeheader()
        w.writerows(out_rows)


def rank(values, p):
    s = sorted(values)
    return s[max(1, math.ceil(p / 100.0 * len(s))) - 1]


def ms(ns):
    return ex(Fraction(int(ns), 1_000_000))


def table(headers, body):
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in body]
    return "\n".join(out)


# ------------------------------------------------------------------ kafka

def kafka(results):
    lb = {r["topic"]: r for r in rows(results, "topic_log_bytes.csv")}
    metrics = rows(results, "msk_metrics.csv")
    out = []
    for p in rows(results, "producer_runs.csv"):
        l = lb[p["topic"]]
        rf = int(l["replication_factor"])
        integ = {}
        for m in metrics:
            if m["topic"] == p["topic"]:
                integ[m["metric"]] = integ.get(m["metric"], Fraction(0)) + Fraction(m["average_per_second"]) * 60
        events = int(p["events"])
        out.append({
            "label": p["label"], "design": p["design"], "compression": p["compression"], "events": events,
            "partitions": l["partitions"], "replication_factor": rf, "min_insync_replicas": l["min_insync_replicas"],
            "acks": p["acks"], "kafka_client_version": p["kafka_client_version"],
            "producer_latency_p50_ms": ms(p["producer_latency_p50_ns"]),
            "producer_latency_p95_ms": ms(p["producer_latency_p95_ns"]),
            "producer_latency_p99_ms": ms(p["producer_latency_p99_ns"]),
            "producer_latency_max_ms": ms(p["producer_latency_max_ns"]),
            "produce_wall_s": ex(Fraction(int(p["produce_wall_ns"]), 10 ** 9)),
            "avro_value_bytes_sent": p["value_bytes_sent"],
            "records_in_topic": l["records"],
            "topic_log_bytes_one_copy": l["topic_log_bytes_one_copy"],
            "topic_log_bytes_all_replicas": l["topic_log_bytes_all_replicas"],
            "topic_log_bytes_per_event_one_copy": ex(Fraction(int(l["topic_log_bytes_one_copy"]), events)),
            "topic_log_bytes_per_broker": l["topic_log_bytes_per_broker"],
            "replication_bytes_in_cloudwatch": ex(integ["ReplicationBytesInPerSec"]) if "ReplicationBytesInPerSec" in integ else "",
            "replication_bytes_out_cloudwatch": ex(integ["ReplicationBytesOutPerSec"]) if "ReplicationBytesOutPerSec" in integ else "",
            "client_bytes_in_cloudwatch": ex(integ["BytesInPerSec"]) if "BytesInPerSec" in integ else "",
            "replication_bytes_derived": int(l["topic_log_bytes_one_copy"]) * (rf - 1),
        })
    return out


# ------------------------------------------------------------------ flink

def flink(results):
    snaps = rows(results, "iceberg_snapshots.csv")
    cps = rows(results, "flink_checkpoints.csv")
    states = rows(results, "flink_job_states.csv")
    out = []
    for f in rows(results, "flink_runs.csv"):
        d, events = f["design"], int(f["events"])
        ss = sorted([s for s in snaps if s["design"] == d and s["label"] == f["label"]], key=lambda s: int(s["timestamp_ms"]))
        data = [s for s in ss if s["added_records"] not in ("", "0")]
        full = next(s for s in ss if int(s["total_records"]) == events)
        job = full["flink_job_id"]
        running = min(int(s["log_timestamp_ms"]) for s in states if s["job_id"] == job and s["to_state"] == "RUNNING")
        last_cp = int(full["flink_max_committed_checkpoint_id"])
        durations = [int(c["checkpoint_duration_ms"]) for c in cps
                     if c["job_id"] == job and int(c["checkpoint_id"]) <= last_cp]
        sizes = [int(c["checkpointed_bytes"]) for c in cps if c["job_id"] == job and int(c["checkpoint_id"]) <= last_cp]
        row = {"label": f["label"], "design": d, "events": events, "parallelism": f["parallelism"],
               "kpu": int(f["parallelism"]) // int(f["parallelism_per_kpu"]) + 1,
               "checkpoint_interval_ms": f["checkpoint_interval_ms"], "min_pause_ms": f["min_pause_ms"],
               "job_running_epoch_ms": running, "all_events_committed_epoch_ms": full["timestamp_ms"],
               "end_to_end_throughput_events_per_s": ex(Fraction(events * 1000, int(full["timestamp_ms"]) - running)),
               "data_commits": len(data)}
        if len(data) >= 3:
            a, z = data[0], data[-2]
            row["sustained_throughput_events_per_s"] = ex(Fraction(
                (int(z["total_records"]) - int(a["total_records"])) * 1000, int(z["timestamp_ms"]) - int(a["timestamp_ms"])))
        else:
            row["sustained_throughput_events_per_s"] = ""
        row.update({"checkpoints_counted": len(durations),
                    "checkpoint_duration_p50_ms": rank(durations, 50) if durations else "",
                    "checkpoint_duration_p95_ms": rank(durations, 95) if durations else "",
                    "checkpoint_duration_max_ms": max(durations) if durations else "",
                    "checkpointed_bytes_p50": rank(sizes, 50) if sizes else ""})
        out.append(row)
    return out


# ---------------------------------------------------------------- iceberg

def iceberg(results):
    hist = {(h["table"], h["state"]): h for h in rows(results, "iceberg_history.csv")}
    out = []
    for s in rows(results, "iceberg_stats.csv"):
        h = hist.get((s["table"], s["state"]), {})
        base = hist.get((s["table"], "after_commits"), {})
        same = (h.get("row_count") == base.get("row_count") and h.get("history_checksum") == base.get("history_checksum")
                and h.get("distinct_customer_sequence") == base.get("distinct_customer_sequence")) if h and base else ""
        r = {k: s[k] for k in ("label", "table", "state", "snapshots", "metadata_json_files", "metadata_json_bytes",
                               "manifest_list_files", "manifest_list_bytes", "manifest_files", "manifest_bytes",
                               "other_metadata_files", "other_metadata_bytes", "metadata_total_bytes",
                               "manifests_in_current_snapshot", "data_files_in_current_snapshot", "data_files_on_s3",
                               "data_bytes_on_s3", "rows")}
        r.update({"history_rows": h.get("row_count", ""), "history_distinct_customer_sequence": h.get("distinct_customer_sequence", ""),
                  "history_checksum": h.get("history_checksum", ""),
                  "history_same_as_after_commits": "" if same == "" else ("yes" if same else "no")})
        out.append(r)
    return out


# ----------------------------------------------------------------- athena

def athena_summary(results):
    runs = [r for r in rows(results, "athena_runs.csv") if r["warmup"] == "no"]
    out = []
    keys = []
    for r in runs:
        k = (r["query"], r["design"])
        if k not in keys:
            keys.append(k)
    for k in keys:
        rs = [r for r in runs if (r["query"], r["design"]) == k]
        scanned = sorted({int(r["data_scanned_bytes"]) for r in rs})
        total = [int(r["total_execution_ms"]) for r in rs]
        engine = [int(r["engine_execution_ms"]) for r in rs]
        costs = [ath.cost_usd(int(r["data_scanned_bytes"])) for r in rs]
        out.append({"label": rs[0]["label"], "query": k[0], "design": k[1], "runs": len(rs),
                    "data_scanned_bytes": scanned[0] if len(scanned) == 1 else "%d to %d" % (scanned[0], scanned[-1]),
                    "total_execution_p50_ms": rank(total, 50), "total_execution_p95_ms": rank(total, 95),
                    "engine_execution_p50_ms": rank(engine, 50), "engine_execution_p95_ms": rank(engine, 95),
                    "billed_bytes_per_query": ath.billed_bytes(scanned[-1]),
                    "cost_per_query_usd": ex(costs[0]) if len(set(costs)) == 1 else "%s to %s" % (ex(min(costs)), ex(max(costs))),
                    "engine_version": rs[0]["engine_version"]})
    return out


# ------------------------------------------------------------------ costs

def costs(results, start, end, run_tag):
    from awscli import aws
    out = []
    for flt, args in (("tag run=%s" % run_tag, ["--filter", json.dumps({"Tags": {"Key": "run", "Values": [run_tag]}})]),
                      ("account, no filter", [])):
        r = aws("ce", "get-cost-and-usage", "--time-period", "Start=%s,End=%s" % (start, end), "--granularity", "DAILY",
                "--metrics", "UnblendedCost", "UsageQuantity", "--group-by", "Type=DIMENSION,Key=SERVICE", *args)
        for day in r["ResultsByTime"]:
            for g in day["Groups"]:
                out.append({"filter": flt, "day": day["TimePeriod"]["Start"], "service": g["Keys"][0],
                            "unblended_cost_usd": g["Metrics"]["UnblendedCost"]["Amount"],
                            "usage_quantity": g["Metrics"]["UsageQuantity"]["Amount"],
                            "estimated": day.get("Estimated", "")})
    write(results, "costs_daily.csv", out)
    by = {}
    for o in out:
        k = (o["filter"], o["service"])
        by[k] = by.get(k, Fraction(0)) + Fraction(o["unblended_cost_usd"])
    write(results, "costs_by_service.csv", [{"filter": k[0], "service": k[1], "unblended_cost_usd": ex(v),
                                             "period": "%s to %s (end exclusive)" % (start, end)} for k, v in sorted(by.items())])


# ---------------------------------------------------------------- summary

def summary(results, summary_path):
    env = json.load(open(os.path.join(results, "environment.json")))
    k, f, i, a = kafka(results), flink(results), iceberg(results), athena_summary(results)
    write(results, "kafka.csv", k)
    write(results, "flink.csv", f)
    write(results, "iceberg.csv", i)
    write(results, "athena.csv", a)
    eq = rows(results, "equivalence.csv")
    cost = rows(results, "costs_by_service.csv")
    cfg = env["config"]
    lines = []
    w = lines.append
    w(HEADING)
    w("")
    w("Measured by `bench/aws/run.py` from `%s` on AWS (%s), with the infrastructure in `infra/`. Synthetic "
      "data from the bench-full generator, seed %d, K=%d, full model, N=%s entities x H=%d events = %s events per "
      "design. Runner %s; Amazon MSK %s, 3 x %s brokers, replication factor %s; Amazon Managed Service for Apache "
      "Flink %s at parallelism %s; Iceberg on S3 with the Glue Data Catalog; %s. Kafka 3.7.x, which the local "
      "pipeline benchmark used, is past MSK end of support, so the brokers run %s." % (
          env["config_file"], env["region"], cfg["seed"], cfg["k_attributes"], "{:,}".format(cfg["n_entities"]),
          cfg["h_events"], "{:,}".format(cfg["n_entities"] * cfg["h_events"]), env["runner_instance_type"],
          env["kafka_version"], env["msk_broker_type"], cfg["aws"]["replication_factor"], env["flink_runtime"],
          cfg["aws"]["flink_parallelism"], env["athena_engine"], env["kafka_version"]))
    w("")
    w("Definitions are in `bench/aws/report.py`. Raw rows: `producer_runs.csv`, `topic_log_bytes.csv`, "
      "`msk_metrics.csv`, `flink_runs.csv`, `flink_checkpoints.csv`, `flink_job_states.csv`, "
      "`iceberg_snapshots.csv`, `iceberg_stats.csv`, `iceberg_history.csv`, `athena_runs.csv`, `equivalence.csv`, "
      "`costs_daily.csv`.")
    w("")
    w("### Equivalence before timing")
    w("")
    w(table(["Dataset", "Query", "Designs", "Rows left", "Rows right", "Differing rows", "Equal"],
            [[r["label"], r["query"], r["designs"], r["rows_left"], r["rows_right"], r["differing_rows"], r["equal"]] for r in eq]))
    w("")
    w("### Producer and Kafka")
    w("")
    w(table(["Design", "Compression", "Events", "p50 ms", "p95 ms", "p99 ms", "Log bytes, one copy", "Bytes per event",
             "Log bytes, all replicas", "Replication bytes in (CloudWatch)", "Replication bytes out (CloudWatch)"],
            [[r["design"], r["compression"], r["events"], r["producer_latency_p50_ms"], r["producer_latency_p95_ms"],
              r["producer_latency_p99_ms"], r["topic_log_bytes_one_copy"], r["topic_log_bytes_per_event_one_copy"],
              r["topic_log_bytes_all_replicas"], r["replication_bytes_in_cloudwatch"], r["replication_bytes_out_cloudwatch"]]
             for r in k]))
    w("")
    w("### Flink")
    w("")
    w(table(["Design", "Events", "KPU", "End-to-end events/s", "Sustained events/s", "Data commits", "Checkpoints",
             "Checkpoint p50 ms", "Checkpoint p95 ms"],
            [[r["design"], r["events"], r["kpu"], r["end_to_end_throughput_events_per_s"],
              r["sustained_throughput_events_per_s"], r["data_commits"], r["checkpoints_counted"],
              r["checkpoint_duration_p50_ms"], r["checkpoint_duration_p95_ms"]] for r in f]))
    w("")
    w("### Iceberg on S3")
    w("")
    w(table(["Table", "State", "Snapshots", "Metadata bytes", "Manifests", "Data files in snapshot", "Data files on S3",
             "Data bytes on S3", "Rows", "History unchanged"],
            [[r["table"], r["state"].replace("_", " "), r["snapshots"], r["metadata_total_bytes"], r["manifest_files"],
              r["data_files_in_current_snapshot"], r["data_files_on_s3"], r["data_bytes_on_s3"], r["rows"],
              r["history_same_as_after_commits"]] for r in i]))
    w("")
    w("### Athena engine version 3")
    w("")
    w(table(["Query", "Design", "Runs", "Bytes scanned", "p50 ms", "p95 ms", "Engine p50 ms", "Engine p95 ms",
             "Cost per query USD"],
            [[r["query"], r["design"], r["runs"], r["data_scanned_bytes"], r["total_execution_p50_ms"],
              r["total_execution_p95_ms"], r["engine_execution_p50_ms"], r["engine_execution_p95_ms"],
              r["cost_per_query_usd"]] for r in a]))
    w("")
    if cost:
        w("### Cost Explorer, unblended cost by service")
        w("")
        w(table(["Filter", "Service", "USD", "Period"], [[c["filter"], c["service"], c["unblended_cost_usd"], c["period"]]
                                                       for c in cost]))
        w("")
    replace_section(summary_path, HEADING, "\n".join(lines))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default="results/aws")
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("costs")
    c.add_argument("--start", required=True)
    c.add_argument("--end", required=True)
    c.add_argument("--run-tag", default="aws-scale-1")
    s = sub.add_parser("summary")
    s.add_argument("--summary", default="results/SUMMARY.md")
    a = ap.parse_args()
    if a.cmd == "costs":
        costs(a.results, a.start, a.end, a.run_tag)
    else:
        summary(a.results, a.summary)


if __name__ == "__main__":
    main()
