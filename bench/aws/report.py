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
  runs. Every SELECT writes its result rows to the workgroup's S3 result location, as any
  Athena query does, and the runtime includes that write; cost per query: DataScannedInBytes rounded up to the next MB, 10 MB minimum, at the
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
        integ, per_broker = {}, {}
        for m in metrics:
            if m["topic"] == p["topic"]:
                v = Fraction(m["average_per_second"]) * 60
                integ[m["metric"]] = integ.get(m["metric"], Fraction(0)) + v
                key = (m["metric"], m["broker_id"])
                per_broker[key] = per_broker.get(key, Fraction(0)) + v
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
            "replication_bytes_in_per_broker_cloudwatch": ";".join(
                "%s=%s" % (b, ex(v)) for (mm, b), v in sorted(per_broker.items()) if mm == "ReplicationBytesInPerSec"),
            "replication_bytes_out_per_broker_cloudwatch": ";".join(
                "%s=%s" % (b, ex(v)) for (mm, b), v in sorted(per_broker.items()) if mm == "ReplicationBytesOutPerSec"),
            "cloudwatch_window_minutes": (int(next(m["window_end_epoch_ms"] for m in metrics if m["topic"] == p["topic"]))
                                          - int(next(m["window_start_epoch_ms"] for m in metrics if m["topic"] == p["topic"])))
                                         // 60000 if any(m["topic"] == p["topic"] for m in metrics) else "",
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
                               "data_bytes_on_s3", "current_snapshot_files_size", "rows")}
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
                    "total_execution_p99_ms": rank(total, 99),
                    "engine_execution_p50_ms": rank(engine, 50), "engine_execution_p95_ms": rank(engine, 95),
                    "engine_execution_p99_ms": rank(engine, 99),
                    "billed_bytes_per_query": ath.billed_bytes(scanned[-1]),
                    "cost_per_query_usd": ex(costs[0]) if len(set(costs)) == 1 else "%s to %s" % (ex(min(costs)), ex(max(costs))),
                    "engine_version": rs[0]["engine_version"]})
    return out


# ------------------------------------------------------------------ costs

def costs(results, start, end, run_tag):
    """Cost Explorer, daily, by service and record type, for the run tag and for the whole account.
    Usage is the gross charge; Credit rows are credits the account applied against it."""
    from awscli import aws
    period = "Start=%s,End=%s" % (start, end)
    out = []
    for flt, args in (("tag run=%s" % run_tag, ["--filter", json.dumps({"Tags": {"Key": "run", "Values": [run_tag]}})]),
                      ("account, no filter", [])):
        r = aws("ce", "get-cost-and-usage", "--time-period", period, "--granularity", "DAILY",
                "--metrics", "UnblendedCost", "UsageQuantity",
                "--group-by", "Type=DIMENSION,Key=SERVICE", "Type=DIMENSION,Key=RECORD_TYPE", *args)
        for day in r["ResultsByTime"]:
            for g in day["Groups"]:
                out.append({"filter": flt, "day": day["TimePeriod"]["Start"], "service": g["Keys"][0],
                            "record_type": g["Keys"][1], "unblended_cost_usd": g["Metrics"]["UnblendedCost"]["Amount"],
                            "usage_quantity": g["Metrics"]["UsageQuantity"]["Amount"],
                            "estimated": day.get("Estimated", "")})
    write(results, "costs_daily.csv", out)
    r = aws("ce", "get-cost-and-usage", "--time-period", period, "--granularity", "DAILY",
            "--metrics", "UnblendedCost", "UsageQuantity",
            "--filter", json.dumps({"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Usage"]}}),
            "--group-by", "Type=DIMENSION,Key=SERVICE", "Type=DIMENSION,Key=USAGE_TYPE")
    write(results, "costs_usage_types.csv", [
        {"day": day["TimePeriod"]["Start"], "service": g["Keys"][0], "usage_type": g["Keys"][1],
         "unblended_cost_usd": g["Metrics"]["UnblendedCost"]["Amount"],
         "usage_quantity": g["Metrics"]["UsageQuantity"]["Amount"], "unit": g["Metrics"]["UsageQuantity"]["Unit"],
         "estimated": day.get("Estimated", "")}
        for day in r["ResultsByTime"] for g in day["Groups"]])
    by = {}
    for o in out:
        k = (o["filter"], o["service"])
        d = by.setdefault(k, {"Usage": Fraction(0), "Credit": Fraction(0), "other": Fraction(0)})
        bucket = o["record_type"] if o["record_type"] in ("Usage", "Credit") else "other"
        d[bucket] += Fraction(o["unblended_cost_usd"])
    rows_out = []
    for flt in sorted({k[0] for k in by}):
        total = {"Usage": Fraction(0), "Credit": Fraction(0), "other": Fraction(0)}
        for (f, service), d in sorted(by.items()):
            if f != flt:
                continue
            for b in total:
                total[b] += d[b]
            rows_out.append({"filter": f, "service": service, "usage_usd": ex(d["Usage"]), "credit_usd": ex(d["Credit"]),
                             "other_record_types_usd": ex(d["other"]), "net_usd": ex(sum(d.values())),
                             "period": "%s to %s (end exclusive)" % (start, end)})
        rows_out.append({"filter": flt, "service": "Total", "usage_usd": ex(total["Usage"]), "credit_usd": ex(total["Credit"]),
                         "other_record_types_usd": ex(total["other"]), "net_usd": ex(sum(total.values())),
                         "period": "%s to %s (end exclusive)" % (start, end)})
    write(results, "costs_by_service.csv", rows_out)


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
    w("Managed Flink rejected parallelism above %s for this account (CreateApplication: the account needs AWS "
      "verification for higher KPU limits), so both applications run at %s with one KPU per slot plus the "
      "orchestration KPU." % (cfg["aws"]["flink_parallelism"], cfg["aws"]["flink_parallelism"]))
    w("")
    w("Definitions are in `bench/aws/report.py`. Raw rows: `producer_runs.csv`, `topic_log_bytes.csv`, "
      "`msk_metrics.csv`, `flink_runs.csv`, `flink_checkpoints.csv`, `flink_job_states.csv`, "
      "`iceberg_snapshots.csv`, `iceberg_stats.csv`, `iceberg_history.csv`, `athena_runs.csv`, `equivalence.csv`, "
      "`costs_daily.csv`, `costs_usage_types.csv`.")
    w("")
    w("### Equivalence before timing")
    w("")
    w(table(["Dataset", "Query", "Designs", "Rows left", "Rows right", "Differing rows", "Equal"],
            [[r["label"], r["query"], r["designs"], r["rows_left"], r["rows_right"], r["differing_rows"], r["equal"]] for r in eq]))
    w("")
    w("### Producer and Kafka")
    w("")
    w("Producer latency is send() to acknowledgement for every event of the run, sent back to back with no pacing "
      "(acks=all, Kafka client %s defaults otherwise), so it includes each record's wait in the producer's buffer. "
      "Log bytes come from describeLogDirs once every replica matched its leader. Replication bytes from CloudWatch "
      "sum ReplicationBytesInPerSec (and ...OutPerSec) over the three brokers, one-minute averages times 60, from the "
      "minute the run started to the minute after the log bytes settled; the derived column is one copy times "
      "(replication factor - 1)." % (k[0]["kafka_client_version"] if k else ""))
    w("")
    w(table(["Design", "Compression", "Events", "p50 ms", "p95 ms", "p99 ms", "Max ms", "Log bytes, one copy",
             "Bytes per event", "Log bytes, all replicas", "Replication bytes in (CloudWatch)",
             "Replication bytes out (CloudWatch)", "Replication bytes (derived)"],
            [[r["design"], r["compression"], r["events"], r["producer_latency_p50_ms"], r["producer_latency_p95_ms"],
              r["producer_latency_p99_ms"], r["producer_latency_max_ms"], r["topic_log_bytes_one_copy"],
              r["topic_log_bytes_per_event_one_copy"], r["topic_log_bytes_all_replicas"],
              r["replication_bytes_in_cloudwatch"], r["replication_bytes_out_cloudwatch"], r["replication_bytes_derived"]]
             for r in k]))
    w("")
    w("### Flink")
    w("")
    w("Each application read its zstd topic from the earliest offset after the topic held every event. End-to-end "
      "throughput is events over the time from the job's switch to RUNNING (JobManager log) to the commit of the first "
      "Iceberg snapshot holding every event; sustained throughput is the records committed between the first and the "
      "second-to-last data commit over the time between them. Checkpoint durations are the checkpointDuration of every "
      "\"Completed checkpoint\" line of the job up to the checkpoint that made every event visible, nearest-rank.")
    w("")
    w(table(["Design", "Events", "KPU", "End-to-end events/s", "Sustained events/s", "Data commits", "Checkpoints",
             "Checkpoint p50 ms", "Checkpoint p95 ms", "Checkpoint max ms"],
            [[r["design"], r["events"], r["kpu"], r["end_to_end_throughput_events_per_s"],
              r["sustained_throughput_events_per_s"], r["data_commits"], r["checkpoints_counted"],
              r["checkpoint_duration_p50_ms"], r["checkpoint_duration_p95_ms"], r["checkpoint_duration_max_ms"]] for r in f]))
    w("")
    w("### Iceberg on S3")
    w("")
    w("Expiration is expireSnapshots with retainLast 1; compaction is the Flink rewriteDataFiles action with Iceberg's "
      "defaults, both run from the runner against the Glue catalog. Bytes and file counts are listed from the table's "
      "metadata/ and data/ prefixes on S3. History unchanged compares, in Athena, the row count, the distinct "
      "(customer_id, event_sequence) count and a checksum over LAG(email) and the previous known email at every row "
      "with the same three values after the Flink commits.")
    w("")
    w(table(["Table", "State", "Snapshots", "Metadata bytes on S3", "Manifest files on S3", "Manifests in snapshot",
             "Data files in snapshot", "Data files on S3", "Data bytes on S3", "Rows", "History unchanged"],
            [[r["table"], r["state"].replace("_", " "), r["snapshots"], r["metadata_total_bytes"], r["manifest_files"],
              r["manifests_in_current_snapshot"], r["data_files_in_current_snapshot"], r["data_files_on_s3"],
              r["data_bytes_on_s3"], r["rows"], r["history_same_as_after_commits"]] for r in i]))
    final = {r["table"]: r for r in i if r["state"] == "after_compaction_and_expiration"}
    if final:
        w("")
        w("After compaction and expiration, %s." % "; ".join(
            "%s keeps %d data files on S3 that its snapshot no longer references" % (
                t, int(r["data_files_on_s3"]) - int(r["data_files_in_current_snapshot"])) for t, r in sorted(final.items())))
        comp = {r["table"]: r for r in i if r["state"] == "after_compaction"}
        w("The files left over are the pre-compaction data files, by count and by bytes: %s. The second expiration "
          "removed %s, and no data files. It ran with expireOlderThan(now + 1 ms) and retainLast(1), which keeps "
          "only the newest snapshot, the compaction snapshot. Iceberg 1.10.1's incremental cleanup "
          "(core IncrementalFileCleanup, used when no snapshot ID or branch is named) deletes a data file only when "
          "the snapshot recorded on its DELETED manifest entry has expired; here that is the compaction snapshot, "
          "which was kept. Orphan-file removal was not run." % (
              "; ".join("%s %d files and %d bytes on S3 outside the snapshot, against %s files and %s bytes after the "
                        "Flink commits" % (t, int(r["data_files_on_s3"]) - int(r["data_files_in_current_snapshot"]),
                                           int(r["data_bytes_on_s3"]) - int(r["current_snapshot_files_size"]),
                                           base_r["data_files_on_s3"], base_r["data_bytes_on_s3"])
                        for t, r in sorted(final.items())
                        for base_r in [next(x for x in i if x["table"] == t and x["state"] == "after_commits")]),
              ", ".join("%s of the manifest files of %s (%s to %s)" % (int(comp[t]["manifest_files"]) - int(r["manifest_files"]), t,
                                                                 comp[t]["manifest_files"], r["manifest_files"])
                        for t, r in sorted(final.items()) if t in comp)))
    w("")
    w("### Athena engine version 3")
    w("")
    w("Bytes scanned is Athena's DataScannedInBytes, the same on every run of a query. p50 and p95 are nearest-rank "
      "over the timed runs of TotalExecutionTimeInMillis; the engine columns are EngineExecutionTimeInMillis. Each "
      "SELECT writes its result rows to the workgroup's S3 result location, and the runtime includes that write. "
      "Cost per query is the bytes scanned, rounded up to the next MB with a 10 MB minimum, at the price in "
      "`infra/prices.json`.")
    w("")
    w(table(["Query", "Design", "Runs", "Bytes scanned", "p50 ms", "p95 ms", "p99 ms", "Engine p50 ms",
             "Engine p95 ms", "Engine p99 ms", "Cost per query USD"],
            [[r["query"], r["design"], r["runs"], r["data_scanned_bytes"], r["total_execution_p50_ms"],
              r["total_execution_p95_ms"], r["total_execution_p99_ms"], r["engine_execution_p50_ms"],
              r["engine_execution_p95_ms"], r["engine_execution_p99_ms"], r["cost_per_query_usd"]] for r in a]))
    w("")
    w("With 10 timed runs, the nearest-rank p95 and p99 are both the slowest run. Q1 and Q4 have no C row and Q2 has "
      "no A or C row: C holds no history and A holds one hop, so they cannot answer those questions from their own data.")
    w("")
    if cost:
        w("### Cost Explorer, unblended cost by service")
        w("")
        w("Cost Explorer UnblendedCost over %s. Usage is the gross charge for what the run used; the account applied "
          "credits against it, shown separately. Cost Explorer still marked these days as estimated when read." %
          cost[0]["period"])
        w("")
        w(table(["Filter", "Service", "Usage USD", "Credits USD", "Net USD"],
                [[c["filter"], c["service"], c["usage_usd"], c["credit_usd"], c["net_usd"]] for c in cost]))
        dt_credit = next((c["credit_usd"] for c in cost if c["service"] == "AWS Data Transfer"), None)
        regional = [u for u in rows(results, "costs_usage_types.csv") if u["usage_type"] == "DataTransfer-Regional-Bytes"]
        if dt_credit and regional:
            w("")
            w("Cost Explorer books the credit for regional (cross-AZ) data transfer under AWS Data Transfer (%s), while "
              "the usage it offsets sits under %s; those usage lines sum to %s." % (
                  dt_credit, " and ".join("%s (%s for %s GB)" % (u["service"], u["unblended_cost_usd"], u["usage_quantity"])
                                          for u in regional),
                  ex(sum(Fraction(u["unblended_cost_usd"]) for u in regional))))
        if not any(c["filter"].startswith("tag") for c in cost):
            w("")
            w("The same query filtered on the run tag returned no rows: the tag was not active as a cost allocation "
              "tag when Cost Explorer was read. In us-east-1 every usage line of these days is the run's "
              "(`costs_usage_types.csv`); the only lines from another region are Glue catalog requests and one S3 "
              "request in us-east-2.")
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
