#!/usr/bin/env python3
"""Decision boundary between the three designs, computed from committed results only.

Nothing is measured here. The script reads the AWS run's committed results (Athena bytes
scanned and cost per query, Iceberg data bytes, Managed Flink throughput and KPUs, Kafka
log bytes), the price list in infra/prices.json and the DuckDB latency in latency.csv, and
computes, with exact fractions:

  C against B   how many population-wide current-state reads per month make a
                current-state table cheaper than answering them from the audit trail
  A against B   under what read mix the previous-value schema is cheaper than LAG over
                the audit trail, and on which queries it is faster

Every assumption is written into the output. Writes decision_boundary.md into --out.

  python3 bench/decision_boundary.py --out results
"""
import argparse
import csv
import json
import os
import sys
from fractions import Fraction as F

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "bench"))
from summary_sections import exact_or_prefix  # noqa: E402

GIB = 2 ** 30   # storage GB as AWS bills it; results/prices.json cites the S3 FAQ for 2^30
SECONDS_PER_HOUR = 3600


def rows(path):
    return list(csv.DictReader(open(os.path.join(ROOT, path))))


def d(x, places=6):
    """Decimal text: exact when it terminates within `places`, else truncated with '...'."""
    s = exact_or_prefix(F(x), places)
    if "..." not in s and "." in s and len(s.split(".")[1]) > places:
        whole, frac = s.split(".")
        return whole + "." + frac[:places] + "..."
    return s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="results")
    a = ap.parse_args()

    prices_doc = json.load(open(os.path.join(ROOT, "infra", "prices.json")))
    price = {k: F(v["usd"]) for k, v in prices_doc["prices"].items()}
    athena = {(r["query"], r["design"]): r for r in rows("results/aws/athena.csv")}
    ice = {(r["table"], r["state"]): r for r in rows("results/aws/iceberg.csv")}
    flink = {r["design"]: r for r in rows("results/aws/flink.csv")}
    kafka = {(r["design"], r["compression"]): r for r in rows("results/aws/kafka.csv")}
    duck = {(r["query"], r["design"]): r["p50_ms"] for r in rows("results/latency.csv")
            if r["model"] == "full" and r["n_entities"] == "1000" and r["h_events"] == "100" and r["supported"] == "yes"}

    cost = lambda q, dsg: F(athena[(q, dsg)]["cost_per_query_usd"])  # noqa: E731
    p50 = lambda q, dsg: int(athena[(q, dsg)]["total_execution_p50_ms"])  # noqa: E731
    stored = {t: int(ice[(t, "after_commits")]["current_snapshot_files_size"]) for t in ("a", "b", "c")}
    events = int(flink["A"]["events"])
    run_s = {dsg: F(int(flink[dsg]["all_events_committed_epoch_ms"]) - int(flink[dsg]["job_running_epoch_ms"]), 1000)
             for dsg in ("A", "B")}
    kpu = {dsg: int(flink[dsg]["kpu"]) for dsg in ("A", "B")}
    log_all = {dsg: int(kafka[(dsg, "zstd")]["topic_log_bytes_one_copy"]) * int(kafka[(dsg, "zstd")]["replication_factor"])
               for dsg in ("A", "B")}

    lines = []
    w = lines.append
    w("# Decision boundary between the three designs")
    w("")
    w("Computed by `bench/decision_boundary.py` from committed results only; nothing here is a new measurement. The "
      "workload is the AWS run's: synthetic full-state events, 1,000,000 entities x 100 events = %s events per "
      "design, Iceberg on S3, queried with Athena engine version 3 (`results/aws/`)." % "{:,}".format(events))
    w("")
    w("## Assumptions")
    w("")
    w("- Prices: `infra/prices.json` (AWS Price List, fetched %s, us-east-1, on-demand): S3 Standard %s USD per "
      "GB-month, Athena %s USD per TB scanned, Managed Service for Apache Flink %s USD per KPU-hour, MSK storage %s "
      "USD per GB-month." % (prices_doc["fetched_utc"][:10], d(price["s3_standard_gb_month"]),
                            d(price["athena_tb_scanned"]), d(price["msf_kpu_hour"]), d(price["msk_storage_gb_month"])))
    w("- Units: storage GB = 2^30 bytes (as `results/prices.json` cites from the S3 FAQ); Athena's per-query cost is "
      "the committed `cost_per_query_usd`, which already applies Athena's rounding to the MB and 10 MB minimum.")
    w("- Scale: the AWS run's 100,000,000 events per design. Costs at other scales are assumed to grow with the "
      "bytes; that is not measured.")
    w("- Storage: each table's current-snapshot data bytes after the Flink commits (`results/aws/iceberg.csv`, "
      "state after_commits). Files left on S3 by compaction until orphan removal are not counted.")
    w("- Reads: each read is billed and timed as one committed Athena run of that query; no result reuse or caching.")
    w("- Current-state table maintenance is not measured in the run, so two cases are given: (i) the ingestion "
      "pipeline keeps C current at no counted extra cost; (ii) C is rebuilt by one population-wide pass over B per "
      "refresh, billed like one q3 read on B.")
    w("- Ingestion compute: Managed Flink KPUs times the time from RUNNING to all events committed "
      "(`results/aws/flink.csv`), that is, an application sized so that its KPUs track the event rate. MSK "
      "broker-hours are the same cluster for both designs and are not counted.")
    w("- Kafka storage: all replicas of the zstd topic's log bytes (`results/aws/kafka.csv`), per month of "
      "retention. The run records no retention period, so it is a parameter. MSK bills provisioned broker "
      "storage, so this difference is a cost only when provisioned storage is sized to the retained log.")
    w("")

    # ---------------------------------------------------------------- C against B
    save = cost("q3_current_state_all", "B") - cost("q3_current_state_all", "C")
    c_month = F(stored["c"], GIB) * price["s3_standard_gb_month"]
    refresh = cost("q3_current_state_all", "B")
    w("## C against B: population-wide current-state reads (q3)")
    w("")
    w(md([["Athena cost per read, USD", d(cost("q3_current_state_all", "B")), d(cost("q3_current_state_all", "C"))],
          ["Athena bytes scanned per read", fmt(athena[("q3_current_state_all", "B")]["data_scanned_bytes"]),
           fmt(athena[("q3_current_state_all", "C")]["data_scanned_bytes"])],
          ["Athena p50 ms", fmt(p50("q3_current_state_all", "B")), fmt(p50("q3_current_state_all", "C"))],
          ["DuckDB p50 ms, main benchmark", duck[("q3_current_state_all", "B")], duck[("q3_current_state_all", "C")]],
          ["Table data bytes on S3", fmt(stored["b"]), fmt(stored["c"])]],
         ["", "B, newest row per entity from the audit trail (ROW_NUMBER)", "C, current-state table"]))
    w("")
    w("- Saving per population-wide read with C: %s USD. C's storage: %s USD per month." % (d(save), d(c_month)))
    w("- Case (i), C kept current by the pipeline: C pays for its storage at %s reads per month, so the first "
      "population-wide read each month already does." % d(c_month / save, 4))
    w("- Case (ii), C rebuilt r times per month at %s USD per rebuild: break-even reads per month = (r x %s + %s) / %s "
      "= %s x r + %s. Daily rebuilds (r = 30): %s reads per month; hourly (r = 720): %s. C is the cheaper choice once "
      "population-wide current-state reads outnumber its rebuilds." % (
          d(refresh), d(refresh), d(c_month), d(save), d(refresh / save, 4), d(c_month / save, 4),
          d((30 * refresh + c_month) / save, 2), d((720 * refresh + c_month) / save, 2)))
    w("- C answers current state only: q1, q2 and q4 need A or B.")
    w("")

    # ---------------------------------------------------------------- A against B
    diff = {q: cost(q, "A") - cost(q, "B") for q in ("q1_one_hop_previous_single", "q3_current_state_all", "q4_previous_value_all")}
    storage_extra = F(stored["a"] - stored["b"], GIB) * price["s3_standard_gb_month"]
    kafka_extra = F(log_all["A"] - log_all["B"], GIB) * price["msk_storage_gb_month"]
    kpu_h = {dsg: kpu[dsg] * run_s[dsg] / SECONDS_PER_HOUR for dsg in ("A", "B")}
    ingest_extra = (kpu_h["A"] - kpu_h["B"]) * price["msf_kpu_hour"]
    q1_gain = -diff["q1_one_hop_previous_single"]
    w("## A against B: embedded previous values against LAG")
    w("")
    w(md([[q, d(cost(q, "A")), d(cost(q, "B")), d(diff[q]), fmt(p50(q, "A")), fmt(p50(q, "B")), duck[(q, "A")], duck[(q, "B")]]
          for q in ("q1_one_hop_previous_single", "q3_current_state_all", "q4_previous_value_all")],
         ["Query", "Athena USD per read, A", "B", "A minus B", "Athena p50 ms, A", "B", "DuckDB p50 ms, A", "B"]))
    w("")
    w("q2 (three changes back) has no A row: A holds one hop.")
    w("")
    w(md([["Iceberg data bytes on S3", fmt(stored["a"]), fmt(stored["b"]), "%s USD per month" % d(storage_extra)],
          ["Kafka log bytes, zstd, all replicas", fmt(log_all["A"]), fmt(log_all["B"]),
           "%s USD per month of retention" % d(kafka_extra)],
          ["Managed Flink KPU-hours for %s events" % fmt(events), d(kpu_h["A"], 4), d(kpu_h["B"], 4),
           "%s USD per %s events ingested" % (d(ingest_extra), fmt(events))]],
         ["Fixed cost", "A", "B", "A's extra cost"]))
    w("")
    w("- Cost boundary: A is cheaper than B for a month when %s x n_q1 - %s x n_q3 - %s x n_q4 > F, where n_q are "
      "that month's reads and F is A's extra fixed cost: %s USD of storage, plus %s USD per month of Kafka retention, "
      "plus %s USD per %s events ingested that month." % (
          d(q1_gain), d(diff["q3_current_state_all"]), d(diff["q4_previous_value_all"]), d(storage_extra),
          d(kafka_extra), d(ingest_extra), fmt(events)))
    w("- With single-entity previous-value reads only (n_q3 = n_q4 = 0): storage alone needs %s q1 reads per month; "
      "with a month of Kafka retention as well, %s; and with one month's ingestion of %s new events added, %s." % (
          d(storage_extra / q1_gain, 1), d((storage_extra + kafka_extra) / q1_gain, 1), fmt(events),
          d((storage_extra + kafka_extra + ingest_extra) / q1_gain, 1)))
    w("- Each population-wide previous-value read (q4) costs more on A than on B, so each one raises the bar by %s "
      "q1 reads; each q3 read by %s." % (d(diff["q4_previous_value_all"] / q1_gain, 2), d(diff["q3_current_state_all"] / q1_gain, 2)))
    w("- Latency boundary: A is faster than B on the population-wide previous-value read q4 (Athena p50 B/A %s, "
      "DuckDB %s) and slower on the single-entity read q1 (Athena B/A %s, DuckDB %s). On q3 the two are within %s on "
      "Athena." % (d(F(p50("q4_previous_value_all", "B"), p50("q4_previous_value_all", "A")), 2),
                   d(F(duck[("q4_previous_value_all", "B")]) / F(duck[("q4_previous_value_all", "A")]), 2),
                   d(F(p50("q1_one_hop_previous_single", "B"), p50("q1_one_hop_previous_single", "A")), 2),
                   d(F(duck[("q1_one_hop_previous_single", "B")]) / F(duck[("q1_one_hop_previous_single", "A")]), 2),
                   "%s%%" % d(abs(F(p50("q3_current_state_all", "A"), p50("q3_current_state_all", "B")) - 1) * 100, 1)))
    w("")
    w("Sources: `results/aws/athena.csv`, `results/aws/iceberg.csv`, `results/aws/flink.csv`, `results/aws/kafka.csv`, "
      "`results/latency.csv`, `infra/prices.json`.")
    w("")
    with open(os.path.join(a.out, "decision_boundary.md"), "w") as fh:
        fh.write("\n".join(lines))
    print("\n".join(lines))


def fmt(v):
    return "{:,}".format(int(v))


def md(rows_, headers):
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows_]
    return "\n".join(out)


if __name__ == "__main__":
    main()
