#!/usr/bin/env python3
"""Cost estimate for the AWS scale run, before anything is created.

Prices come from infra/prices.json (AWS Price List Query API). Data volumes per event come
from the local pipeline measurements in results/pipeline_kafka.csv and
results/pipeline_iceberg.csv, scaled to 100,000,000 events per design. Hours are planning
assumptions, listed with their basis. Everything here is an estimate; the run reports what
Cost Explorer bills.

  python3 infra/estimate.py --out infra/COST_ESTIMATE.md
"""
import argparse
import csv
import json
import math
import os
from fractions import Fraction as F

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EVENTS = 100_000_000
SUBSET = 10_000_000
HOURS_PER_MONTH = 720  # GB-month and metric-month prorating; 720 is the shortest month that matters here, so the estimate errs high
GB = 10 ** 9  # decimal GB; AWS bills binary GB, so this also errs high


def load_prices():
    with open(os.path.join(ROOT, "infra", "prices.json")) as fh:
        doc = json.load(fh)
    return {k: F(v["usd"]) for k, v in doc["prices"].items()}, doc


def local_bytes():
    """Bytes per event (Kafka, one copy) and per row (Iceberg data files after the commits), full model."""
    kafka = {(r["schema"], r["compression"]): F(r["topic_log_bytes_per_event_one_copy"])
             for r in csv.DictReader(open(os.path.join(ROOT, "results", "pipeline_kafka.csv"))) if r["model"] == "full"}
    ice = {r["schema"]: F(r["data_bytes_on_disk_median"]) / F(r["rows_median"])
           for r in csv.DictReader(open(os.path.join(ROOT, "results", "pipeline_iceberg.csv")))
           if r["model"] == "full" and r["state"] == "after_commits"}
    for r in csv.DictReader(open(os.path.join(ROOT, "results", "storage.csv"))):
        if r["model"] == "full" and r["design"] == "C" and r["parquet_compression"] == "zstd":
            ice["C"] = F(r["parquet_bytes"]) / F(r["rows"])
    thr = {r["schema"]: r["flink_throughput_events_per_s_median"]
           for r in csv.DictReader(open(os.path.join(ROOT, "results", "pipeline_kafka.csv")))
           if r["model"] == "full" and r["compression"] == "zstd"}
    return kafka, ice, thr


def plan(none_full):
    """Planning quantities. Each value has a basis in the returned notes."""
    p = {
        "runner_hours": 36 + (2 if none_full else 0),
        "msk_hours": 16 + (2 if none_full else 0),
        "msk_brokers": 3,
        "msk_storage_gb_per_broker": 400,
        "msf_kpu": 12 + 1,
        "msf_storage_gb": 12 * 50,
        "msf_app_hours": 6,
        "vpc_endpoints": 2,
        "root_volume_gb": 50,
        "msk_per_broker_metrics": 60,
        "msf_metrics": 60,
        "logs_gb": 3,
        "get_metric_data_metrics": 50_000,
        "glue_requests": 20_000,
        "cost_explorer_requests": 30,
        "s3_tier1_requests": 200_000,
        "s3_tier2_requests": 5_000_000,
        "athena_results_gb": 50,
        "s3_days": 3,
        "none_events": EVENTS if none_full else SUBSET,
    }
    return p


def estimate(prices, kafka, ice, p):
    rows = []

    def add(service, item, qty, unit, price_key, cost):
        rows.append({"service": service, "item": item, "qty": qty, "unit": unit, "price_key": price_key, "cost": cost})

    h = p["runner_hours"]
    add("Amazon EC2", "runner m8id.2xlarge (generator, producer, Iceberg maintenance)", h, "instance-hours",
        "ec2_m8id_2xlarge_hour", h * prices["ec2_m8id_2xlarge_hour"])
    gbmo = F(p["root_volume_gb"] * h, HOURS_PER_MONTH)
    add("Amazon EC2", "runner root volume, gp3 %d GB" % p["root_volume_gb"], gbmo, "GB-months", "ebs_gp3_gb_month",
        gbmo * prices["ebs_gp3_gb_month"])
    add("Amazon VPC", "runner public IPv4 address", h, "hours", "public_ipv4_hour", h * prices["public_ipv4_hour"])

    mh = p["msk_hours"] * p["msk_brokers"]
    add("Amazon MSK", "3 x kafka.m7g.large brokers", mh, "broker-hours", "msk_m7g_large_broker_hour",
        mh * prices["msk_m7g_large_broker_hour"])
    gbmo = F(p["msk_brokers"] * p["msk_storage_gb_per_broker"] * p["msk_hours"], HOURS_PER_MONTH)
    add("Amazon MSK", "broker storage, %d GB per broker" % p["msk_storage_gb_per_broker"], gbmo, "GB-months",
        "msk_storage_gb_month", gbmo * prices["msk_storage_gb_month"])

    kh = p["msf_kpu"] * p["msf_app_hours"]
    add("Managed Service for Apache Flink", "parallelism 12 plus 1 orchestration KPU, while an application runs", kh,
        "KPU-hours", "msf_kpu_hour", kh * prices["msf_kpu_hour"])
    gbmo = F(p["msf_storage_gb"] * p["msf_app_hours"], HOURS_PER_MONTH)
    add("Managed Service for Apache Flink", "running application storage, 50 GB per KPU", gbmo, "GB-months",
        "msf_running_storage_gb_month", gbmo * prices["msf_running_storage_gb_month"])

    eh = p["vpc_endpoints"] * p["msk_hours"]
    add("Amazon VPC", "interface endpoints (Glue, and STS if the Flink app needs it), one AZ each", eh,
        "endpoint-hours", "vpc_endpoint_hour", eh * prices["vpc_endpoint_hour"])
    add("Amazon VPC", "interface endpoint data processed", F(1), "GB", "vpc_endpoint_gb", prices["vpc_endpoint_gb"])

    zstd_gb = sum(kafka[(d, "zstd")] * EVENTS for d in ("A", "B")) / GB
    none_gb = sum(kafka[(d, "none")] * p["none_events"] for d in ("A", "B")) / GB
    # Producer to partition leaders and leaders to Flink: two of three leaders sit in another AZ.
    # A cross-AZ GB is billed once leaving and once arriving.
    cross_gb = (zstd_gb + none_gb + zstd_gb) * F(2, 3) * 2
    add("Data transfer", "cross-AZ client traffic: produce zstd and none topics, Flink reads zstd topics", cross_gb,
        "GB (billed both ways)", "data_transfer_regional_gb", cross_gb * prices["data_transfer_regional_gb"])

    ice_gb = sum(ice[d] * EVENTS for d in ("A", "B")) / GB + ice["C"] * 1_000_000 / GB
    peak_gb = 2 * ice_gb + p["athena_results_gb"]
    gbmo = peak_gb * p["s3_days"] * 24 / HOURS_PER_MONTH
    add("Amazon S3", "Iceberg data and metadata at peak (before expiration removes replaced files) plus Athena results",
        gbmo, "GB-months", "s3_standard_gb_month", gbmo * prices["s3_standard_gb_month"])
    add("Amazon S3", "PUT, COPY, POST, LIST requests", p["s3_tier1_requests"], "requests", "s3_tier1_request",
        p["s3_tier1_requests"] * prices["s3_tier1_request"])
    add("Amazon S3", "GET requests (Athena and Iceberg reads)", p["s3_tier2_requests"], "requests", "s3_tier2_request",
        p["s3_tier2_requests"] * prices["s3_tier2_request"])

    a_gb, b_gb = ice["A"] * EVENTS / GB, ice["B"] * EVENTS / GB
    c_gb = ice["C"] * 1_000_000 / GB
    narrow_a, narrow_b, wide_a = a_gb * F(3, 100), b_gb * F(4, 100), a_gb * F(55, 100)
    per_run = narrow_a + wide_a + narrow_a + narrow_b * 3 + b_gb + c_gb
    equivalence = 2 * ((narrow_a + narrow_b) + (wide_a + b_gb) + (b_gb + c_gb) + (narrow_a + narrow_b))
    history = 4 * (narrow_a + narrow_b)
    smoke = F(150 * 10, 1000)
    athena_gb = per_run * 11 + equivalence + history + smoke + per_run * 10
    athena_tb = athena_gb / 1000
    add("Amazon Athena", "4 queries x 3 designs, 1 warm-up + 10 timed runs, equivalence checks, history checks, "
        "AWS smoke run, and one full repeat of the timed runs", athena_tb, "TB scanned", "athena_tb_scanned",
        athena_tb * prices["athena_tb_scanned"])

    mm = F(p["msk_per_broker_metrics"] * p["msk_brokers"] * p["msk_hours"] + p["msf_metrics"] * p["msf_app_hours"],
           HOURS_PER_MONTH)
    add("Amazon CloudWatch", "MSK PER_BROKER metrics (replication bytes) and Flink metrics", mm, "metric-months",
        "cloudwatch_metric_month", mm * prices["cloudwatch_metric_month"])
    add("Amazon CloudWatch", "GetMetricData", p["get_metric_data_metrics"], "metrics requested",
        "cloudwatch_get_metric_data_metric", p["get_metric_data_metrics"] * prices["cloudwatch_get_metric_data_metric"])
    add("Amazon CloudWatch", "Flink logs ingested (checkpoint lines are read from them)", p["logs_gb"], "GB",
        "cloudwatch_logs_ingest_gb", p["logs_gb"] * prices["cloudwatch_logs_ingest_gb"])
    gbmo = F(p["logs_gb"] * p["s3_days"] * 24, HOURS_PER_MONTH)
    add("Amazon CloudWatch", "Flink log storage", gbmo, "GB-months", "cloudwatch_logs_storage_gb_month",
        gbmo * prices["cloudwatch_logs_storage_gb_month"])
    add("AWS Glue", "Data Catalog requests (Iceberg commits, Athena planning)", p["glue_requests"], "requests",
        "glue_catalog_request", p["glue_requests"] * prices["glue_catalog_request"])
    add("AWS Cost Explorer", "API requests for the cost report", p["cost_explorer_requests"], "requests",
        "cost_explorer_api_request", p["cost_explorer_requests"] * prices["cost_explorer_api_request"])
    add("AWS Budgets", "one cost budget with email alerts, no actions", F(5), "budget-days", "budgets_budget_day",
        5 * prices["budgets_budget_day"])
    sizes = {"zstd_gb": zstd_gb, "none_gb": none_gb, "iceberg_gb": ice_gb, "athena_per_run_gb": per_run,
             "athena_total_gb": athena_gb}
    return rows, sizes


def cents_up(x):
    return F(math.ceil(x * 100), 100)


def money(x):
    return "%.2f" % float(cents_up(x))


def num(x):
    if isinstance(x, F) and x.denominator != 1:
        return "%.3f" % float(x)
    return "{:,}".format(int(x))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(ROOT, "infra", "COST_ESTIMATE.md"))
    args = ap.parse_args()
    prices, doc = load_prices()
    kafka, ice, thr = local_bytes()
    results = {}
    for name, full in (("subset", False), ("full", True)):
        results[name] = estimate(prices, kafka, ice, plan(full))
    lines = []
    w = lines.append
    w("# AWS scale run: cost estimate")
    w("")
    w("Written by `infra/estimate.py` before any resource exists. This is an estimate, not a measurement: the "
      "run reports what Cost Explorer bills. Prices are on-demand us-east-1 prices from the AWS Price List Query "
      "API, fetched %s (`infra/prices.json` keeps the SKU, usage type and effective date of each). Dollar "
      "amounts are rounded up to the cent; GB are decimal and months are 720 hours, which both make the "
      "estimate err high." % doc["fetched_utc"])
    w("")
    w("Two plans differ only in the producer compression `none` measurement: 10,000,000 events per design "
      "(the first 100,000 entities) or the full 100,000,000.")
    w("")
    for name, title in (("subset", "compression none on a 10,000,000 event subset"),
                        ("full", "compression none at full scale")):
        rows, sizes = results[name]
        w("## Plan: %s" % title)
        w("")
        w("| Service | Item | Quantity | Unit | Price (USD) | Price key | Cost (USD) |")
        w("|---|---|---|---|---|---|---|")
        for r in rows:
            w("| %s | %s | %s | %s | %s | `%s` | %s |" % (r["service"], r["item"], num(r["qty"]), r["unit"],
                                                       doc["prices"][r["price_key"]]["usd"].rstrip("0").rstrip("."),
                                                       r["price_key"], money(r["cost"])))
        by_service = {}
        for r in rows:
            by_service[r["service"]] = by_service.get(r["service"], F(0)) + r["cost"]
        w("")
        w("| Service | Cost (USD) |")
        w("|---|---|")
        for s, c in by_service.items():
            w("| %s | %s |" % (s, money(c)))
        w("| **Total** | **%s** |" % money(sum(r["cost"] for r in rows)))
        w("")
    p = plan(False)
    base = prices["ec2_m8id_2xlarge_hour"] + prices["public_ipv4_hour"] + F(p["root_volume_gb"], HOURS_PER_MONTH) * prices["ebs_gp3_gb_month"]
    stream = (p["msk_brokers"] * prices["msk_m7g_large_broker_hour"]
              + F(p["msk_brokers"] * p["msk_storage_gb_per_broker"], HOURS_PER_MONTH) * prices["msk_storage_gb_month"]
              + F(p["msk_per_broker_metrics"] * p["msk_brokers"], HOURS_PER_MONTH) * prices["cloudwatch_metric_month"]
              + p["vpc_endpoints"] * prices["vpc_endpoint_hour"])
    flink = p["msf_kpu"] * prices["msf_kpu_hour"] + F(p["msf_storage_gb"], HOURS_PER_MONTH) * prices["msf_running_storage_gb_month"]
    w("## Hourly cost while resources exist")
    w("")
    w("These rates show what an idle or forgotten stage costs. Usage charges (Athena scans, data transfer, S3 "
      "requests, log ingestion) come on top.")
    w("")
    w("| Running | USD per hour |")
    w("|---|---|")
    w("| Base stage only (runner instance, its address and root volume) | %s |" % money(base))
    w("| Base and stream stages, no Flink application running (adds MSK brokers, broker storage, PER_BROKER metrics, endpoints) | %s |" % money(base + stream))
    w("| Base and stream stages with one Flink application running (adds 13 KPU and running storage) | %s |" % money(base + stream + flink))
    w("")
    w("## Inputs and their basis")
    w("")
    w("- Kafka bytes per event, one copy, full model (results/pipeline_kafka.csv, local 3-broker run): "
      "A zstd %s, B zstd %s, A none %s, B none %s. Scaled to 100,000,000 events: zstd topics %s GB one copy "
      "for A and B together." % (float(kafka[("A", "zstd")]), float(kafka[("B", "zstd")]), float(kafka[("A", "none")]),
                                  float(kafka[("B", "none")]), num(results["subset"][1]["zstd_gb"])))
    w("- Iceberg data bytes per row, full model, after the Flink commits (results/pipeline_iceberg.csv): "
      "A %s, B %s; C %s (results/storage.csv, DuckDB zstd, 1,000 rows). Scaled to 100,000,000 rows (C: 1,000,000): "
      "%s GB for A, B and C. Compaction writes a second copy until expiration deletes the first, so S3 holds up "
      "to twice that for up to %d days." % (float(ice["A"]), float(ice["B"]), float(ice["C"]),
                                            num(results["subset"][1]["iceberg_gb"]), plan(False)["s3_days"]))
    w("- Athena bytes per query (upper bound, no file pruning): Q3 current state reads 55%% of A's bytes and all of "
      "B's; Q1, Q2 and Q4 read the key columns plus email (and previous_email in A), taken as 3%% of A and 4%% of B. "
      "One pass over all 8 supported query and design pairs: %s GB. Equivalence checks compare both directions "
      "with EXCEPT ALL, so each side is scanned twice." % num(results["subset"][1]["athena_per_run_gb"]))
    w("- Generator: bench/benchmark.py generate(), unchanged, with an Avro step-file sink, measured at 106.09 "
      "microseconds per event on an Apple M3 (200,000 events): 2.95 hours for 100,000,000 events. The plan "
      "allows 4.5 hours on the runner.")
    w("- Runner hours (36, plus 2 for the full-scale none plan): setup and build 1, generation 4.5, waiting for "
      "MSK 0.75, AWS smoke run at bench-full scale 2, full Kafka and Flink runs 8, C table and Iceberg "
      "maintenance 3, Athena phase 4, margin 12.75.")
    w("- MSK hours (16, plus 2): creation 0.75, smoke 2, full runs 8, margin 5.25. Broker storage 400 GB each, "
      "so a full uncompressed A topic (192 GB per broker) fits and gp2-style volumes keep their baseline throughput.")
    w("- Managed Flink: 6 application-hours at 13 KPU. Basis: the local runs reached %s (A) and %s (B) events per "
      "second at parallelism 3 (results/pipeline_kafka.csv, full model, zstd). If one KPU does a quarter of one "
      "local slot, parallelism 12 needs about 1.3 hours for A and 0.6 hours for B; 6 hours covers that, "
      "the smoke run, and one repeat of A." % (thr["A"], thr["B"]))
    w("- Cross-AZ data transfer uses DataTransfer-Regional-Bytes at 0.01 USD per GB each way. The Price List also "
      "returns USE1-DataTransfer-xAZ-In-Bytes and -Out-Bytes at 0.00 effective 2026-06-01; the estimate keeps the "
      "higher price.")
    w("- MSK replication between brokers is not billed (MSK pricing page). DEFAULT-level MSK metrics are free; "
      "PER_BROKER metrics, needed for ReplicationBytesInPerSec and ReplicationBytesOutPerSec, are billed as "
      "CloudWatch metrics, taken as 60 per broker.")
    w("- Not estimated because they round to zero here: KMS requests for AWS managed keys, Glue catalog storage, "
      "Pricing API calls (free).")
    w("")
    with open(args.out, "w") as fh:
        fh.write("\n".join(lines))
    for name in ("subset", "full"):
        rows, _ = results[name]
        print("%s: %s USD" % (name, money(sum(r["cost"] for r in rows))))


if __name__ == "__main__":
    main()
