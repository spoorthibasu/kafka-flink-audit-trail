#!/usr/bin/env python3
"""Resource use and cost for every (model, query, design) that bench/benchmark.py runs.

Regenerates the main FULL and SPARSE datasets from the same config and seed as
bench/benchmark.py, writes the same Parquet files, and checks their sizes against the
storage.csv already in the output directory. It then measures, per query:

  CPU time       DuckDB profiler metric cpu_time: the sum of operator_timing over every
                 operator and thread (src/main/query_profiler.cpp, DuckDB v1.4.5).
  peak memory    DuckDB profiler metric system_peak_buffer_memory: the buffer pool's used
                 memory for the whole database instance, sampled after each operator call,
                 maximum within the query.
  spill          DuckDB profiler metric system_peak_temp_dir_size: temp-directory usage,
                 sampled the same way.
  bytes scanned  compressed bytes of the Parquet column chunks the scan reads, in the row
                 groups its pushed-down filters do not prune, from parquet_metadata().

In DuckDB 1.4.5, QueryProfiler::Reset() does not clear the per-query global metrics, so
the two peak metrics are running maxima over every profiled query on a connection. Each
timed run therefore uses a fresh in-memory database: warm-up runs execute without the
profiler (the peaks are only sampled while profiling is on), then one profiled run.

Costs apply the list prices in bench/prices.json to measured bytes with exact decimal
arithmetic. Writes resources.csv, cost.csv, prices.json, resources_environment.json and
appends a "Resource use and cost" section to SUMMARY.md, replacing an earlier one.
No existing results file other than SUMMARY.md is touched.

  python3 bench/resources.py --config bench/config.json --prices bench/prices.json --out results
"""
import argparse
import csv
import json
import math
import os
import re
import shutil
import statistics
import sys
import tempfile
import time
from decimal import Decimal
from fractions import Fraction

import duckdb

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import benchmark as b  # noqa: E402  same generator, schemas, Parquet writer and queries

TABLE_FOR_DESIGN = {"A": "a", "B": "b", "C": "c"}
SECTION_HEADING = "## Resource use and cost"
REQUIRED_METRICS = ("cpu_time", "system_peak_buffer_memory", "system_peak_temp_dir_size")


# ---------------------------------------------------------------------- arithmetic


def exact(value):
    """Exact decimal string of a Fraction whose denominator divides a power of ten."""
    fr = Fraction(value)
    den = fr.denominator
    twos = fives = 0
    while den % 2 == 0:
        den //= 2
        twos += 1
    while den % 5 == 0:
        den //= 5
        fives += 1
    if den != 1:
        raise ValueError("%s has no terminating decimal expansion" % fr)
    k = max(twos, fives)
    scaled = abs(fr.numerator) * 10 ** k // fr.denominator
    digits = str(scaled).rjust(k + 1, "0")
    whole, frac = digits[:len(digits) - k], digits[len(digits) - k:]
    frac = frac.rstrip("0")
    text = whole + ("." + frac if frac else "")
    return ("-" if fr < 0 else "") + text


def median(values):
    """statistics.median over exact values: the mean of the two middle values for even n."""
    return statistics.median([Fraction(v) for v in values])


# ------------------------------------------------------------------- bytes scanned


def scan_nodes(profile):
    out = []

    def walk(node):
        if node.get("operator_type") == "TABLE_SCAN":
            out.append(node)
        for child in node.get("children", []):
            walk(child)
    walk(profile)
    return out


def as_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value]
    return [s for s in str(value).splitlines() if s.strip()]


EQ_STRING = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*'((?:[^']|'')*)'\s*$")
EQ_NUMBER = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(-?[0-9]+)\s*$")
NOT_NULL = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s+IS NOT NULL\s*$", re.IGNORECASE)


def strip_parens(text):
    """Remove outer parentheses that wrap the whole expression, as DuckDB prints '(email IS NOT NULL)'."""
    text = text.strip()
    while text.startswith("(") and text.endswith(")"):
        depth = 0
        for i, ch in enumerate(text):
            depth += ch == "("
            depth -= ch == ")"
            if depth == 0 and i < len(text) - 1:
                return text
        text = text[1:-1].strip()
    return text


def row_group_kept(rg, predicate):
    """False only when the row group's statistics prove no row can satisfy the predicate."""
    kind, column, value = predicate
    st = rg["columns"].get(column)
    if st is None:
        return True
    if kind == "not_null":
        return st["null_count"] is None or st["null_count"] < rg["num_rows"]
    lo, hi = st["min"], st["max"]
    if lo is None or hi is None:
        return True
    if kind == "eq_number":
        lo, hi = int(lo), int(hi)
    return lo <= value <= hi


def parquet_layout(con, path):
    rows = con.execute(
        "SELECT row_group_id, row_group_num_rows, path_in_schema, total_compressed_size, "
        "stats_min_value, stats_max_value, stats_null_count FROM parquet_metadata(?)", [path]).fetchall()
    groups = {}
    for rg_id, num_rows, column, compressed, lo, hi, nulls in rows:
        g = groups.setdefault(rg_id, {"num_rows": num_rows, "columns": {}})
        g["columns"][column] = {"compressed": compressed, "min": lo, "max": hi, "null_count": nulls}
    return groups


def bytes_scanned(profile, layout_by_table, table):
    """Columns = scan projections plus columns named in pushed-down filters.
    Row groups = those whose min/max or null-count statistics do not exclude every row
    under the pushed-down filters. Bytes = sum of total_compressed_size of those chunks."""
    scans = scan_nodes(profile)
    if len(scans) != 1:
        raise RuntimeError("expected one TABLE_SCAN, found %d" % len(scans))
    info = scans[0].get("extra_info", {})
    layout = layout_by_table[table]
    all_columns = set(next(iter(layout.values()))["columns"].keys())
    projections = as_list(info.get("Projections"))
    filters = []
    for f in as_list(info.get("Filters")):
        filters.extend(strip_parens(p) for p in re.split(r"\s+AND\s+", strip_parens(f)) if p.strip())
    filter_columns = set()
    predicates, unparsed = [], []
    for f in filters:
        for col in all_columns:
            if re.search(r"(?<![A-Za-z0-9_])%s(?![A-Za-z0-9_])" % re.escape(col), f):
                filter_columns.add(col)
        m = EQ_STRING.match(f)
        if m:
            predicates.append(("eq_string", m.group(1), m.group(2).replace("''", "'")))
            continue
        m = EQ_NUMBER.match(f)
        if m:
            predicates.append(("eq_number", m.group(1), int(m.group(2))))
            continue
        m = NOT_NULL.match(f)
        if m:
            predicates.append(("not_null", m.group(1), None))
            continue
        unparsed.append(f)
    unknown = [c for c in projections if c not in all_columns]
    if unknown:
        raise RuntimeError("projections not in file schema: %s" % unknown)
    columns = sorted(set(projections) | filter_columns)
    kept = [rg_id for rg_id, rg in sorted(layout.items()) if all(row_group_kept(rg, p) for p in predicates)]
    total = sum(layout[rg_id]["columns"][c]["compressed"] for rg_id in kept for c in columns)
    return {"bytes_scanned": total, "columns_read": len(columns), "columns_in_file": len(all_columns),
            "row_groups_read": len(kept), "row_groups_in_file": len(layout),
            "pushed_filters": " AND ".join(filters), "unparsed_filters": " AND ".join(unparsed)}


# ------------------------------------------------------------------------ measuring


def measure(parquet, sql, table, runs, warmup, work, layout_by_table):
    """One fresh in-memory database per timed run: views, unprofiled warm-ups, one profiled run."""
    samples = {"cpu_ms": [], "peak_buffer_bytes": [], "peak_temp_bytes": []}
    scan_info = None
    ctas = "CREATE OR REPLACE TEMP TABLE bench_result AS " + sql
    profile_path = os.path.join(work, "profile.json")
    for _ in range(runs):
        con = duckdb.connect()
        for t, p in parquet.items():
            con.execute("CREATE VIEW %s AS SELECT * FROM read_parquet('%s')" % (t, p))
        for _ in range(warmup):
            con.execute(ctas)
        con.execute("DROP TABLE IF EXISTS bench_result")
        con.execute("SET enable_profiling = 'json'")
        con.execute("SET profiling_output = '%s'" % profile_path)
        con.execute(ctas)
        con.execute("RESET enable_profiling")
        con.execute("RESET profiling_output")
        con.close()
        with open(profile_path) as fh:
            prof = json.load(fh, parse_float=Decimal)
        missing = [m for m in REQUIRED_METRICS if m not in prof]
        if missing:
            raise RuntimeError("profiler output lacks %s" % missing)
        samples["cpu_ms"].append(Decimal(prof["cpu_time"]) * 1000)
        samples["peak_buffer_bytes"].append(int(prof["system_peak_buffer_memory"]))
        samples["peak_temp_bytes"].append(int(prof["system_peak_temp_dir_size"]))
        info = bytes_scanned(prof, layout_by_table, table)
        if scan_info is None:
            scan_info = info
        elif info != scan_info:
            raise RuntimeError("scan columns or row groups changed between runs: %s vs %s" % (scan_info, info))
    return samples, scan_info


def run_model(cfg, model, attrs, work, runs, warmup, out_dir, log):
    n, h = cfg["n_entities"], cfg["h_events"]
    data_dir = os.path.join(work, model)
    os.makedirs(data_dir, exist_ok=True)
    b.generate(cfg, model, n, h, attrs, data_dir, None)
    con = duckdb.connect()
    b.load_csv(con, "a", os.path.join(data_dir, "a.csv"), b.columns_sql(attrs, True))
    b.load_csv(con, "b", os.path.join(data_dir, "b.csv"), b.columns_sql(attrs, False))
    b.load_csv(con, "c", os.path.join(data_dir, "c.csv"), b.columns_sql(attrs, False))
    rg = cfg["storage"]["parquet_row_group_size"]
    sizes, parquet = {}, {}
    for comp in cfg["storage"]["parquet_compressions"]:
        for design, table in TABLE_FOR_DESIGN.items():
            path = os.path.join(data_dir, "%s_%s.parquet" % (table, comp))
            sizes[(design, comp)] = b.write_parquet(con, table, path, comp, rg)
            if comp == cfg["storage"]["parquet_compressions"][0]:
                parquet[table] = path
    rows = {t: con.execute("SELECT count(*) FROM %s" % t).fetchone()[0] for t in TABLE_FOR_DESIGN.values()}
    layout_by_table = {t: parquet_layout(con, p) for t, p in parquet.items()}
    rg_rows = {t: describe_row_groups([g["num_rows"] for _, g in sorted(layout.items())])
               for t, layout in layout_by_table.items()}
    con.close()
    check_storage(out_dir, model, n, h, sizes)
    log("  %s: data regenerated, Parquet sizes match storage.csv" % model)

    entity = "C%07d" % (n // 2)
    results = []
    for qid, desc, scope, design, sql, reason in b.build_queries(attrs, model, entity):
        base = {"model": model, "n_entities": n, "h_events": h, "query": qid, "description": desc,
                "scope": scope, "design": design, "entity": entity if scope == "single" else ""}
        if sql is None:
            base.update({"supported": "no", "reason": reason})
            results.append(base)
            continue
        table = TABLE_FOR_DESIGN[design]
        t0 = time.perf_counter()
        samples, scan = measure(parquet, sql, table, runs, warmup, data_dir, layout_by_table)
        base.update({"supported": "yes", "reason": "", "runs": runs, "warmup_runs_per_run": warmup,
                     "cpu_ms_median": exact(median(samples["cpu_ms"])),
                     "cpu_ms_min": exact(min(samples["cpu_ms"])), "cpu_ms_max": exact(max(samples["cpu_ms"])),
                     "peak_buffer_bytes_median": exact(median(samples["peak_buffer_bytes"])),
                     "peak_buffer_bytes_min": min(samples["peak_buffer_bytes"]),
                     "peak_buffer_bytes_max": max(samples["peak_buffer_bytes"]),
                     "peak_temp_bytes_median": exact(median(samples["peak_temp_bytes"])),
                     "peak_temp_bytes_max": max(samples["peak_temp_bytes"]),
                     "parquet_file": os.path.basename(parquet[table]), "parquet_file_bytes": sizes[(design, cfg["storage"]["parquet_compressions"][0])],
                     "rows_in_file": rows[table], "row_group_rows": rg_rows[table]})
        base.update(scan)
        results.append(base)
        log("  %-36s %s cpu_ms=%s peak_bytes=%s spill=%s scanned=%s (%.1fs)" % (
            qid, design, base["cpu_ms_median"], base["peak_buffer_bytes_median"], base["peak_temp_bytes_median"],
            scan["bytes_scanned"], time.perf_counter() - t0))
    return results, sizes, rg_rows


def describe_row_groups(counts):
    """'9 x 10240 + 1 x 7840' style summary of row-group row counts, in file order."""
    parts, prev, run = [], None, 0
    for c in counts + [None]:
        if c == prev:
            run += 1
            continue
        if prev is not None:
            parts.append("%d x %d" % (run, prev))
        prev, run = c, 1
    return " + ".join(parts)


def check_storage(out_dir, model, n, h, sizes):
    """The regenerated Parquet must be byte-identical in size to what storage.csv recorded."""
    path = os.path.join(out_dir, "storage.csv")
    if not os.path.exists(path):
        raise RuntimeError("%s not found; run bench/benchmark.py with the same config first" % path)
    found = 0
    for r in csv.DictReader(open(path)):
        if r["model"] == model and int(r["n_entities"]) == n and int(r["h_events"]) == h:
            key = (r["design"], r["parquet_compression"])
            if key in sizes:
                found += 1
                if int(r["parquet_bytes"]) != sizes[key]:
                    raise RuntimeError("Parquet size differs from storage.csv for %s %s: %d vs %s"
                                       % (model, key, sizes[key], r["parquet_bytes"]))
    if found != len(sizes):
        raise RuntimeError("storage.csv has %d of %d expected rows for %s" % (found, len(sizes), model))


# ----------------------------------------------------------------------------- cost


def cost_rows(cfg, prices, resource_rows, sizes_by_model):
    s3 = prices["s3_standard_storage"]
    ath = prices["athena_sql_queries"]
    gb = Fraction(s3["gb_bytes"])
    gb_price = Fraction(s3["price_per_gb_month"])
    tb = Fraction(ath["tb_bytes"])
    mb = ath["mb_bytes"]
    tb_price = Fraction(ath["price_per_tb_scanned"])
    minimum = ath["minimum_mb_per_query"] * mb
    events = cfg["n_entities"] * cfg["h_events"]
    common = {"region": prices["region"], "date_read": prices["date_read"]}
    out = []
    for model, sizes in sizes_by_model.items():
        for (design, comp), nbytes in sorted(sizes.items(), key=lambda kv: (kv[0][1], kv[0][0])):
            per_month = Fraction(nbytes) / gb * gb_price
            out.append(dict(common, kind="storage", model=model, design=design, query="", parquet_compression=comp,
                            events_in_dataset=events, bytes=nbytes, billed_bytes="",
                            usd_per_month_dataset=exact(per_month),
                            usd_per_million_events_per_month=exact(per_month * Fraction(1_000_000, events)),
                            usd_per_1000_queries_per_tb_rate="", usd_per_1000_queries_with_minimum="",
                            price="%s USD per GB-month (%s)" % (s3["price_per_gb_month"], s3["tier"]),
                            unit="1 GB = %d bytes" % s3["gb_bytes"], url=s3["url"]))
    for r in resource_rows:
        if r["supported"] != "yes":
            continue
        scanned = r["bytes_scanned"]
        billed = max(minimum, math.ceil(scanned / mb) * mb)
        out.append(dict(common, kind="query", model=r["model"], design=r["design"], query=r["query"],
                        parquet_compression=cfg["storage"]["parquet_compressions"][0],
                        events_in_dataset=events, bytes=scanned, billed_bytes=billed,
                        usd_per_month_dataset="", usd_per_million_events_per_month="",
                        usd_per_1000_queries_per_tb_rate=exact(Fraction(scanned) * 1000 / tb * tb_price),
                        usd_per_1000_queries_with_minimum=exact(Fraction(billed) * 1000 / tb * tb_price),
                        price="%s USD per TB scanned, %d MB minimum per query, rounded up to the nearest MB" % (
                            ath["price_per_tb_scanned"], ath["minimum_mb_per_query"]),
                        unit="1 TB = %d bytes, 1 MB = %d bytes" % (ath["tb_bytes"], mb), url=ath["url"]))
    return out


# ------------------------------------------------------------------------- summary


def mb(value):
    return exact(Fraction(value) / 1_000_000)


def section(env, prices, resource_rows, cost):
    cfg = env["config"]
    s3, ath = prices["s3_standard_storage"], prices["athena_sql_queries"]
    lines = []
    w = lines.append
    w(SECTION_HEADING)
    w("")
    w("Measured by `bench/resources.py` from `%s` and `%s` on %s, on the same datasets as the tables above "
      "(seed %d, N=%d, H=%d, K=%d): the script regenerates them and checks every Parquet size against storage.csv. "
      "Machine: %s, %s logical CPUs. DuckDB %s, threads=%s. Wall time %s s. CPU time and peak memory vary between runs; "
      "bytes scanned and cost reproduce exactly." % (
          env["config_file"], env["prices_file"], env["started_utc"], cfg["seed"], cfg["n_entities"], cfg["h_events"],
          cfg["k_attributes"], env["machine"].get("cpu", env["machine"]["platform"]), env["machine"]["cpu_count"],
          env["duckdb"], env["duckdb_threads"], env["wall_seconds"]))
    w("")
    w("- CPU ms: DuckDB profiler metric `cpu_time`, the sum of `operator_timing` over every operator and thread, "
      "times 1000. It excludes parsing and planning and is not operating-system process CPU time. Values are "
      "kept exactly as DuckDB writes them to the profiler JSON; long tails such as 0.08604200000000001 come from "
      "DuckDB's floating-point summation.")
    w("- Peak memory MB: profiler metric `system_peak_buffer_memory`, the database instance's buffer pool usage "
      "sampled after each operator call, maximum within the query, in units of 1,000,000 bytes.")
    w("- Spill MB: profiler metric `system_peak_temp_dir_size`, temp-directory usage sampled the same way, "
      "in units of 1,000,000 bytes.")
    w("- In DuckDB %s these two peaks are running maxima across profiled queries on one connection "
      "(QueryProfiler::Reset does not clear them), so each of the %d timed runs uses a fresh in-memory database, "
      "executes the query for %d unprofiled warm-up runs, drops the result, and profiles the next execution. "
      "Values are the median of the %d runs (the mean of the two middle values); resources.csv also holds min and max."
      % (env["duckdb"], cfg["queries"]["runs"], cfg["queries"]["warmup_runs"], cfg["queries"]["runs"]))
    w("- Bytes scanned: from `parquet_metadata()` of the %s file each query reads. Columns are the TABLE_SCAN "
      "operator's `Projections` plus any column named in its pushed-down `Filters`. Row groups are those whose "
      "min/max statistics (or null counts, for IS NOT NULL) do not exclude every row under those filters. The value "
      "is the sum of `total_compressed_size` over those column chunks. Footer and page-index reads are excluded. "
      "DuckDB %s reports neither bytes read nor pruned row groups for Parquet, so this is derived, not observed."
      % (cfg["storage"]["parquet_compressions"][0], env["duckdb"]))
    w("- Row groups as written, read from `parquet_metadata()`: with ROW_GROUP_SIZE %d, DuckDB %s wrote %s. "
      "The Parameters table above gives the setting, not these counts." % (
          cfg["storage"]["parquet_row_group_size"], env["duckdb"],
          "; ".join("%s model: %s" % (m.upper(), ", ".join("design %s %s rows" % (d, v) for d, v in sorted(groups.items())))
                    for m, groups in env["row_group_rows"].items())))
    w("")
    for model in cfg["event_models"]:
        w("### %s model: resources" % model.upper())
        w("")
        w(b.md_table(["Query", "Design", "CPU ms", "Peak memory MB", "Spill MB", "Bytes scanned", "Columns read",
                      "Row groups read"],
                     [[r["query"], r["design"], r["cpu_ms_median"], mb(Fraction(r["peak_buffer_bytes_median"])),
                       mb(Fraction(r["peak_temp_bytes_median"])), b.fmt_bytes(r["bytes_scanned"]),
                       "%d of %d" % (r["columns_read"], r["columns_in_file"]),
                       "%d of %d" % (r["row_groups_read"], r["row_groups_in_file"])]
                      if r["supported"] == "yes" else
                      [r["query"], r["design"], "not supported: %s" % r["reason"], "", "", "", "", ""]
                      for r in resource_rows if r["model"] == model]))
        w("")
    w("### Prices")
    w("")
    w(b.md_table(["Item", "Price", "Region", "Page", "Read on"], [
        ["S3 Standard storage", "%s USD per GB-month (%s); 1 GB = %s bytes per the S3 FAQ" % (
            s3["price_per_gb_month"], s3["tier"], "{:,}".format(s3["gb_bytes"])), prices["region"], s3["url"], prices["date_read"]],
        ["Athena SQL queries", "%s USD per TB scanned; rounded up to the nearest MB; %d MB minimum per query" % (
            ath["price_per_tb_scanned"], ath["minimum_mb_per_query"]), prices["region"], ath["url"], prices["date_read"]],
    ]))
    w("")
    w(ath["unit_definition"] + " The tables use TB = %s bytes and MB = %s bytes." % (
        "{:,}".format(ath["tb_bytes"]), "{:,}".format(ath["mb_bytes"])))
    w("")
    w("### Storage cost: S3 Standard")
    w("")
    w("Parquet bytes from storage.csv times the price. Per million events scales the dataset's %s events "
      "linearly; design C's size follows the number of entities, so its per-event figure holds only for %d events "
      "per entity." % ("{:,}".format(cfg["n_entities"] * cfg["h_events"]), cfg["h_events"]))
    w("")
    w(b.md_table(["Model", "Design", "Compression", "Parquet bytes", "USD per month, dataset",
                  "USD per million events per month"],
                 [[c["model"], c["design"], c["parquet_compression"], b.fmt_bytes(c["bytes"]), c["usd_per_month_dataset"],
                   c["usd_per_million_events_per_month"]] for c in cost if c["kind"] == "storage"]))
    w("")
    w("### Query cost: Athena")
    w("")
    w("Bytes scanned times the price, per 1,000 runs of the query. \"With minimum\" bills each query at "
      "max(10 MB, bytes scanned rounded up to the nearest MB).")
    w("")
    w(b.md_table(["Model", "Query", "Design", "Bytes scanned", "Billed bytes with minimum",
                  "USD per 1,000 queries, per-TB rate", "USD per 1,000 queries, with minimum"],
                 [[c["model"], c["query"], c["design"], b.fmt_bytes(c["bytes"]), b.fmt_bytes(c["billed_bytes"]),
                   c["usd_per_1000_queries_per_tb_rate"], c["usd_per_1000_queries_with_minimum"]]
                  for c in cost if c["kind"] == "query"]))
    w("")
    w("Files: `resources.csv`, `cost.csv`, `prices.json`, `resources_environment.json`.")
    w("")
    return "\n".join(lines)


def append_section(summary_path, text):
    with open(summary_path) as fh:
        current = fh.read()
    marker = "\n" + SECTION_HEADING + "\n"
    if marker in current:
        current = current[:current.index(marker)]
    current = current.rstrip("\n") + "\n\n" + text
    with open(summary_path, "w") as fh:
        fh.write(current)


# ---------------------------------------------------------------------------- main


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="bench/config.json")
    ap.add_argument("--prices", default="bench/prices.json")
    ap.add_argument("--out", default="results")
    args = ap.parse_args()
    cfg = json.load(open(args.config))
    prices = json.load(open(args.prices))
    out_dir = args.out
    t_start = time.perf_counter()

    def log(msg):
        print("[%7.1fs] %s" % (time.perf_counter() - t_start, msg), flush=True)

    env = {"started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "config_file": args.config,
           "prices_file": args.prices, "config": cfg, "machine": b.machine_info(), "duckdb": duckdb.__version__,
           "duckdb_threads": duckdb.connect().execute("SELECT current_setting('threads')").fetchone()[0]}
    log("DuckDB %s, threads=%s" % (env["duckdb"], env["duckdb_threads"]))
    attrs = b.attributes(cfg["k_attributes"])
    work = tempfile.mkdtemp(prefix="audit-resources-")
    resource_rows, sizes_by_model = [], {}
    try:
        for model in cfg["event_models"]:
            rows, sizes, rg_rows = run_model(cfg, model, attrs, work, cfg["queries"]["runs"],
                                             cfg["queries"]["warmup_runs"], out_dir, log)
            resource_rows.extend(rows)
            sizes_by_model[model] = sizes
            env.setdefault("row_group_rows", {})[model] = {d: rg_rows[t] for d, t in TABLE_FOR_DESIGN.items()}
    finally:
        shutil.rmtree(work, ignore_errors=True)
    cost = cost_rows(cfg, prices, resource_rows, sizes_by_model)

    resource_fields = ["model", "n_entities", "h_events", "query", "description", "scope", "design", "entity",
                       "supported", "reason", "runs", "warmup_runs_per_run", "cpu_ms_median", "cpu_ms_min", "cpu_ms_max",
                       "peak_buffer_bytes_median", "peak_buffer_bytes_min", "peak_buffer_bytes_max",
                       "peak_temp_bytes_median", "peak_temp_bytes_max", "bytes_scanned", "columns_read",
                       "columns_in_file", "row_groups_read", "row_groups_in_file", "pushed_filters", "unparsed_filters",
                       "parquet_file", "parquet_file_bytes", "rows_in_file", "row_group_rows"]
    b.write_csv(os.path.join(out_dir, "resources.csv"), resource_rows, resource_fields)
    cost_fields = ["kind", "model", "query", "design", "parquet_compression", "events_in_dataset", "bytes",
                   "billed_bytes", "usd_per_month_dataset", "usd_per_million_events_per_month",
                   "usd_per_1000_queries_per_tb_rate", "usd_per_1000_queries_with_minimum", "price", "unit", "region",
                   "url", "date_read"]
    b.write_csv(os.path.join(out_dir, "cost.csv"), cost, cost_fields)
    shutil.copyfile(args.prices, os.path.join(out_dir, "prices.json"))
    env["wall_seconds"] = round(time.perf_counter() - t_start, 1)
    env["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    with open(os.path.join(out_dir, "resources_environment.json"), "w") as fh:
        json.dump(env, fh, indent=2)
    append_section(os.path.join(out_dir, "SUMMARY.md"), section(env, prices, resource_rows, cost))
    log("resources.csv, cost.csv, prices.json, resources_environment.json written; SUMMARY.md section appended")


if __name__ == "__main__":
    main()
