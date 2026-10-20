#!/usr/bin/env python3
"""Benchmark three ways of keeping the previous value of a customer attribute.

  A  previous-value schema: every attribute x has a companion previous_x
  B  audit trail: attributes only; history is reconstructed with window functions
  C  materialized current-state table: one row per entity holding the latest values

All data is synthetic, generated from the seed in the config file at demonstration
scale. The three designs are derived from one event stream, so they describe the same
history. Every number written to results/ is measured by this script; nothing is
estimated. Run with:

  python3 bench/benchmark.py --config bench/config.json --out results

Dependencies: bench/requirements.txt (duckdb, fastavro, cramjam).
"""
import argparse
import csv
import io
import json
import math
import os
import platform
import random
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import zlib

import cramjam
import duckdb
import fastavro
from fastavro.schema import to_parsing_canonical_form

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from generate_schemas import MUTABLE_ATTRIBUTES  # noqa: E402  one attribute list for every benchmark

KEYS = [("customer_id", "string"), ("event_timestamp", "long"), ("event_sequence", "long")]
DUCK_TYPE = {"string": "VARCHAR", "long": "BIGINT", "double": "DOUBLE", "boolean": "BOOLEAN"}
BASE_TS = 1_700_000_000_000
DESIGN_NAMES = {
    "A": "A previous-value schema",
    "B": "B audit trail",
    "C": "C current-state table",
}


# ----------------------------------------------------------------------------- data


def attributes(k):
    attrs = [(name, typ) for name, typ in MUTABLE_ATTRIBUTES[:k]]
    for i in range(len(attrs), k):
        attrs.append(("attribute_%d" % i, "string"))
    return attrs


def avro_schema(name, attrs, with_previous):
    fields = [{"name": "customer_id", "type": "string"},
              {"name": "event_timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
              {"name": "event_sequence", "type": "long"}]
    for attr, typ in attrs:
        fields.append({"name": attr, "type": ["null", typ], "default": None})
        if with_previous:
            fields.append({"name": "previous_" + attr, "type": ["null", typ], "default": None})
    return {"type": "record", "name": name, "namespace": "com.example.audit.bench", "fields": fields}


class ValueGen:
    """Deterministic attribute values. Strings are 8 to 20 lowercase letters."""

    def __init__(self, rng):
        self.rng = rng
        self.letters = "abcdefghijklmnopqrstuvwxyz"

    def value(self, typ):
        r = self.rng
        if typ == "string":
            n = r.randint(8, 20)
            return "".join(r.choice(self.letters) for _ in range(n))
        if typ == "long":
            return r.randint(0, 1_000_000)
        if typ == "double":
            return round(r.uniform(0, 10_000), 2)
        if typ == "boolean":
            return r.random() < 0.5
        raise ValueError(typ)


class CsvSink:
    def __init__(self, path, header):
        self.fh = open(path, "w", newline="")
        self.w = csv.writer(self.fh)
        self.w.writerow(header)

    def row(self, values):
        self.w.writerow(["" if v is None else ("true" if v is True else "false" if v is False else v)
                         for v in values])

    def close(self):
        self.fh.close()


def generate(cfg, model, n, h, attrs, data_dir, payload_sizes=None):
    """Stream one synthetic history into CSV files for designs A, B and C.

    FULL model: every event carries every attribute (the full current state). The
    first event sets every attribute; each later event changes a random subset of
    them. In design A, previous_x on a full-state event is the previous full state.
    SPARSE model: the first event sets every attribute; each later event carries only
    the attributes it changes and null for the rest. In design A, previous_x is set
    only for a changed attribute and holds the value it replaced.
    """
    rng = random.Random("%d:%s:%d:%d" % (cfg["seed"], model, n, h))
    vg = ValueGen(rng)
    cmin = cfg["changed_attributes_per_event"]["min"]
    cmax = cfg["changed_attributes_per_event"]["max"]
    names = [a for a, _ in attrs]
    types = dict(attrs)

    key_header = [k for k, _ in KEYS]
    a_header = key_header + [c for a in names for c in (a, "previous_" + a)]
    b_header = key_header + names
    a_sink = CsvSink(os.path.join(data_dir, "a.csv"), a_header)
    b_sink = CsvSink(os.path.join(data_dir, "b.csv"), b_header)
    c_sink = CsvSink(os.path.join(data_dir, "c.csv"), b_header)

    schema_a = fastavro.parse_schema(avro_schema("PreviousValue", attrs, True)) if payload_sizes is not None else None
    schema_b = fastavro.parse_schema(avro_schema("AuditTrail", attrs, False)) if payload_sizes is not None else None
    level = cfg["payload"]["deflate_level"]

    def measure(design, schema, record):
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, schema, record)
        raw = buf.getvalue()
        d = zlib.compressobj(level, zlib.DEFLATED, -15)
        deflated = d.compress(raw) + d.flush()
        snappy = bytes(cramjam.snappy.compress_raw(raw))
        sizes = payload_sizes[design]
        sizes["none"].append(len(raw))
        sizes["deflate"].append(len(deflated))
        sizes["snappy"].append(len(snappy))

    for e in range(n):
        cid = "C%07d" % e
        state = {a: None for a in names}
        for i in range(h):
            seq = i + 1
            ts = BASE_TS + i * 60_000 + e
            if i == 0:
                changed = list(names)
            else:
                count = rng.randint(cmin, min(cmax, len(names)))
                changed = rng.sample(names, count)
            previous_state = dict(state)
            for a in changed:
                state[a] = vg.value(types[a])

            if model == "full":
                b_vals = [state[a] for a in names]
                a_vals = []
                for a in names:
                    a_vals.append(state[a])
                    a_vals.append(previous_state[a] if i > 0 else None)
            else:
                changed_set = set(changed)
                b_vals = [state[a] if a in changed_set else None for a in names]
                a_vals = []
                for a in names:
                    if a in changed_set:
                        a_vals.append(state[a])
                        a_vals.append(previous_state[a] if i > 0 else None)
                    else:
                        a_vals.append(None)
                        a_vals.append(None)

            keys = [cid, ts, seq]
            a_sink.row(keys + a_vals)
            b_sink.row(keys + b_vals)
            if payload_sizes is not None:
                measure("A", schema_a, dict(zip(a_header, keys + a_vals)))
                measure("B", schema_b, dict(zip(b_header, keys + b_vals)))
        c_sink.row([cid, ts, seq] + [state[a] for a in names])

    for s in (a_sink, b_sink, c_sink):
        s.close()


# --------------------------------------------------------------------------- helpers


def pct(values, p):
    """Nearest-rank percentile: the value at rank ceil(p/100 * n)."""
    s = sorted(values)
    k = max(1, math.ceil(p / 100.0 * len(s)))
    return s[k - 1]


def columns_sql(attrs, with_previous):
    cols = ["customer_id VARCHAR", "event_timestamp BIGINT", "event_sequence BIGINT"]
    for a, t in attrs:
        cols.append("%s %s" % (a, DUCK_TYPE[t]))
        if with_previous:
            cols.append("previous_%s %s" % (a, DUCK_TYPE[t]))
    return cols


def duck_columns_struct(cols):
    return "{" + ", ".join("'%s': '%s'" % tuple(c.split(" ", 1)) for c in cols) + "}"


def load_csv(con, table, path, cols):
    con.execute("CREATE OR REPLACE TABLE %s AS SELECT * FROM read_csv('%s', header=true, nullstr='', columns=%s)"
                % (table, path, duck_columns_struct(cols)))


def write_parquet(con, table, path, compression, row_group_size):
    con.execute("COPY (SELECT * FROM %s ORDER BY customer_id, event_timestamp, event_sequence) TO '%s' "
                "(FORMAT PARQUET, COMPRESSION %s, ROW_GROUP_SIZE %d)" % (table, path, compression, row_group_size))
    return os.path.getsize(path)


def machine_info():
    info = {"platform": platform.platform(), "machine": platform.machine(),
            "python": platform.python_version(), "cpu_count": os.cpu_count()}
    try:
        if sys.platform == "darwin":
            info["cpu"] = subprocess.check_output(["sysctl", "-n", "machdep.cpu.brand_string"]).decode().strip()
            info["memory_bytes"] = int(subprocess.check_output(["sysctl", "-n", "hw.memsize"]).decode().strip())
        elif sys.platform.startswith("linux"):
            with open("/proc/cpuinfo") as fh:
                for line in fh:
                    if line.startswith("model name"):
                        info["cpu"] = line.split(":", 1)[1].strip()
                        break
            with open("/proc/meminfo") as fh:
                for line in fh:
                    if line.startswith("MemTotal"):
                        info["memory_bytes"] = int(line.split()[1]) * 1024
                        break
    except Exception as exc:  # the benchmark still runs without hardware details
        info["hardware_lookup_error"] = str(exc)
    try:
        info["git_commit"] = subprocess.check_output(["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL).decode().strip()
    except Exception:
        info["git_commit"] = None
    return info


# --------------------------------------------------------------------------- queries


def build_queries(attrs, model, entity):
    """Return [(query_id, description, scope, design, sql or None, reason_if_none)]."""
    names = [a for a, _ in attrs]
    x = names[0]
    order = "ORDER BY event_timestamp, event_sequence"
    latest_rn = "ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY event_timestamp DESC, event_sequence DESC) AS rn"
    w_cur = "PARTITION BY customer_id ORDER BY event_timestamp, event_sequence ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
    w_prev = "PARTITION BY customer_id ORDER BY event_timestamp, event_sequence ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING"
    attr_list = ", ".join(names)
    q = []

    # Q1 one-hop previous value, single entity. Rows where x is set, so that the
    # SPARSE answer is the value x replaced on each change.
    q.append(("q1_one_hop_previous_single", "one-hop previous value of %s, single entity" % x, "single", "A",
              "SELECT event_timestamp, event_sequence, %s, previous_%s FROM a WHERE customer_id = '%s' AND %s IS NOT NULL %s"
              % (x, x, entity, x, order), None))
    q.append(("q1_one_hop_previous_single", "one-hop previous value of %s, single entity" % x, "single", "B",
              "SELECT event_timestamp, event_sequence, %s, LAG(%s) OVER (PARTITION BY customer_id %s) AS previous_%s "
              "FROM b WHERE customer_id = '%s' AND %s IS NOT NULL %s" % (x, x, order, x, entity, x, order), None))
    q.append(("q1_one_hop_previous_single", "one-hop previous value of %s, single entity" % x, "single", "C",
              None, "holds no history"))

    # Q2 three changes back, single entity.
    q.append(("q2_three_changes_back_single", "value of %s three changes back, single entity" % x, "single", "A",
              None, "holds one hop only"))
    q.append(("q2_three_changes_back_single", "value of %s three changes back, single entity" % x, "single", "B",
              "SELECT event_timestamp, event_sequence, %s, LAG(%s, 3) OVER (PARTITION BY customer_id %s) AS %s_three_back "
              "FROM b WHERE customer_id = '%s' AND %s IS NOT NULL %s" % (x, x, order, x, entity, x, order), None))
    q.append(("q2_three_changes_back_single", "value of %s three changes back, single entity" % x, "single", "C",
              None, "holds no history"))

    # Q3 current state of every attribute, all entities.
    if model == "full":
        for design, table in (("A", "a"), ("B", "b")):
            q.append(("q3_current_state_all", "current state, all %d attributes, all entities" % len(names), "all", design,
                      "SELECT customer_id, %s FROM (SELECT customer_id, %s, %s FROM %s) t WHERE rn = 1"
                      % (attr_list, attr_list, latest_rn, table), None))
    else:
        last_known = ", ".join("LAST_VALUE(%s IGNORE NULLS) OVER w_cur AS %s" % (a, a) for a in names)
        for design, table in (("A", "a"), ("B", "b")):
            q.append(("q3_current_state_all", "current state, all %d attributes, all entities" % len(names), "all", design,
                      "SELECT customer_id, %s FROM (SELECT customer_id, %s, %s FROM %s WINDOW w_cur AS (%s)) t WHERE rn = 1"
                      % (attr_list, last_known, latest_rn, table, w_cur), None))
    q.append(("q3_current_state_all", "current state, all %d attributes, all entities" % len(names), "all", "C",
              "SELECT customer_id, %s FROM c" % attr_list, None))

    # Q4 previous value of x, all entities: current x and the value it replaced.
    q.append(("q4_previous_value_all", "current and previous value of %s, all entities" % x, "all", "A",
              "SELECT customer_id, %s, previous_%s FROM (SELECT customer_id, %s, previous_%s, %s FROM a WHERE %s IS NOT NULL) t WHERE rn = 1"
              % (x, x, x, x, latest_rn, x), None))
    q.append(("q4_previous_value_all", "current and previous value of %s, all entities" % x, "all", "B",
              "SELECT customer_id, %s, previous_%s FROM (SELECT customer_id, %s, LAG(%s) OVER (PARTITION BY customer_id %s) AS previous_%s, %s "
              "FROM b WHERE %s IS NOT NULL) t WHERE rn = 1" % (x, x, x, x, order, x, latest_rn, x), None))
    q.append(("q4_previous_value_all", "current and previous value of %s, all entities" % x, "all", "C",
              None, "holds no history"))

    # Q5 last-known and previous-known value of every attribute at every event (SPARSE only).
    if model == "sparse":
        cur = ", ".join("LAST_VALUE(%s IGNORE NULLS) OVER w_cur AS current_%s" % (a, a) for a in names)
        prev_b = ", ".join("LAST_VALUE(%s IGNORE NULLS) OVER w_prev AS previous_%s" % (a, a) for a in names)
        prev_a = ", ".join("LAST_VALUE(previous_%s IGNORE NULLS) OVER w_cur AS previous_%s" % (a, a) for a in names)
        for scope, where in (("single", "WHERE customer_id = '%s' " % entity), ("all", "")):
            desc = "last-known and previous-known value of all %d attributes, %s" % (
                len(names), "single entity" if scope == "single" else "all entities")
            qid = "q5_last_and_previous_known_%s" % scope
            q.append((qid, desc, scope, "A",
                      "SELECT customer_id, event_timestamp, event_sequence, %s, %s FROM a %sWINDOW w_cur AS (%s) %s"
                      % (cur, prev_a, where, w_cur, order), None))
            q.append((qid, desc, scope, "B",
                      "SELECT customer_id, event_timestamp, event_sequence, %s, %s FROM b %sWINDOW w_cur AS (%s), w_prev AS (%s) %s"
                      % (cur, prev_b, where, w_cur, w_prev, order), None))
            q.append((qid, desc, scope, "C", None, "holds the last known value only"))
    return q


def time_query(con, sql, runs, warmup, profile_path, explain_path):
    """Wall time of CREATE OR REPLACE TEMP TABLE bench_result AS <sql>.

    The result is materialized inside DuckDB, so Python object creation is not part
    of the measurement. One extra run with profiling enabled collects rows scanned
    and bytes read. EXPLAIN ANALYZE text for the SELECT is saved beside it.
    """
    ctas = "CREATE OR REPLACE TEMP TABLE bench_result AS " + sql
    for _ in range(warmup):
        con.execute(ctas)
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        con.execute(ctas)
        times.append((time.perf_counter() - t0) * 1000.0)
    rows_returned = con.execute("SELECT count(*) FROM bench_result").fetchone()[0]

    con.execute("SET enable_profiling = 'json'")
    con.execute("SET profiling_output = '%s'" % profile_path)
    con.execute(ctas)
    con.execute("RESET enable_profiling")
    con.execute("RESET profiling_output")
    with open(profile_path) as fh:
        prof = json.load(fh)
    # DuckDB's profiler reports, for read_parquet, the file's row count once per scanning
    # thread in rows_scanned, and nothing for file bytes read. The TABLE_SCAN operators'
    # cardinality (rows emitted after pushed-down filters) and timing are what the scan
    # really did, so both are recorded; the raw profiler figure is kept for reference.
    scans = []

    def walk(node):
        if node.get("operator_type") == "TABLE_SCAN":
            scans.append(node)
        for child in node.get("children", []):
            walk(child)
    walk(prof)
    scan_rows_out = sum(int(n.get("operator_cardinality", 0)) for n in scans)
    scan_ms = round(sum(float(n.get("operator_timing", 0.0)) for n in scans) * 1000.0, 3)
    profiler_rows_scanned = prof.get("cumulative_rows_scanned")

    explain = con.execute("EXPLAIN ANALYZE " + sql).fetchall()
    with open(explain_path, "w") as fh:
        for row in explain:
            fh.write(row[1] if len(row) > 1 else str(row[0]))
            fh.write("\n")
    con.execute("DROP TABLE IF EXISTS bench_result")
    return {"runs": runs, "warmup_runs": warmup,
            "p50_ms": round(pct(times, 50), 3), "p95_ms": round(pct(times, 95), 3),
            "p99_ms": round(pct(times, 99), 3), "mean_ms": round(statistics.fmean(times), 3),
            "min_ms": round(min(times), 3), "max_ms": round(max(times), 3),
            "rows_returned": rows_returned, "scan_rows_out": scan_rows_out, "scan_ms": scan_ms,
            "profiler_rows_scanned": profiler_rows_scanned}


# ------------------------------------------------------------------------ equivalence


def check_equivalence(con, attrs, model, queries):
    """The three designs describe one history, so they must agree wherever they overlap.

    Compares, as sorted row sets: Q1 A vs B, Q3 A vs B vs C, Q4 A vs B, and in the
    SPARSE model the current_* columns of Q5 A vs B. Returns one record per comparison
    and raises if any pair differs, because a difference means the generator or a query
    is wrong and every other number would be suspect.
    """
    sql_by = {(qid, design): sql for qid, _, _, design, sql, _ in queries if sql is not None}
    names = [a for a, _ in attrs]

    def rows(sql):
        return sorted(con.execute(sql).fetchall(),
                      key=lambda r: tuple("\x00" if v is None else str(v) for v in r))

    pairs = [("q1_one_hop_previous_single", "A", "B", None),
             ("q3_current_state_all", "A", "B", None),
             ("q3_current_state_all", "B", "C", None),
             ("q4_previous_value_all", "A", "B", None)]
    if model == "sparse":
        current_cols = "customer_id, event_timestamp, event_sequence, " + ", ".join("current_" + a for a in names)
        pairs.append(("q5_last_and_previous_known_all", "A", "B", current_cols))
    records = []
    for qid, d1, d2, project in pairs:
        s1, s2 = sql_by[(qid, d1)], sql_by[(qid, d2)]
        if project:
            s1 = "SELECT %s FROM (%s) t" % (project, s1)
            s2 = "SELECT %s FROM (%s) t" % (project, s2)
        r1, r2 = rows(s1), rows(s2)
        equal = r1 == r2
        records.append({"model": model, "query": qid, "designs": "%s vs %s" % (d1, d2),
                        "columns": "current_* only" if project else "all", "rows_compared": len(r1), "equal": "yes" if equal else "no"})
        if not equal:
            raise RuntimeError("designs %s and %s disagree on %s (%s model): %d vs %d rows" % (d1, d2, qid, model, len(r1), len(r2)))
    return records


# ------------------------------------------------------------------------------ run


def run_dataset(cfg, model, n, h, attrs, work_dir, out_dir, tag, runs, warmup, compressions, with_payload, log):
    """Generate one dataset, write Parquet, run the queries. Returns (storage_rows, latency_rows, payload_sizes)."""
    data_dir = os.path.join(work_dir, tag)
    os.makedirs(data_dir, exist_ok=True)
    payload_sizes = ({"A": {"none": [], "deflate": [], "snappy": []},
                      "B": {"none": [], "deflate": [], "snappy": []}} if with_payload else None)
    t0 = time.perf_counter()
    generate(cfg, model, n, h, attrs, data_dir, payload_sizes)
    log("  generated %s: N=%d H=%d (%.1fs)" % (tag, n, h, time.perf_counter() - t0))

    con = duckdb.connect()
    load_csv(con, "a", os.path.join(data_dir, "a.csv"), columns_sql(attrs, True))
    load_csv(con, "b", os.path.join(data_dir, "b.csv"), columns_sql(attrs, False))
    load_csv(con, "c", os.path.join(data_dir, "c.csv"), columns_sql(attrs, False))
    counts = {t: con.execute("SELECT count(*) FROM %s" % t).fetchone()[0] for t in ("a", "b", "c")}
    assert counts["a"] == n * h and counts["b"] == n * h and counts["c"] == n, counts

    rg = cfg["storage"]["parquet_row_group_size"]
    storage_rows = []
    parquet_paths = {}
    for comp in compressions:
        for design, table in (("A", "a"), ("B", "b"), ("C", "c")):
            path = os.path.join(data_dir, "%s_%s.parquet" % (table, comp))
            size = write_parquet(con, table, path, comp, rg)
            row_groups = con.execute("SELECT count(DISTINCT row_group_id) FROM parquet_metadata('%s')" % path).fetchone()[0]
            storage_rows.append({"model": model, "n_entities": n, "h_events": h, "design": design,
                                 "parquet_compression": comp, "rows": counts[table], "row_groups": row_groups,
                                 "parquet_bytes": size, "bytes_per_row": round(size / counts[table], 2)})
            if comp == compressions[0]:
                parquet_paths[table] = path
    log("  parquet written for %s" % tag)

    # Queries read the Parquet files, not the in-memory tables the files were written from.
    for table in ("a", "b", "c"):
        con.execute("DROP TABLE %s" % table)
    for table, path in parquet_paths.items():
        con.execute("CREATE VIEW %s AS SELECT * FROM read_parquet('%s')" % (table, path))

    entity = "C%07d" % (n // 2)
    explain_dir = os.path.join(out_dir, "explain")
    os.makedirs(explain_dir, exist_ok=True)
    queries = build_queries(attrs, model, entity)
    equivalence = check_equivalence(con, attrs, model, queries)
    for rec in equivalence:
        rec["dataset"] = tag
    log("  equivalence checks passed for %s (%d comparisons)" % (tag, len(equivalence)))
    latency_rows = []
    for qid, desc, scope, design, sql, reason in queries:
        base = {"model": model, "n_entities": n, "h_events": h, "query": qid, "description": desc,
                "scope": scope, "design": design, "entity": entity if scope == "single" else ""}
        if sql is None:
            base.update({"supported": "no", "reason": reason, "runs": 0, "warmup_runs": 0,
                         "p50_ms": "", "p95_ms": "", "p99_ms": "", "mean_ms": "", "min_ms": "", "max_ms": "",
                         "rows_returned": "", "scan_rows_out": "", "scan_ms": "", "profiler_rows_scanned": ""})
        else:
            prof = os.path.join(data_dir, "%s_%s.prof.json" % (qid, design))
            explain = os.path.join(explain_dir, "%s_%s_%s.txt" % (tag, qid, design))
            m = time_query(con, sql, runs, warmup, prof, explain)
            base.update({"supported": "yes", "reason": ""})
            base.update(m)
            log("  %-38s %s p50=%8.2fms p95=%8.2fms rows=%d scan_rows_out=%s" % (qid, design, m["p50_ms"], m["p95_ms"], m["rows_returned"], m["scan_rows_out"]))
        latency_rows.append(base)
    con.close()
    return storage_rows, latency_rows, payload_sizes, equivalence


def write_csv(path, rows, fieldnames):
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="bench/config.json")
    ap.add_argument("--out", default="results")
    ap.add_argument("--keep-data", default=None, help="keep generated CSV and Parquet files in this directory")
    args = ap.parse_args()

    with open(args.config) as fh:
        cfg = json.load(fh)
    out_dir = args.out
    os.makedirs(out_dir, exist_ok=True)
    work_dir = args.keep_data or tempfile.mkdtemp(prefix="audit-bench-")
    os.makedirs(work_dir, exist_ok=True)
    started = time.time()
    t_start = time.perf_counter()

    def log(msg):
        print("[%7.1fs] %s" % (time.perf_counter() - t_start, msg), flush=True)

    k = cfg["k_attributes"]
    attrs = attributes(k)
    n, h = cfg["n_entities"], cfg["h_events"]
    env = {
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(started)),
        "config_file": args.config, "config": cfg, "machine": machine_info(),
        "duckdb": duckdb.__version__, "fastavro": fastavro.__version__, "cramjam": cramjam.__version__,
        "duckdb_threads": duckdb.connect().execute("SELECT current_setting('threads')").fetchone()[0],
    }
    log("DuckDB %s, threads=%s, %s" % (env["duckdb"], env["duckdb_threads"], env["machine"].get("cpu", env["machine"]["platform"])))

    # Schema metrics.
    schema_rows = []
    for design, name, with_prev in (("A", "PreviousValue", True), ("B", "AuditTrail", False), ("C", "CurrentState", False)):
        schema = avro_schema(name, attrs, with_prev)
        pretty = json.dumps(schema, indent=2) + "\n"
        canonical = to_parsing_canonical_form(fastavro.parse_schema(schema))
        schema_rows.append({"design": design, "k_attributes": k, "field_count": len(schema["fields"]),
                            "attribute_fields": len(schema["fields"]) - len(KEYS),
                            "avsc_pretty_bytes": len(pretty.encode("utf-8")),
                            "avsc_canonical_bytes": len(canonical.encode("utf-8"))})
        with open(os.path.join(out_dir, "schema_%s.avsc" % design), "w") as fh:
            fh.write(pretty)
    write_csv(os.path.join(out_dir, "schema.csv"), schema_rows, list(schema_rows[0].keys()))
    log("schema metrics written")

    # Main datasets: payload, storage, latency.
    payload_rows, storage_rows, latency_rows, equivalence_rows = [], [], [], []
    for model in cfg["event_models"]:
        tag = "main_%s" % model
        s_rows, l_rows, sizes, eq = run_dataset(
            cfg, model, n, h, attrs, work_dir, out_dir, tag,
            cfg["queries"]["runs"], cfg["queries"]["warmup_runs"],
            cfg["storage"]["parquet_compressions"], True, log)
        storage_rows.extend(s_rows)
        latency_rows.extend(l_rows)
        equivalence_rows.extend(eq)
        for design in ("A", "B"):
            for comp in ("none", "deflate", "snappy"):
                v = sizes[design][comp]
                payload_rows.append({"model": model, "design": design, "compression": comp, "events": len(v),
                                     "mean_bytes": round(statistics.fmean(v), 2), "median_bytes": pct(v, 50),
                                     "p95_bytes": pct(v, 95), "min_bytes": min(v), "max_bytes": max(v), "total_bytes": sum(v)})
    write_csv(os.path.join(out_dir, "payload.csv"), payload_rows, list(payload_rows[0].keys()))
    write_csv(os.path.join(out_dir, "storage.csv"), storage_rows, list(storage_rows[0].keys()))
    write_csv(os.path.join(out_dir, "latency.csv"), latency_rows, list(latency_rows[0].keys()))
    log("payload, storage and latency written")

    # Scaling sweep over H.
    sc = cfg["scaling"]
    scaling_storage, scaling_latency = [], []
    for model in cfg["event_models"]:
        for hh in sc["h_values"]:
            tag = "sweep_%s_h%d" % (model, hh)
            s_rows, l_rows, _, eq = run_dataset(
                cfg, model, sc["n_entities"], hh, attrs, work_dir, out_dir, tag,
                sc["runs"], sc["warmup_runs"], [cfg["storage"]["parquet_compressions"][0]], False, log)
            scaling_storage.extend(s_rows)
            scaling_latency.extend(l_rows)
            equivalence_rows.extend(eq)
    write_csv(os.path.join(out_dir, "scaling_storage.csv"), scaling_storage, list(scaling_storage[0].keys()))
    write_csv(os.path.join(out_dir, "scaling_latency.csv"), scaling_latency, list(scaling_latency[0].keys()))
    write_csv(os.path.join(out_dir, "equivalence.csv"), equivalence_rows,
              ["dataset", "model", "query", "designs", "columns", "rows_compared", "equal"])
    log("scaling sweep written")

    wall = time.perf_counter() - t_start
    env["wall_seconds"] = round(wall, 1)
    env["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    with open(os.path.join(out_dir, "environment.json"), "w") as fh:
        json.dump(env, fh, indent=2)
    write_summary(out_dir, env, attrs, schema_rows, payload_rows, storage_rows, latency_rows, scaling_storage, scaling_latency, equivalence_rows)
    log("SUMMARY.md written; wall time %.1fs" % wall)
    if not args.keep_data:
        shutil.rmtree(work_dir, ignore_errors=True)


# -------------------------------------------------------------------------- summary


def md_table(headers, rows):
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    for r in rows:
        out.append("| " + " | ".join(str(c) for c in r) + " |")
    return "\n".join(out)


def fmt_bytes(b):
    return "{:,}".format(int(b))


def write_summary(out_dir, env, attrs, schema_rows, payload_rows, storage_rows, latency_rows, scaling_storage, scaling_latency, equivalence_rows):
    cfg = env["config"]
    m = env["machine"]
    n, h, k = cfg["n_entities"], cfg["h_events"], cfg["k_attributes"]
    models = cfg["event_models"]
    lines = []
    w = lines.append
    w("# Benchmark summary")
    w("")
    w("Synthetic data at demonstration scale. Every number below was measured by `bench/benchmark.py` "
      "from `%s`; regenerate everything with `make bench-full`." % env["config_file"])
    w("")
    w("## Environment")
    w("")
    w(md_table(["Item", "Value"], [
        ["Machine", "%s, %s logical CPUs, %s GiB memory" % (m.get("cpu", "unknown"), m.get("cpu_count"),
                                                        round(m["memory_bytes"] / 2**30, 1) if "memory_bytes" in m else "unknown")],
        ["OS", m["platform"]],
        ["Python", m["python"]],
        ["DuckDB", "%s, threads=%s" % (env["duckdb"], env["duckdb_threads"])],
        ["fastavro / cramjam", "%s / %s" % (env["fastavro"], env["cramjam"])],
        ["Git commit", m.get("git_commit") or "unknown"],
        ["Started (UTC)", env["started_utc"]],
        ["Wall time", "%s s" % env["wall_seconds"]],
    ]))
    w("")
    w("## Parameters")
    w("")
    w(md_table(["Parameter", "Value"], [
        ["Seed", cfg["seed"]],
        ["N entities", n], ["H events per entity", h], ["N x H rows", n * h],
        ["K attributes", "%d (%s ... %s)" % (k, attrs[0][0], attrs[-1][0])],
        ["Event models", ", ".join(models)],
        ["Attributes changed per event", "uniform %d to %d; the first event sets all K" % (
            cfg["changed_attributes_per_event"]["min"], cfg["changed_attributes_per_event"]["max"])],
        ["Query runs", "%d timed after %d warm-up runs" % (cfg["queries"]["runs"], cfg["queries"]["warmup_runs"])],
        ["Scaling sweep", "N=%d, H in %s, %d timed runs after %d warm-up" % (
            cfg["scaling"]["n_entities"], cfg["scaling"]["h_values"], cfg["scaling"]["runs"], cfg["scaling"]["warmup_runs"])],
        ["Parquet", "compression %s; row groups of %d rows; sorted by customer_id, event_timestamp, event_sequence" % (
            ", ".join(cfg["storage"]["parquet_compressions"]), cfg["storage"]["parquet_row_group_size"])],
        ["Deflate level", cfg["payload"]["deflate_level"]],
    ]))
    w("")
    w("## Designs")
    w("")
    w("- A, previous-value schema: every attribute x has a companion previous_x in the event and the table.")
    w("- B, audit trail: attributes only; previous values are reconstructed with window functions.")
    w("- C, materialized current-state table: one row per entity holding the latest values. "
      "C consumes the same change events as B, so its wire payload is B's and is not listed separately.")
    w("")
    w("## Schema")
    w("")
    w(md_table(["Design", "Fields", "Attribute fields", "Schema bytes (pretty .avsc)", "Schema bytes (Avro canonical form)"],
               [[DESIGN_NAMES[r["design"]], r["field_count"], r["attribute_fields"], fmt_bytes(r["avsc_pretty_bytes"]),
                 fmt_bytes(r["avsc_canonical_bytes"])] for r in schema_rows]))
    w("")
    w("Field count includes the three key fields (customer_id, event_timestamp, event_sequence). The canonical "
      "form is Avro's Parsing Canonical Form, which drops defaults, docs and whitespace. Design C's schema "
      "describes the table row, which has the same shape as design B's event.")
    w("")
    w("## Payload: Avro binary bytes per event")
    w("")
    w("Avro binary encoding of each event with fastavro's schemaless writer: datum bytes only. Excluded: "
      "Confluent wire-format framing (1 magic byte plus 4-byte schema id), Avro object container headers and "
      "sync markers, Kafka record headers. Compression is applied to each event on its own: deflate is raw "
      "DEFLATE (RFC 1951, zlib level %d, the same stream Avro's deflate codec writes); snappy is the raw snappy "
      "block format with no framing. Kafka producers compress whole record batches, so a batch of similar events "
      "compresses further than these per-event numbers. Percentiles are nearest-rank over all N x H events." % cfg["payload"]["deflate_level"])
    w("")
    for model in models:
        w("### %s model" % model.upper())
        w("")
        rows = [r for r in payload_rows if r["model"] == model]
        w(md_table(["Design", "Compression", "Events", "Mean bytes", "Median bytes", "p95 bytes", "Min", "Max", "Total bytes"],
                   [[DESIGN_NAMES[r["design"]], r["compression"], fmt_bytes(r["events"]), r["mean_bytes"], r["median_bytes"],
                     r["p95_bytes"], r["min_bytes"], r["max_bytes"], fmt_bytes(r["total_bytes"])] for r in rows]))
        w("")
    w("## Storage: Parquet on disk, full N x H dataset")
    w("")
    w("Written by DuckDB's Parquet writer from the same rows, sorted by customer_id, event_timestamp, "
      "event_sequence, with the row group size above. Iceberg's own writer uses different defaults (zstd, "
      "128 MB row groups), so absolute sizes differ there; the ratios between designs are what this table shows. "
      "Design C holds N rows whatever H is.")
    w("")
    for model in models:
        w("### %s model" % model.upper())
        w("")
        rows = [r for r in storage_rows if r["model"] == model]
        w(md_table(["Design", "Compression", "Rows", "Row groups", "Parquet bytes", "Bytes per row"],
                   [[DESIGN_NAMES[r["design"]], r["parquet_compression"], fmt_bytes(r["rows"]), r["row_groups"],
                     fmt_bytes(r["parquet_bytes"]), r["bytes_per_row"]] for r in rows]))
        w("")
    w("## Query latency in DuckDB")
    w("")
    w("Each query reads the %s Parquet file through read_parquet. Latency is the wall time of "
      "`CREATE OR REPLACE TEMP TABLE ... AS <query>`, so the result is materialized inside DuckDB and Python "
      "object creation is excluded. Percentiles are nearest-rank over the timed runs. Scan rows out and scan ms "
      "come from DuckDB's JSON profiler on one extra run: the TABLE_SCAN operators' cardinality (rows emitted "
      "after pushed-down filters and row-group pruning) and their operator time. Bytes read are not available: "
      "DuckDB %s reports total_bytes_read as 0 for read_parquet, and its cumulative_rows_scanned for a Parquet "
      "scan is the file's row count once per scanning thread, so neither reflects I/O; the raw figure is kept in "
      "latency.csv as profiler_rows_scanned. The EXPLAIN ANALYZE text for every query is under results/explain/. "
      "The single entity is C%07d. \"not supported\" marks a question the design cannot answer from its own data." % (
          cfg["storage"]["parquet_compressions"][0], env["duckdb"], n // 2))
    w("")
    for model in models:
        w("### %s model" % model.upper())
        w("")
        rows = [r for r in latency_rows if r["model"] == model]
        table = []
        for r in rows:
            if r["supported"] == "yes":
                table.append([r["query"], r["design"], r["p50_ms"], r["p95_ms"], r["p99_ms"], fmt_bytes(r["rows_returned"]),
                              fmt_bytes(r["scan_rows_out"]), r["scan_ms"]])
            else:
                table.append([r["query"], r["design"], "not supported: %s" % r["reason"], "", "", "", "", ""])
        w(md_table(["Query", "Design", "p50 ms", "p95 ms", "p99 ms", "Rows returned", "Scan rows out", "Scan ms"], table))
        w("")
        seen = set()
        for r in rows:
            if r["query"] not in seen:
                seen.add(r["query"])
                w("- %s: %s" % (r["query"], r["description"]))
        w("")
    w("## Scaling with history length H")
    w("")
    w("N=%d entities, H swept over %s, %s model(s). Storage is the %s Parquet file. Latency is p50 / p95 in ms "
      "over %d timed runs. This answers whether query cost grows with the length of a customer's history." % (
          cfg["scaling"]["n_entities"], cfg["scaling"]["h_values"], ", ".join(models),
          cfg["storage"]["parquet_compressions"][0], cfg["scaling"]["runs"]))
    w("")
    for model in models:
        w("### %s model: storage" % model.upper())
        w("")
        rows = [r for r in scaling_storage if r["model"] == model]
        hs = sorted(set(r["h_events"] for r in rows))
        table = []
        for design in ("A", "B", "C"):
            line = [DESIGN_NAMES[design]]
            for hh in hs:
                r = next(x for x in rows if x["design"] == design and x["h_events"] == hh)
                line.append("%s (%s rows)" % (fmt_bytes(r["parquet_bytes"]), fmt_bytes(r["rows"])))
            table.append(line)
        w(md_table(["Design"] + ["H=%d" % hh for hh in hs], table))
        w("")
        w("### %s model: latency p50 / p95 ms" % model.upper())
        w("")
        rows = [r for r in scaling_latency if r["model"] == model]
        qids = []
        for r in rows:
            if r["query"] not in qids:
                qids.append(r["query"])
        table = []
        for qid in qids:
            for design in ("A", "B", "C"):
                sub = [r for r in rows if r["query"] == qid and r["design"] == design]
                if not sub:
                    continue
                if sub[0]["supported"] != "yes":
                    table.append([qid, design, "not supported: %s" % sub[0]["reason"]] + [""] * (len(hs) - 1))
                    continue
                line = [qid, design]
                for hh in hs:
                    r = next(x for x in sub if x["h_events"] == hh)
                    line.append("%s / %s" % (r["p50_ms"], r["p95_ms"]))
                table.append(line)
        w(md_table(["Query", "Design"] + ["H=%d" % hh for hh in hs], table))
        w("")
        w("### %s model: scan rows out / scan ms" % model.upper())
        w("")
        table = []
        for qid in qids:
            for design in ("A", "B", "C"):
                sub = [r for r in rows if r["query"] == qid and r["design"] == design and r["supported"] == "yes"]
                if not sub:
                    continue
                line = [qid, design]
                for hh in hs:
                    r = next(x for x in sub if x["h_events"] == hh)
                    line.append("%s / %s" % (fmt_bytes(r["scan_rows_out"]), r["scan_ms"]))
                table.append(line)
        w(md_table(["Query", "Design"] + ["H=%d" % hh for hh in hs], table))
        w("")
    w("## Equivalence of the three designs")
    w("")
    w("Before timing, each dataset was checked so that the designs agree wherever they answer the same "
      "question: the row sets below were compared after sorting. A difference would have aborted the run.")
    w("")
    w(md_table(["Dataset", "Query", "Designs", "Columns compared", "Rows compared", "Equal"],
               [[r["dataset"], r["query"], r["designs"], r["columns"],
                 fmt_bytes(r["rows_compared"]), r["equal"]] for r in equivalence_rows]))
    w("")
    w("## Files")
    w("")
    w("- `schema.csv`, `schema_A.avsc`, `schema_B.avsc`, `schema_C.avsc`")
    w("- `payload.csv`, `storage.csv`, `latency.csv`, `equivalence.csv`")
    w("- `scaling_storage.csv`, `scaling_latency.csv`")
    w("- `explain/` EXPLAIN ANALYZE text per dataset, query and design")
    w("- `environment.json` machine, versions, configuration, wall time")
    w("")
    with open(os.path.join(out_dir, "SUMMARY.md"), "w") as fh:
        fh.write("\n".join(lines))


if __name__ == "__main__":
    main()
