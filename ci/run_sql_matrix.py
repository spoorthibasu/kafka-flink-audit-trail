#!/usr/bin/env python3
"""Run every sql/ file on DuckDB, Spark and Trino against the golden cases.

For each engine and each golden case the input CSV becomes a table named
customer_profile_changes, every SQL file under sql/ runs against it, and the rows that
come back are compared, as sorted row sets, with golden/<case>/expected/<query>.csv.
The DuckDB variants under sql/duckdb/ share the expected files of their standard twins.

A file's "-- engines:" header names the engines it is written for. The exit status is
non-zero when a file fails to run or returns different rows on an engine it claims.
On an engine it does not claim the outcome is recorded and does not fail the run.

  python3 ci/run_sql_matrix.py --engines duckdb,spark,trino --out ci/matrix.md
"""
import argparse
import csv
import glob
import os
import re
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQL_DIR = os.path.join(ROOT, "sql")
GOLDEN_DIR = os.path.join(ROOT, "ci", "golden")
COLUMNS = [("customer_id", "VARCHAR"), ("event_timestamp", "BIGINT"), ("event_sequence", "BIGINT"),
           ("email", "VARCHAR"), ("phone", "VARCHAR"), ("address", "VARCHAR")]
ENGINE_LABEL = {"duckdb": "DuckDB", "spark": "Spark", "trino": "Trino"}


# ----------------------------------------------------------------------------- input


def sql_files():
    files = sorted(glob.glob(os.path.join(SQL_DIR, "*.sql"))) + sorted(glob.glob(os.path.join(SQL_DIR, "duckdb", "*.sql")))
    out = []
    for path in files:
        text = open(path).read()
        m = re.search(r"^--\s*engines:\s*(.+)$", text, re.MULTILINE)
        if not m:
            raise SystemExit("%s has no '-- engines:' header" % path)
        claimed = [e.strip().lower() for e in m.group(1).split(",")]
        # The statement is everything up to the trailing semicolon. Comment-only lines
        # after it are dropped, because Trino rejects a ';' inside a statement.
        lines = [ln for ln in text.strip().splitlines()]
        while lines and (not lines[-1].strip() or lines[-1].strip().startswith("--")):
            lines.pop()
        query = "\n".join(lines).strip()
        if query.endswith(";"):
            query = query[:-1]
        out.append({"path": os.path.relpath(path, ROOT), "name": os.path.splitext(os.path.basename(path))[0],
                    "claimed": claimed, "sql": query})
    return out


def read_rows(path):
    with open(path, newline="") as fh:
        reader = csv.reader(fh)
        header = [h.strip().lower() for h in next(reader)]
        rows = []
        for raw in reader:
            if not raw:
                continue
            rows.append(tuple(None if v == "" else v for v in raw))
    return header, rows


def typed(rows):
    """Input rows with BIGINT columns as int, for inserting."""
    out = []
    for r in rows:
        out.append((r[0], int(r[1]), int(r[2]), r[3], r[4], r[5]))
    return out


def golden_cases():
    cases = []
    for case_dir in sorted(glob.glob(os.path.join(GOLDEN_DIR, "case*"))):
        header, rows = read_rows(os.path.join(case_dir, "input.csv"))
        assert header == [c for c, _ in COLUMNS], (case_dir, header)
        expected = {}
        for exp in sorted(glob.glob(os.path.join(case_dir, "expected", "*.csv"))):
            expected[os.path.splitext(os.path.basename(exp))[0]] = read_rows(exp)
        cases.append({"name": os.path.basename(case_dir), "rows": typed(rows), "expected": expected})
    return cases


def normalize(header, rows):
    """Lower-case column names; every value to a string, None stays None; sorted."""
    def cell(v):
        if v is None:
            return None
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, float) and v.is_integer():
            return str(int(v))
        return str(v)
    norm = sorted((tuple(cell(v) for v in r) for r in rows),
                  key=lambda r: tuple("" if v is None else v for v in r))
    return [h.lower() for h in header], norm


# --------------------------------------------------------------------------- engines


class DuckDBEngine:
    name = "duckdb"

    def __init__(self):
        import duckdb
        self.con = duckdb.connect()
        self.version = duckdb.__version__

    def load(self, rows):
        self.con.execute("DROP TABLE IF EXISTS customer_profile_changes")
        self.con.execute("CREATE TABLE customer_profile_changes (%s)" % ", ".join("%s %s" % c for c in COLUMNS))
        self.con.executemany("INSERT INTO customer_profile_changes VALUES (?, ?, ?, ?, ?, ?)", rows)

    def run(self, sql):
        cur = self.con.execute(sql)
        return [d[0] for d in cur.description], cur.fetchall()

    def close(self):
        self.con.close()


class SparkEngine:
    name = "spark"

    def __init__(self):
        from pyspark.sql import SparkSession
        from pyspark.sql.types import StructType, StructField, StringType, LongType
        self.spark = (SparkSession.builder.master("local[1]").appName("sql-matrix")
                      .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
                      .getOrCreate())
        self.spark.sparkContext.setLogLevel("ERROR")
        self.version = self.spark.version
        self.schema = StructType([StructField(c, LongType() if t == "BIGINT" else StringType(), True) for c, t in COLUMNS])

    def load(self, rows):
        self.spark.createDataFrame(rows, self.schema).createOrReplaceTempView("customer_profile_changes")

    def run(self, sql):
        df = self.spark.sql(sql)
        return df.columns, [tuple(r) for r in df.collect()]

    def close(self):
        self.spark.stop()


class TrinoEngine:
    name = "trino"

    def __init__(self, host, port, catalog="memory", schema="default"):
        import trino
        self.conn = trino.dbapi.connect(host=host, port=port, user="ci", catalog=catalog, schema=schema)
        cur = self.conn.cursor()
        cur.execute("SELECT version()")
        self.version = cur.fetchall()[0][0]

    def load(self, rows):
        cur = self.conn.cursor()
        cur.execute("DROP TABLE IF EXISTS customer_profile_changes")
        cur.fetchall()
        cur.execute("CREATE TABLE customer_profile_changes (%s)" % ", ".join("%s %s" % c for c in COLUMNS))
        cur.fetchall()

        def lit(v):
            if v is None:
                return "NULL"
            if isinstance(v, int):
                return str(v)
            return "'%s'" % v.replace("'", "''")
        values = ", ".join("(%s)" % ", ".join(lit(v) for v in r) for r in rows)
        cur.execute("INSERT INTO customer_profile_changes VALUES %s" % values)
        cur.fetchall()

    def run(self, sql):
        cur = self.conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        return [d[0] for d in cur.description], [tuple(r) for r in rows]

    def close(self):
        self.conn.close()


def make_engine(name, args):
    if name == "duckdb":
        return DuckDBEngine()
    if name == "spark":
        return SparkEngine()
    if name == "trino":
        return TrinoEngine(args.trino_host, args.trino_port)
    raise SystemExit("unknown engine %s" % name)


# ------------------------------------------------------------------------------ run


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engines", default="duckdb,spark,trino")
    ap.add_argument("--trino-host", default="localhost")
    ap.add_argument("--trino-port", type=int, default=8080)
    ap.add_argument("--out", default=None, help="write the matrix as Markdown to this file")
    args = ap.parse_args()
    engines = [e.strip().lower() for e in args.engines.split(",") if e.strip()]

    files = sql_files()
    cases = golden_cases()
    results = {}   # (file, engine) -> {"status": ..., "detail": ..., "cases": {case: status}}
    versions = {}
    failures = []

    for engine_name in engines:
        t0 = time.time()
        engine = make_engine(engine_name, args)
        versions[engine_name] = engine.version
        print("== %s %s" % (ENGINE_LABEL[engine_name], engine.version), flush=True)
        for case in cases:
            engine.load(case["rows"])
            for f in files:
                key = (f["path"], engine_name)
                entry = results.setdefault(key, {"cases": {}, "claimed": engine_name in f["claimed"]})
                exp = case["expected"].get(f["name"])
                if exp is None:
                    entry["cases"][case["name"]] = "no expected file"
                    continue
                try:
                    header, rows = engine.run(f["sql"])
                except Exception as exc:
                    msg = str(exc).strip().splitlines()[0] if str(exc).strip() else type(exc).__name__
                    entry["cases"][case["name"]] = "error: %s" % msg[:160]
                    continue
                got_h, got = normalize(header, rows)
                exp_h, exp_rows = normalize(*exp)
                if got_h != exp_h:
                    entry["cases"][case["name"]] = "mismatch: columns %s, expected %s" % (got_h, exp_h)
                elif got != exp_rows:
                    entry["cases"][case["name"]] = "mismatch: rows %s, expected %s" % (got, exp_rows)
                else:
                    entry["cases"][case["name"]] = "pass"
        engine.close()
        print("   %d files x %d cases in %.1fs" % (len(files), len(cases), time.time() - t0), flush=True)

    # Summarize per (file, engine).
    for (path, engine_name), entry in results.items():
        statuses = entry["cases"].values()
        if all(s == "pass" for s in statuses):
            entry["status"] = "pass"
            entry["detail"] = ""
        else:
            bad = next(s for s in statuses if s != "pass")
            entry["status"] = "fail"
            entry["detail"] = bad
        if entry["claimed"] and entry["status"] != "pass":
            failures.append((path, engine_name, entry["detail"]))

    # Matrix.
    lines = []
    lines.append("# SQL dialect matrix")
    lines.append("")
    lines.append("Golden cases: %s. Engines: %s." % (
        ", ".join(c["name"] for c in cases),
        ", ".join("%s %s" % (ENGINE_LABEL[e], versions[e]) for e in engines)))
    lines.append("")
    lines.append("`pass` means every golden case returned exactly the expected rows. A file is only required to pass "
                 "on the engines in its `-- engines:` header; an outcome on another engine is informational and shown "
                 "in parentheses.")
    lines.append("")
    header = ["SQL file", "Written for"] + [ENGINE_LABEL[e] for e in engines]
    lines.append("| " + " | ".join(header) + " |")
    lines.append("|" + "|".join("---" for _ in header) + "|")
    for f in files:
        cells = [f["path"], ", ".join(f["claimed"])]
        for e in engines:
            entry = results[(f["path"], e)]
            if entry["claimed"]:
                cells.append(entry["status"] if entry["status"] == "pass" else "FAIL: %s" % entry["detail"])
            else:
                label = "pass" if entry["status"] == "pass" else "fail"
                cells.append("(%s, not claimed%s)" % (label, "" if label == "pass" else ": " + entry["detail"].split(":")[0]))
        lines.append("| " + " | ".join(cells) + " |")
    lines.append("")
    lines.append("## Per case")
    lines.append("")
    header = ["SQL file", "Engine"] + [c["name"] for c in cases]
    lines.append("| " + " | ".join(header) + " |")
    lines.append("|" + "|".join("---" for _ in header) + "|")
    for f in files:
        for e in engines:
            entry = results[(f["path"], e)]
            lines.append("| %s | %s | %s |" % (f["path"], ENGINE_LABEL[e],
                                               " | ".join(entry["cases"].get(c["name"], "") for c in cases)))
    lines.append("")
    if failures:
        lines.append("## Failures on claimed engines")
        lines.append("")
        for path, e, detail in failures:
            lines.append("- %s on %s: %s" % (path, ENGINE_LABEL[e], detail))
        lines.append("")
    else:
        lines.append("Every file passed on every engine it claims.")
        lines.append("")
    text = "\n".join(lines)
    print(text)
    if args.out:
        with open(args.out, "w") as fh:
            fh.write(text)
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
