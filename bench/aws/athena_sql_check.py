#!/usr/bin/env python3
"""Run the standard-form sql/ files on Athena engine version 3 against the CI golden cases.

Creates one temporary S3 bucket, one Glue database (athena_sql_check) and one Athena
workgroup (athena-sql-check) with engine version 3 selected, each tagged
run=athena-sql-check and project=audit-trail-bench. The IAM permissions are in
infra/iam/athena-sql-check-policy.json.

Each golden input, ci/golden/<case>/input.csv, is written to one Parquet file with the
column types ci/run_sql_matrix.py uses and uploaded to its own prefix in the bucket.
For each case the Glue table customer_profile_changes is created over that prefix,
SELECT * FROM customer_profile_changes is checked against the input rows, and then every
sql/*.sql file (not sql/duckdb/) runs with the file's full text, comments and trailing
semicolon included, as the query string. The rows that come back are compared with
ci/golden/<case>/expected/<file>.csv as ci/run_sql_matrix.py compares them: column
names, then rows as sorted row sets. Result reuse is off.

Billed bytes and cost per query follow bench/aws/athena.py: bytes scanned rounded up to
the next MB, at least 10 MB, at the price in infra/prices.json.

  python3 bench/aws/athena_sql_check.py setup --bucket <name>
  python3 bench/aws/athena_sql_check.py run --bucket <name> --out results
  python3 bench/aws/athena_sql_check.py teardown --bucket <name>
  python3 bench/aws/athena_sql_check.py check-teardown --bucket <name> --out results
  python3 bench/aws/athena_sql_check.py report --bucket <name> --out results

If a file fails as written, its statement as ci/run_sql_matrix.py sends it (trailing
comment lines and the semicolon removed) runs next and is recorded beside it.

The bucket name and the account ID never reach a results file.
"""
import argparse
import collections
import csv
import datetime
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "..", "ci"))
from athena import billed_bytes, cost_usd  # noqa: E402  Athena's billing rule, as in the scale run
from awscli import aws, aws_status, ROOT  # noqa: E402
import run_sql_matrix as m  # noqa: E402  same files, golden cases and comparison as CI
sys.path.insert(0, os.path.join(ROOT, "bench"))
from summary_sections import replace_section, exact_or_prefix  # noqa: E402

REGION = "us-east-1"
WORKGROUP = "athena-sql-check"
DATABASE = "athena_sql_check"
TABLE = "customer_profile_changes"
ENGINE = "Athena engine version 3"
TAGS = {"run": "athena-sql-check", "project": "audit-trail-bench"}
HIVE_TYPE = {"VARCHAR": "string", "BIGINT": "bigint"}
SECTION_HEADING = "## sql/ files on Athena engine version 3"


def standard_files():
    return [f for f in m.sql_files() if not f["path"].startswith("sql/duckdb/")]


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


# ------------------------------------------------------------------------------ setup


def write_parquet(case, path):
    con = duckdb.connect()
    con.execute("CREATE TABLE t (%s)" % ", ".join("%s %s" % c for c in m.COLUMNS))
    con.executemany("INSERT INTO t VALUES (?, ?, ?, ?, ?, ?)", case["rows"])
    con.execute("COPY t TO '%s' (FORMAT PARQUET)" % path)
    back = con.execute("SELECT * FROM read_parquet('%s')" % path).fetchall()
    con.close()
    if back != case["rows"]:
        raise RuntimeError("%s: the Parquet file does not hold the input rows" % case["name"])


def preflight(bucket):
    """Nothing with these names may exist before setup; a bucket name owned elsewhere answers 403."""
    for check, args, ok in (("bucket", ("s3api", "head-bucket", "--bucket", bucket), "Not Found"),
                            ("glue database", ("glue", "get-database", "--name", DATABASE), "EntityNotFoundException"),
                            ("workgroup", ("athena", "get-work-group", "--work-group", WORKGROUP), "is not found")):
        rc, _, err = aws_status(*args, region=REGION)
        if rc == 0 or ok not in err:
            raise SystemExit("preflight: %s already exists or cannot be checked: %s" % (check, " ".join(err.split())[:200]))
    print("preflight: bucket, glue database and workgroup do not exist")


def setup(a):
    preflight(a.bucket)
    tags_kv = ",".join("%s=%s" % kv for kv in TAGS.items())
    aws("s3api", "create-bucket", "--bucket", a.bucket, region=REGION)
    aws("s3api", "put-bucket-tagging", "--bucket", a.bucket, "--tagging",
        json.dumps({"TagSet": [{"Key": k, "Value": v} for k, v in TAGS.items()]}), region=REGION, parse=False)
    print("bucket created and tagged")
    aws("glue", "create-database", "--database-input",
        json.dumps({"Name": DATABASE, "Description": "Temporary: sql/ files on Athena engine version 3"}),
        "--tags", tags_kv, region=REGION, parse=False)
    print("glue database created")
    config = {"ResultConfiguration": {"OutputLocation": "s3://%s/athena-results/" % a.bucket},
              "EnforceWorkGroupConfiguration": True, "PublishCloudWatchMetricsEnabled": False,
              "BytesScannedCutoffPerQuery": 10000000, "RequesterPaysEnabled": False,
              "EngineVersion": {"SelectedEngineVersion": ENGINE}}
    aws("athena", "create-work-group", "--name", WORKGROUP, "--description",
        "Temporary: sql/ files on Athena engine version 3", "--configuration", json.dumps(config),
        "--tags", *["Key=%s,Value=%s" % kv for kv in TAGS.items()], region=REGION, parse=False)
    print("workgroup created")
    work = tempfile.mkdtemp(prefix="athena-sql-check-")
    for case in m.golden_cases():
        path = os.path.join(work, "%s.parquet" % case["name"])
        write_parquet(case, path)
        aws("s3api", "put-object", "--bucket", a.bucket, "--key", "input/%s/input.parquet" % case["name"],
            "--body", path, region=REGION)
        print("uploaded %s (%d rows, %d bytes)" % (case["name"], len(case["rows"]), os.path.getsize(path)))
    wg = aws("athena", "get-work-group", "--work-group", WORKGROUP, region=REGION)["WorkGroup"]
    print("workgroup engine version: %s" % json.dumps(wg["Configuration"]["EngineVersion"]))


# -------------------------------------------------------------------------------- run


def point_table_at(bucket, case_name):
    aws_status("glue", "delete-table", "--database-name", DATABASE, "--name", TABLE, region=REGION)
    table = {"Name": TABLE, "TableType": "EXTERNAL_TABLE",
             "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE"},
             "StorageDescriptor": {
                 "Columns": [{"Name": c, "Type": HIVE_TYPE[t]} for c, t in m.COLUMNS],
                 "Location": "s3://%s/input/%s/" % (bucket, case_name),
                 "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                 "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                 "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"}}}
    aws("glue", "create-table", "--database-name", DATABASE, "--table-input", json.dumps(table), region=REGION, parse=False)


def athena_query(text, work):
    """Runs one query string exactly as given. Returns (GetQueryExecution record, header, rows)."""
    request = {"QueryString": text, "WorkGroup": WORKGROUP,
               "QueryExecutionContext": {"Database": DATABASE, "Catalog": "AwsDataCatalog"},
               "ResultReuseConfiguration": {"ResultReuseByAgeConfiguration": {"Enabled": False}}}
    path = os.path.join(work, "request.json")
    with open(path, "w") as fh:
        json.dump(request, fh)
    qid = aws("athena", "start-query-execution", "--cli-input-json", "file://" + path, region=REGION)["QueryExecutionId"]
    deadline = time.time() + 300
    while True:
        time.sleep(1)
        q = aws("athena", "get-query-execution", "--query-execution-id", qid, region=REGION)["QueryExecution"]
        if q["Status"]["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.time() > deadline:
            aws("athena", "stop-query-execution", "--query-execution-id", qid, region=REGION, check=False)
            raise RuntimeError("Athena query %s timed out" % qid)
    if q["Status"]["State"] != "SUCCEEDED":
        return q, None, None
    res = aws("athena", "get-query-results", "--query-execution-id", qid, region=REGION)
    header = [c["Name"] for c in res["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    rows = [tuple(d.get("VarCharValue") for d in r["Data"]) for r in res["ResultSet"]["Rows"]]
    if not rows or list(rows[0]) != header:
        raise RuntimeError("%s: first result row is not the header" % qid)
    return q, header, rows[1:]


def compare(header, rows, expected):
    """ci/run_sql_matrix.py's comparison, plus counts: rows matched is the multiset
    intersection of returned and expected rows."""
    got_h, got = m.normalize(header, rows)
    exp_h, exp = m.normalize(*expected)
    matched = sum((collections.Counter(got) & collections.Counter(exp)).values())
    if got_h != exp_h:
        return "mismatch", len(exp), len(got), matched, "columns %s, expected %s" % (got_h, exp_h)
    if got != exp:
        missing = list((collections.Counter(exp) - collections.Counter(got)).elements())
        extra = list((collections.Counter(got) - collections.Counter(exp)).elements())
        return "mismatch", len(exp), len(got), matched, "missing %s, extra %s" % (missing, extra)
    return "pass", len(exp), len(got), matched, ""


def record(q, text, sql_file, case, kind, result, n_exp, n_got, matched, difference):
    s = q.get("Statistics", {})
    scanned = s.get("DataScannedInBytes", 0)
    status = q["Status"]
    error = "" if status["State"] == "SUCCEEDED" else "%s: %s" % (status["State"], status.get("StateChangeReason", ""))
    return {"sql_file": sql_file, "case": case, "kind": kind, "query_sha256": sha256_bytes(text.encode("utf-8")),
            "athena_recorded_same_text": "yes" if q.get("Query") == text else "no",
            "query_execution_id": q["QueryExecutionId"],
            "engine_version": q.get("EngineVersion", {}).get("EffectiveEngineVersion", ""),
            "state": status["State"], "result": result if not error else "error",
            "rows_expected": n_exp, "rows_returned": n_got, "rows_matched": matched,
            "difference": difference or error, "data_scanned_bytes": scanned,
            "billed_bytes": billed_bytes(scanned), "cost_usd": exact_or_prefix(cost_usd(scanned)),
            "engine_execution_ms": s.get("EngineExecutionTimeInMillis", ""),
            "total_execution_ms": s.get("TotalExecutionTimeInMillis", ""),
            "submitted_utc": str(status.get("SubmissionDateTime", ""))}


def read_tags(bucket):
    """The tags each service reports for the three resources, read back before the queries."""
    account = aws("sts", "get-caller-identity")["Account"]
    s3 = aws("s3api", "get-bucket-tagging", "--bucket", bucket, region=REGION)["TagSet"]
    glue = aws("glue", "get-tags", "--resource-arn", "arn:aws:glue:%s:%s:database/%s" % (REGION, account, DATABASE),
               region=REGION)["Tags"]
    athena = aws("athena", "list-tags-for-resource", "--resource-arn",
                 "arn:aws:athena:%s:%s:workgroup/%s" % (REGION, account, WORKGROUP), region=REGION)["Tags"]
    return {"s3 bucket": {t["Key"]: t["Value"] for t in s3}, "glue database": glue,
            "athena workgroup": {t["Key"]: t["Value"] for t in athena}}


def run(a):
    files = standard_files()
    cases = m.golden_cases()
    work = tempfile.mkdtemp(prefix="athena-sql-check-")
    wg = aws("athena", "get-work-group", "--work-group", WORKGROUP, region=REGION)["WorkGroup"]
    env = {"started_utc": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
           "region": REGION, "workgroup": WORKGROUP, "database": DATABASE, "table": TABLE,
           "workgroup_engine_version": wg["Configuration"]["EngineVersion"],
           "duckdb_parquet_writer": duckdb.__version__,
           "tags": read_tags(a.bucket),
           "files": [{"path": f["path"], "sha256": sha256_bytes(open(os.path.join(ROOT, f["path"]), "rb").read())}
                     for f in files]}
    records = []
    for case in cases:
        point_table_at(a.bucket, case["name"])
        expected_input = ([c for c, _ in m.COLUMNS], [tuple(None if v is None else str(v) for v in r) for r in case["rows"]])
        queries = [("SELECT * FROM %s" % TABLE, "(input check)", "input", expected_input)]
        for f in files:
            queries.append((open(os.path.join(ROOT, f["path"]), "rb").read().decode("utf-8"), f["path"], "file as written",
                            case["expected"][f["name"]]))
        for text, label, kind, expected in queries:
            q, header, rows = athena_query(text, work)
            res = compare(header, rows, expected) if header else ("error", len(expected[1]), 0, 0, "")
            records.append(record(q, text, label, case["name"], kind, *res))
            print("%-26s %-38s %s %s" % (case["name"], label, records[-1]["result"], records[-1]["difference"][:200]))
            if kind == "file as written" and records[-1]["result"] == "error":
                ci_form = next(f["sql"] for f in files if f["path"] == label)
                q, header, rows = athena_query(ci_form, work)
                res = compare(header, rows, expected) if header else ("error", len(expected[1]), 0, 0, "")
                records.append(record(q, ci_form, label, case["name"], "file, CI statement form", *res))
                print("%-26s %-38s %s %s" % (case["name"], label + " (CI form)", records[-1]["result"],
                                             records[-1]["difference"][:200]))
    env["finished_utc"] = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with open(os.path.join(a.out, "athena_sql_check.csv"), "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(records[0].keys()))
        w.writeheader()
        w.writerows(records)
    with open(os.path.join(a.out, "athena_sql_check_environment.json"), "w") as fh:
        json.dump(env, fh, indent=2)
    print("%d queries, %d passed" % (len(records), sum(r["result"] == "pass" for r in records)))


# --------------------------------------------------------------------------- teardown


def teardown(a):
    aws_status("glue", "delete-table", "--database-name", DATABASE, "--name", TABLE, region=REGION)
    rc, _, err = aws_status("glue", "delete-database", "--name", DATABASE, region=REGION)
    print("glue database: %s" % ("deleted" if rc == 0 else err.strip()[:200]))
    rc, _, err = aws_status("athena", "delete-work-group", "--work-group", WORKGROUP, "--recursive-delete-option",
                            region=REGION)
    print("workgroup: %s" % ("deleted" if rc == 0 else err.strip()[:200]))
    uploads = aws("s3api", "list-multipart-uploads", "--bucket", a.bucket, region=REGION) or {}
    for u in uploads.get("Uploads", []):
        aws("s3api", "abort-multipart-upload", "--bucket", a.bucket, "--key", u["Key"], "--upload-id", u["UploadId"],
            region=REGION, parse=False)
    deleted = 0
    while True:
        listing = aws("s3api", "list-objects-v2", "--bucket", a.bucket, "--max-items", "1000", region=REGION) or {}
        keys = [o["Key"] for o in listing.get("Contents", [])]
        if not keys:
            break
        aws("s3api", "delete-objects", "--bucket", a.bucket, "--delete",
            json.dumps({"Objects": [{"Key": k} for k in keys], "Quiet": True}), region=REGION, parse=False)
        deleted += len(keys)
    print("objects deleted: %d, multipart uploads aborted: %d" % (deleted, len(uploads.get("Uploads", []))))
    aws("s3api", "delete-bucket", "--bucket", a.bucket, region=REGION, parse=False)
    print("bucket deleted")


def check_teardown(a):
    """Asks each service for each resource by name, and the tagging API for anything tagged
    run=athena-sql-check. Writes the answers with the bucket name and account replaced."""
    account = aws("sts", "get-caller-identity")["Account"]
    lines, leftovers = [], []

    def note(check, result):
        lines.append("%-56s %s" % (check, result))

    def absent(check, *args):
        rc, out, err = aws_status(*args, region=REGION)
        text = " ".join(err.split())
        if rc == 0:
            note(check, "STILL PRESENT")
            leftovers.append(check)
        elif any(s in text for s in ("Not Found", "NoSuchBucket", "EntityNotFoundException", "is not found")):
            note(check, "not found (%s)" % text.replace("aws: [ERROR]: ", "")[:110])
        else:
            note(check, "ERROR: %s" % text[:200])
            leftovers.append(check)

    absent("s3 head-bucket <bucket>", "s3api", "head-bucket", "--bucket", a.bucket)
    names = aws("s3api", "list-buckets", "--query", "Buckets[].Name", region=REGION)
    note("s3 list-buckets contains <bucket>", "yes" if a.bucket in names else "no")
    if a.bucket in names:
        leftovers.append("bucket listed")
    absent("glue get-database %s" % DATABASE, "glue", "get-database", "--name", DATABASE)
    absent("athena get-work-group %s" % WORKGROUP, "athena", "get-work-group", "--work-group", WORKGROUP)
    groups = [g["Name"] for g in aws("athena", "list-work-groups", region=REGION)["WorkGroups"]]
    note("athena list-work-groups", ", ".join(sorted(groups)))
    if WORKGROUP in groups:
        leftovers.append("workgroup listed")
    tagged = aws("resourcegroupstaggingapi", "get-resources", "--tag-filters", "Key=run,Values=athena-sql-check",
                 region=REGION)["ResourceTagMappingList"]
    note("tag:GetResources run=athena-sql-check", "%d resources" % len(tagged))
    for t in tagged:
        note("  tagged (the tagging API can lag deletions by hours)", t["ResourceARN"])
    text = "\n".join(["Athena sql/ check teardown, %s UTC, region %s" % (
        datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"), REGION)] + lines
                     + ["", "leftovers: %s" % (", ".join(leftovers) if leftovers else "none")]) + "\n"
    text = text.replace(account, "<account>").replace(a.bucket, "<bucket>")
    with open(os.path.join(a.out, "athena_sql_check_teardown.txt"), "w") as fh:
        fh.write(text)
    print(text)
    sys.exit(1 if leftovers else 0)


# ----------------------------------------------------------------------------- report


def md_table(headers, rows):
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def report(a):
    """athena_sql_check.md and the SUMMARY.md section, from the files run and check-teardown wrote."""
    from fractions import Fraction
    recs = list(csv.DictReader(open(os.path.join(a.out, "athena_sql_check.csv"))))
    env = json.load(open(os.path.join(a.out, "athena_sql_check_environment.json")))
    teardown_text = open(os.path.join(a.out, "athena_sql_check_teardown.txt")).read()
    leftovers = teardown_text.strip().splitlines()[-1].split(": ", 1)[1]
    price = json.load(open(os.path.join(ROOT, "infra", "prices.json")))["prices"]["athena_tb_scanned"]["usd"]
    versions = sorted(set(r["engine_version"] for r in recs))
    inputs = [r for r in recs if r["kind"] == "input"]
    as_written = [r for r in recs if r["kind"] == "file as written"]
    fallback = [r for r in recs if r["kind"] == "file, CI statement form"]
    sha = {f["path"]: f["sha256"] for f in env["files"]}
    wg = env["workgroup_engine_version"]
    total_cost = sum(Fraction(r["cost_usd"]) for r in recs)

    def per_file(rows):
        out = []
        for path in sha:
            mine = [r for r in rows if r["sql_file"] == path]
            if not mine:
                continue
            diffs = "; ".join("%s: %s" % (r["case"], r["difference"]) for r in mine if r["result"] != "pass")
            shas = sorted(set(r["query_sha256"][:16] for r in mine))
            out.append(["`%s`" % path, ", ".join("`%s`" % h for h in shas), "%d of %d" % (sum(r["result"] == "pass" for r in mine), len(mine)),
                        "%d of %d" % (sum(int(r["rows_matched"]) for r in mine), sum(int(r["rows_expected"]) for r in mine)),
                        "%d of %d" % (sum(r["athena_recorded_same_text"] == "yes" for r in mine), len(mine)),
                        diffs or "none"])
        return out

    file_headers = ["SQL file", "Query SHA-256 (first 16 hex)", "Cases passed", "Rows matched of expected",
                    "Query text Athena recorded equals the text sent", "Differences"]
    verbatim = sum(r["query_sha256"] == sha[r["sql_file"]] for r in as_written)
    tagged = all(t.get(k) == v for t in env["tags"].values() for k, v in TAGS.items())
    passed_files = [row for row in per_file(as_written) if row[2].split(" of ")[0] == row[2].split(" of ")[1]]
    lines = []
    w = lines.append
    w("# sql/ files on Athena engine version 3")
    w("")
    w("Run by `bench/aws/athena_sql_check.py` from %s to %s in %s. Athena workgroup `%s` with engine version "
      "selected \"%s\" (effective \"%s\"); every query reported EffectiveEngineVersion %s. Glue Data Catalog "
      "database `%s`, external table `%s`; one temporary S3 bucket held the inputs and the query results. %s All three "
      "were deleted after the run: `athena_sql_check_teardown.txt`, leftovers: %s."
      % (env["started_utc"], env["finished_utc"], env["region"], env["workgroup"], wg["SelectedEngineVersion"],
         wg.get("EffectiveEngineVersion", ""), " and ".join("\"%s\"" % v for v in versions), env["database"],
         env["table"], "Read back before the queries, each of the three carried the tags %s." % ", ".join(
             "%s=%s" % kv for kv in TAGS.items()) if tagged else "Tags read back: %s." % json.dumps(env["tags"]),
         leftovers))
    w("")
    w("Inputs: the four CI golden cases, `ci/golden/<case>/input.csv`, each written to one Parquet file by DuckDB %s "
      "with the column types of `ci/run_sql_matrix.py` (customer_id, email, phone and address string; event_timestamp "
      "and event_sequence bigint) and read through `%s`, which pointed at one case at a time. Before each case's "
      "queries, `SELECT * FROM %s` returned the input rows exactly: %d of %d cases."
      % (env["duckdb_parquet_writer"], env["table"], env["table"], sum(r["result"] == "pass" for r in inputs), len(inputs)))
    w("")
    w("Queries: the %d standard-form files under `sql/`; the two files under `sql/duckdb/` were not run. Each query "
      "string was the file's full text, byte for byte, with its comments and trailing semicolon: the SHA-256 of the "
      "query string equals that of the file in %d of %d queries. `%s` was each query's default database, so the "
      "files' unqualified "
      "`%s` resolves as written. Result reuse was off. The comparison is `ci/run_sql_matrix.py`'s: lower-cased column "
      "names, then the rows as sorted row sets, against `ci/golden/<case>/expected/<file>.csv`. Rows matched counts "
      "the returned rows that are expected rows, as a multiset."
      % (len(sha), verbatim, len(as_written), env["database"], env["table"]))
    w("")
    w("Result: %d of %d files returned exactly the expected rows in all four cases, %d of %d queries."
      % (len(passed_files), len(sha), sum(r["result"] == "pass" for r in as_written), len(as_written)))
    w("")
    w("## Per file, as written")
    w("")
    w(md_table(file_headers, per_file(as_written)))
    w("")
    if fallback:
        w("## Per file, CI statement form (run only where the file as written failed)")
        w("")
        w(md_table(file_headers, per_file(fallback)))
        w("")
    w("## Per query")
    w("")
    w(md_table(["Query", "Case", "Form", "Engine version", "Rows expected", "Rows returned", "Rows matched", "Result",
                "Bytes scanned", "Difference"],
               [[r["sql_file"] if r["kind"] != "input" else "`SELECT * FROM %s`" % env["table"], r["case"], r["kind"],
                 r["engine_version"], r["rows_expected"], r["rows_returned"], r["rows_matched"], r["result"],
                 r["data_scanned_bytes"], r["difference"] or "none"] for r in recs]))
    w("")
    w("## Cost")
    w("")
    w("Athena bills each query's bytes scanned rounded up to the next MB, with a 10 MB minimum, at %s USD per TB "
      "(`infra/prices.json`; MB = 2^20 and TB = 2^40 bytes, as in `bench/aws/athena.py`). The %d queries scanned %d "
      "bytes in all and were billed %d bytes: %s USD. S3 and Glue request charges are not in this figure; they were "
      "not measured." % (price.rstrip("0").rstrip("."), len(recs), sum(int(r["data_scanned_bytes"]) for r in recs),
                         sum(int(r["billed_bytes"]) for r in recs), exact_or_prefix(total_cost)))
    w("")
    w("Files: `athena_sql_check.csv` (one row per query, with its query execution ID), "
      "`athena_sql_check_environment.json`, `athena_sql_check_teardown.txt`.")
    w("")
    with open(os.path.join(a.out, "athena_sql_check.md"), "w") as fh:
        fh.write("\n".join(lines))

    summary = [SECTION_HEADING, "",
               "Run once by `bench/aws/athena_sql_check.py` on %s, outside CI: the %d standard-form `sql/` files, each "
               "file's full text as the query string, on Athena engine version 3 (every query reported %s) against the "
               "four CI golden cases, compared with the golden expected rows as `ci/run_sql_matrix.py` compares them. "
               "%d of %d files returned exactly the expected rows in all four cases. The two `sql/duckdb/` files were "
               "not run. Details, per query, in `athena_sql_check.md`; the workgroup, Glue database and bucket were "
               "deleted afterwards (leftovers: %s)." % (env["started_utc"][:10], len(sha), " and ".join(versions),
                                                         len(passed_files), len(sha), leftovers), "",
               md_table(file_headers[:1] + file_headers[2:4] + file_headers[5:],
                        [row[:1] + row[2:4] + row[5:] for row in per_file(as_written)]), ""]
    replace_section(os.path.join(a.out, "SUMMARY.md"), SECTION_HEADING, "\n".join(summary))
    print("\n".join(lines))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("step", choices=["setup", "run", "teardown", "check-teardown", "report"])
    ap.add_argument("--bucket", default=None, help="required for every step but report; never written to a file")
    ap.add_argument("--out", default="results")
    a = ap.parse_args()
    if a.step != "report" and not a.bucket:
        ap.error("--bucket is required for %s" % a.step)
    {"setup": setup, "run": run, "teardown": teardown, "check-teardown": check_teardown, "report": report}[a.step](a)


if __name__ == "__main__":
    main()
