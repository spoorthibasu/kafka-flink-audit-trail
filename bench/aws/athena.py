"""Athena queries for the AWS scale run.

The questions are bench/benchmark.py build_queries() for the full model, unchanged: Q1
one-hop previous value of email for one entity, Q2 (q2_three_changes_back_single) the value
of email three events back for one entity, Q3 current state of every attribute for every
entity, Q4 current and previous email for every entity. The tables a, b and c live in the
run's Glue database, which is each query's default database, so the SQL reads exactly as
it does on DuckDB.

The equivalence check compares, as multisets, the pairs bench/benchmark.py
check_equivalence() compares for the full model: Q1 A vs B, Q3 A vs B, Q3 B vs C, Q4 A vs
B. One query per pair counts, for every distinct row, how often each side returns it; the
pair is equal when no row has different counts. Each side is scanned once.

Billed cost per query follows Athena's pricing rule: the bytes Athena reports scanned,
rounded up to the next megabyte (2^20 bytes), at least 10 MB, at the price per terabyte
(2^40 bytes) in infra/prices.json.
"""
import json
import math
import os
import sys
import time
from fractions import Fraction

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import benchmark as b  # noqa: E402
from awscli import aws, ROOT  # noqa: E402

MB = 2 ** 20
TB = 2 ** 40
EQUIVALENCE_PAIRS = [("q1_one_hop_previous_single", "A", "B"), ("q3_current_state_all", "A", "B"),
                     ("q3_current_state_all", "B", "C"), ("q4_previous_value_all", "A", "B")]


def price_per_tb():
    with open(os.path.join(ROOT, "infra", "prices.json")) as fh:
        return Fraction(json.load(fh)["prices"]["athena_tb_scanned"]["usd"])


def billed_bytes(scanned):
    return max(math.ceil(scanned / MB), 10) * MB


def cost_usd(scanned):
    return Fraction(billed_bytes(scanned), TB) * price_per_tb()


def queries(cfg):
    attrs = b.attributes(cfg["k_attributes"])
    entity = "C%07d" % (cfg["n_entities"] // 2)
    return attrs, entity, b.build_queries(attrs, "full", entity)


def columns(qid, attrs):
    names = [a for a, _ in attrs]
    x = names[0]
    return {"q1_one_hop_previous_single": ["event_timestamp", "event_sequence", x, "previous_" + x],
            "q2_three_changes_back_single": ["event_timestamp", "event_sequence", x, x + "_three_back"],
            "q3_current_state_all": ["customer_id"] + names,
            "q4_previous_value_all": ["customer_id", x, "previous_" + x]}[qid]


def equivalence_sql(sql_left, sql_right, cols):
    group = ", ".join('"%s"' % c for c in cols)
    return ("WITH u AS (SELECT 1 AS in_left, 0 AS in_right, l.* FROM (%s) l "
            "UNION ALL SELECT 0, 1, r.* FROM (%s) r), "
            "g AS (SELECT sum(in_left) AS n_left, sum(in_right) AS n_right FROM u GROUP BY %s) "
            "SELECT sum(n_left) AS rows_left, sum(n_right) AS rows_right, count_if(n_left <> n_right) AS differing_rows FROM g"
            % (sql_left, sql_right, group))


def history_sql(table):
    """Row count, distinct (customer_id, event_sequence), and an order-insensitive checksum over
    LAG(email) and the previous known email at every row: the history check of
    PipelineBenchmark.digest, in Athena's dialect."""
    w = "PARTITION BY customer_id ORDER BY event_timestamp, event_sequence"
    return ("SELECT count(*) AS row_count, "
            "count(DISTINCT concat(customer_id, ':', CAST(event_sequence AS varchar))) AS distinct_customer_sequence, "
            "to_hex(checksum(ROW(customer_id, event_sequence, prev_email, prev_known_email))) AS history_checksum "
            "FROM (SELECT customer_id, event_sequence, LAG(email) OVER (%s) AS prev_email, "
            "last_value(email) IGNORE NULLS OVER (%s ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_known_email "
            "FROM %s) t" % (w, w, table))


class Athena:
    def __init__(self, region, workgroup, database, log):
        self.region, self.workgroup, self.database, self.log = region, workgroup, database, log

    def run(self, sql, timeout_s=3600):
        """Runs one query with result reuse off; returns the final GetQueryExecution record."""
        qid = aws("athena", "start-query-execution", "--query-string", sql, "--work-group", self.workgroup,
                  "--query-execution-context", "Database=%s,Catalog=AwsDataCatalog" % self.database,
                  "--result-reuse-configuration", "ResultReuseByAgeConfiguration={Enabled=false}",
                  region=self.region)["QueryExecutionId"]
        deadline = time.time() + timeout_s
        while True:
            time.sleep(1)
            q = aws("athena", "get-query-execution", "--query-execution-id", qid, region=self.region)["QueryExecution"]
            state = q["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            if time.time() > deadline:
                aws("athena", "stop-query-execution", "--query-execution-id", qid, region=self.region, check=False)
                raise RuntimeError("Athena query %s timed out" % qid)
        if state != "SUCCEEDED":
            raise RuntimeError("Athena query %s %s: %s\n%s" % (qid, state, q["Status"].get("StateChangeReason"), sql[:500]))
        return q

    def rows(self, qid):
        res = aws("athena", "get-query-results", "--query-execution-id", qid, region=self.region)
        rs = res["ResultSet"]["Rows"]
        head = [c.get("VarCharValue") for c in rs[0]["Data"]]
        return [dict(zip(head, [c.get("VarCharValue") for c in r["Data"]])) for r in rs[1:]]


def stats_row(q):
    s = q["Statistics"]
    return {"query_execution_id": q["QueryExecutionId"],
            "engine_version": q.get("EngineVersion", {}).get("EffectiveEngineVersion", ""),
            "data_scanned_bytes": s["DataScannedInBytes"],
            "total_execution_ms": s["TotalExecutionTimeInMillis"],
            "engine_execution_ms": s["EngineExecutionTimeInMillis"],
            "queue_ms": s.get("QueryQueueTimeInMillis", ""),
            "planning_ms": s.get("QueryPlanningTimeInMillis", ""),
            "service_preprocessing_ms": s.get("ServicePreProcessingTimeInMillis", ""),
            "service_processing_ms": s.get("ServiceProcessingTimeInMillis", ""),
            "submitted_utc": q["Status"]["SubmissionDateTime"],
            "completed_utc": q["Status"]["CompletionDateTime"]}
