#!/usr/bin/env python3
"""Drives the AWS scale run from the operator's machine.

The infrastructure comes from infra/ (Terraform; see infra/TEARDOWN.md for the order). This
script runs the measurements against it: the generator and the Kafka and Iceberg tools on
the runner instance (through SSM Run Command), the Managed Flink applications, Athena,
CloudWatch, AWS Budgets and Cost Explorer. Each step writes its raw rows to the run's
results directory (results/aws for the full run, results/aws/smoke for the smoke run) and
records completion in infra/.run/state-<label>.json, so a failed step can be repeated
without repeating the steps before it.

  python3 bench/aws/run.py --config bench/aws/config.aws-smoke.json setup
  python3 bench/aws/run.py --config bench/aws/config.aws-smoke.json generate
  python3 bench/aws/run.py --config bench/aws/config.aws-smoke.json pipeline
  ...  (README "AWS scale run" lists the whole sequence)
"""
import argparse
import csv
import datetime
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import tarfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, ".."))
from awscli import aws, tf_outputs, Runner, ROOT  # noqa: E402
import athena as ath  # noqa: E402

STATE_DIR = os.path.join(ROOT, "infra", ".run")
REPO_ON_RUNNER = "/data/code/repo"


def log(msg):
    print("[%s] %s" % (datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S"), msg), flush=True)


def now_ms():
    return int(time.time() * 1000)


def read_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh))


def append_csv(path, row):
    exists = os.path.exists(path) and os.path.getsize(path) > 0
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(row.keys()))
        if not exists:
            w.writeheader()
        w.writerow(row)


class Run:
    def __init__(self, config_path):
        self.cfg_path = os.path.relpath(os.path.abspath(config_path), ROOT)
        self.cfg = json.load(open(config_path))
        self.a = self.cfg["aws"]
        self.label = self.a["label"]
        self.base = tf_outputs("base")
        self.region = self.base["region"]
        self.runner = Runner(self.base["runner_instance_id"], self.region, log)
        self.out = os.path.join(ROOT, "results", "aws") if self.label == "full" else os.path.join(ROOT, "results", "aws", self.label)
        os.makedirs(self.out, exist_ok=True)
        os.makedirs(STATE_DIR, exist_ok=True)
        self.state_path = os.path.join(STATE_DIR, "state-%s.json" % self.label)
        self.state = json.load(open(self.state_path)) if os.path.exists(self.state_path) else {}
        self.n = self.cfg["n_entities"]
        self.h = self.cfg["h_events"]
        self.steps_dir = "/data/steps/%s" % self.label
        self.work = "/data/work/%s" % self.label
        self.s3_runs = "s3://%s/runs/%s" % (self.base["bucket"], self.label)
        self.athena = ath.Athena(self.region, self.base["athena_workgroup"], self.base["database"], log)

    # ------------------------------------------------------------- helpers

    def save(self):
        with open(self.state_path, "w") as fh:
            json.dump(self.state, fh, indent=2, sort_keys=True)

    def done(self, key):
        return self.state.get("done", {}).get(key)

    def mark(self, key, value=True):
        self.state.setdefault("done", {})[key] = value
        self.save()

    def stream(self):
        return tf_outputs("stream")

    def tools(self, args, java_opts="-Xmx4g", timeout_s=3600):
        return self.runner.run("cd %s && JAVA_OPTS=%r bench/aws/tools.sh %s" % (REPO_ON_RUNNER, java_opts, args),
                               timeout_s=timeout_s)

    def catalog_args(self):
        return "--catalog glue --warehouse %s --database %s" % (self.base["warehouse"], self.base["database"])

    def fetch(self, runner_path, local_name):
        """Copies a file from the runner into the results directory through the run bucket."""
        key = "%s/%s" % (self.s3_runs, os.path.basename(runner_path))
        self.runner.run("aws s3 cp --only-show-errors %s %s" % (runner_path, key), quiet=True)
        dest = os.path.join(self.out, local_name)
        aws("s3", "cp", "--only-show-errors", key, dest, region=self.region, parse=False)
        return dest

    def append_runner_csv(self, runner_path, local_name):
        """Appends the rows of a one-row CSV on the runner to a results CSV."""
        tmp = self.fetch(runner_path, "." + local_name + ".part")
        for row in read_csv(tmp):
            append_csv(os.path.join(self.out, local_name), row)
        os.remove(tmp)

    # --------------------------------------------------------------- setup

    def setup(self):
        """Waits for the runner's first boot, uploads this working tree, builds the tools there."""
        log("waiting for the runner to finish its first boot")
        for _ in range(60):
            try:
                ready = self.runner.run("cat /var/lib/audit-trail-bench-ready", quiet=True).strip()
                if ready:
                    break
            except RuntimeError:
                pass
            time.sleep(20)
        else:
            raise RuntimeError("runner never became ready")
        files = subprocess.run(["git", "ls-files", "-co", "--exclude-standard"], cwd=ROOT, stdout=subprocess.PIPE,
                               text=True, check=True).stdout.split("\n")
        files = [f for f in files if f and os.path.isfile(os.path.join(ROOT, f))
                 and not f.startswith(("results-smoke", "infra/.run/", "target/"))]
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tar:
            for f in sorted(files):
                tar.add(os.path.join(ROOT, f), arcname=f)
        data = buf.getvalue()
        digest = hashlib.sha256(data).hexdigest()
        tmp = os.path.join(STATE_DIR, "repo.tar.gz")
        open(tmp, "wb").write(data)
        aws("s3", "cp", "--only-show-errors", tmp, "s3://%s/code/repo-%s.tar.gz" % (self.base["bucket"], digest),
            region=self.region, parse=False)
        self.runner.run(
            "rm -rf %s && mkdir -p %s && aws s3 cp --only-show-errors s3://%s/code/repo-%s.tar.gz /data/code/repo.tar.gz "
            "&& tar -xzf /data/code/repo.tar.gz -C %s && cd %s && python3.11 -m pip install --user -q -r bench/requirements.txt "
            "&& (cd bench/aws/flink-app && mvn -q -B compile dependency:build-classpath -Dmdep.includeScope=compile "
            "-Dmdep.outputFile=target/cp.txt) && java -version 2>&1 | head -1 && python3.11 --version && "
            "python3.11 -c 'import fastavro, duckdb, cramjam; print(fastavro.__version__)' && nproc && free -g | head -2 && df -h /data | tail -1"
            % (REPO_ON_RUNNER, REPO_ON_RUNNER, self.base["bucket"], digest, REPO_ON_RUNNER, REPO_ON_RUNNER), timeout_s=3600)
        self.state["code_sha256"] = digest
        self.mark("setup", digest)
        log("runner set up with code archive %s" % digest)

    # ------------------------------------------------------------ generate

    def generate(self, wait=True):
        if not self.done("generate_started"):
            self.runner.start_job("generate-" + self.label,
                                  "mkdir -p %s && python3.11 bench/aws/generate_steps.py --config %s --out %s"
                                  % (self.steps_dir, self.cfg_path, self.steps_dir))
            self.mark("generate_started", now_ms())
        if wait:
            self.runner.wait_job("generate-" + self.label, poll_s=120 if self.label == "full" else 15, timeout_s=10 * 3600)
            self.fetch("%s/manifest.json" % self.steps_dir, "generation_manifest.json")
            self.mark("generate")

    # --------------------------------------------------------------- kafka

    def topic(self, design, compression):
        return "%s-%s-%s" % (self.label, design.lower(), compression)

    def kafka(self, design, compression):
        key = "kafka_%s_%s" % (design, compression)
        if self.done(key):
            log("%s already measured" % key)
            return
        st = self.stream()
        boot = st["bootstrap_brokers"]
        topic = self.topic(design, compression)
        entities = self.n if compression == "zstd" else self.a["none_entities"]
        self.tools("topic-create --bootstrap %s --topic %s --partitions %d --replication-factor %d"
                   % (boot, topic, self.a["partitions"], self.a["replication_factor"]))
        self.runner.run("mkdir -p %s" % self.work, quiet=True)
        prod = "%s/producer-%s.csv" % (self.work, topic)
        lb = "%s/logbytes-%s.csv" % (self.work, topic)
        self.runner.run("rm -f %s %s" % (prod, lb), quiet=True)
        # One-minute CloudWatch datapoints: keep two whole minutes without producing between
        # runs, so no minute of one run's window holds another run's traffic.
        quiet_until = self.state.get("last_produce_end_ms", 0) + 150_000
        if now_ms() < quiet_until:
            log("waiting %d s so this produce window shares no minute with the previous one" % ((quiet_until - now_ms()) // 1000))
            time.sleep((quiet_until - now_ms()) / 1000)
        log("producing %d events to %s" % (entities * self.h, topic))
        self.tools("produce --bootstrap %s --topic %s --compression %s --steps-dir %s/%s --steps %d --entities %d "
                   "--label %s --design %s --out %s" % (boot, topic, compression, self.steps_dir, design, self.h, entities,
                                                         self.label, design, prod),
                   java_opts="-Xmx16g", timeout_s=6 * 3600)
        self.tools("log-bytes --bootstrap %s --topic %s --replication-factor %d --label %s --out %s"
                   % (boot, topic, self.a["replication_factor"], self.label, lb))
        self.append_runner_csv(prod, "producer_runs.csv")
        self.append_runner_csv(lb, "topic_log_bytes.csv")
        self.state["last_produce_end_ms"] = now_ms()
        self.mark(key)

    def topic_delete(self, design, compression):
        key = "deleted_%s_%s" % (design, compression)
        if self.done(key):
            return
        self.tools("topic-delete --bootstrap %s --topic %s" % (self.stream()["bootstrap_brokers"], self.topic(design, compression)))
        self.mark(key)

    # --------------------------------------------------------------- flink

    def iceberg_metadata(self, table):
        t = aws("glue", "get-table", "--database-name", self.base["database"], "--name", table, region=self.region)
        loc = t["Table"]["Parameters"]["metadata_location"]
        body = aws("s3", "cp", "--only-show-errors", loc, "-", region=self.region, parse=False)
        return json.loads(body)

    def app_status(self, app):
        d = aws("kinesisanalyticsv2", "describe-application", "--application-name", app, region=self.region)
        return d["ApplicationDetail"]

    def flink(self, design):
        key = "flink_%s" % design
        if self.done(key):
            log("%s already measured" % key)
            return
        st = self.stream()
        app = st["flink_applications"][design]
        table = design.lower()
        self.tools("tables-create %s --tables %s=%s" % (self.catalog_args(), table, design))
        detail = self.app_status(app)
        if detail["ApplicationStatus"] != "READY":
            raise RuntimeError("%s is %s, not READY" % (app, detail["ApplicationStatus"]))
        group = "%s-%s-%d" % (self.label, table, now_ms())
        props = {"bootstrap": st["bootstrap_brokers"], "topic": self.topic(design, "zstd"), "group.id": group,
                 "design": design, "catalog": "glue", "warehouse": self.base["warehouse"],
                 "database": self.base["database"], "table": table}
        update = {"EnvironmentPropertyUpdates": {"PropertyGroups": [{"PropertyGroupId": "job", "PropertyMap": props}]}}
        aws("kinesisanalyticsv2", "update-application", "--application-name", app,
            "--current-application-version-id", detail["ApplicationVersionId"],
            "--application-configuration-update", json.dumps(update), region=self.region)
        self.wait_app(app, "READY")
        expected = self.n * self.h
        start_ms = now_ms()
        aws("kinesisanalyticsv2", "start-application", "--application-name", app, "--run-configuration",
            json.dumps({"ApplicationRestoreConfiguration": {"ApplicationRestoreType": "SKIP_RESTORE_FROM_SNAPSHOT"},
                        "FlinkRunConfiguration": {"AllowNonRestoredState": True}}), region=self.region)
        log("started %s for %s (%d events expected)" % (app, props["topic"], expected))
        self.wait_app(app, "RUNNING", timeout_s=1800)
        running_ms = now_ms()
        deadline = time.time() + 6 * 3600
        total = 0
        while True:
            time.sleep(30)
            try:
                md = self.iceberg_metadata(table)
            except RuntimeError as e:
                log("  metadata not readable yet: %s" % str(e)[:120])
                continue
            snaps = md.get("snapshots", [])
            total = int(snaps[-1]["summary"].get("total-records", 0)) if snaps else 0
            log("  %s: %d snapshots, %d of %d records committed" % (table, len(snaps), total, expected))
            if total >= expected:
                break
            if self.app_status(app)["ApplicationStatus"] != "RUNNING":
                raise RuntimeError("%s stopped before committing every event" % app)
            if time.time() > deadline:
                raise RuntimeError("%s did not commit every event in time" % app)
        stop_ms = now_ms()
        aws("kinesisanalyticsv2", "stop-application", "--application-name", app, "--force", region=self.region)
        self.wait_app(app, "READY", timeout_s=1800)
        stopped_ms = now_ms()
        if total != expected:
            raise RuntimeError("table %s holds %d records, expected %d" % (table, total, expected))
        # The bucket name stays out of the results: S3 paths keep their shape with a placeholder.
        with open(os.path.join(self.out, "iceberg_metadata_%s_after_flink.json" % table), "w") as fh:
            fh.write(json.dumps(md, indent=1).replace(self.base["bucket"], "<run-bucket>"))
        for s in md["snapshots"]:
            sm = s["summary"]
            append_csv(os.path.join(self.out, "iceberg_snapshots.csv"), {
                "label": self.label, "design": design, "table": table, "snapshot_id": s["snapshot-id"],
                "sequence_number": s.get("sequence-number", ""), "timestamp_ms": s["timestamp-ms"],
                "operation": sm.get("operation", ""), "added_records": sm.get("added-records", ""),
                "total_records": sm.get("total-records", ""), "added_data_files": sm.get("added-data-files", ""),
                "total_data_files": sm.get("total-data-files", ""), "added_files_size": sm.get("added-files-size", ""),
                "total_files_size": sm.get("total-files-size", ""),
                "flink_max_committed_checkpoint_id": sm.get("flink.max-committed-checkpoint-id", ""),
                "flink_job_id": sm.get("flink.job-id", "")})
        self.flink_logs(design, app, start_ms, stopped_ms, md)
        append_csv(os.path.join(self.out, "flink_runs.csv"), {
            "label": self.label, "design": design, "application": app, "topic": props["topic"], "events": expected,
            "parallelism": self.a["flink_parallelism"], "parallelism_per_kpu": self.a["flink_parallelism_per_kpu"],
            "checkpoint_interval_ms": self.a["checkpoint_interval_ms"], "min_pause_ms": self.a["min_pause_ms"],
            "start_call_epoch_ms": start_ms, "application_running_seen_epoch_ms": running_ms,
            "all_records_seen_epoch_ms": stop_ms, "stopped_epoch_ms": stopped_ms})
        self.mark(key)

    def wait_app(self, app, status, timeout_s=900):
        deadline = time.time() + timeout_s
        while True:
            s = self.app_status(app)["ApplicationStatus"]
            if s == status:
                return
            if time.time() > deadline:
                raise RuntimeError("%s is %s, waited for %s" % (app, s, status))
            time.sleep(10)

    def flink_logs(self, design, app, start_ms, end_ms, md):
        """Checkpoint completions and job state changes from the JobManager log lines Managed Flink
        writes to CloudWatch Logs, for the job that committed the table's snapshots."""
        job_ids = {s["summary"].get("flink.job-id") for s in md["snapshots"]} - {None}
        group = self.base["flink_log_group"]

        def events(pattern):
            r = aws("logs", "filter-log-events", "--log-group-name", group, "--start-time", start_ms,
                    "--end-time", end_ms + 60_000, "--filter-pattern", pattern, region=self.region)
            return r.get("events", [])

        def message(e):
            try:
                return json.loads(e["message"]).get("message", e["message"])
            except (ValueError, AttributeError):
                return e["message"]

        done_re = re.compile(r"Completed checkpoint (\d+) for job ([0-9a-f]{32}) \((\d+) bytes, "
                             r"checkpointDuration=(\d+) ms, finalizationTime=(\d+) ms\)")
        n = 0
        for e in events('"Completed checkpoint"'):
            m = done_re.search(message(e))
            if not m or (job_ids and m.group(2) not in job_ids):
                continue
            n += 1
            append_csv(os.path.join(self.out, "flink_checkpoints.csv"), {
                "label": self.label, "design": design, "job_id": m.group(2), "checkpoint_id": int(m.group(1)),
                "checkpointed_bytes": int(m.group(3)), "checkpoint_duration_ms": int(m.group(4)),
                "finalization_ms": int(m.group(5)), "log_timestamp_ms": e["timestamp"]})
        state_re = re.compile(r"Job (.*) \(([0-9a-f]{32})\) switched from state (\w+) to (\w+)")
        for e in events('"switched from state"'):
            m = state_re.search(message(e))
            if not m or (job_ids and m.group(2) not in job_ids):
                continue
            append_csv(os.path.join(self.out, "flink_job_states.csv"), {
                "label": self.label, "design": design, "job_id": m.group(2), "from_state": m.group(3),
                "to_state": m.group(4), "log_timestamp_ms": e["timestamp"]})
        log("  %d completed checkpoints found in the Flink log for %s" % (n, app))
        if n == 0:
            raise RuntimeError("no checkpoint lines in %s for %s; check the application's log level" % (group, app))

    # ------------------------------------------------------------- iceberg

    def load_c(self):
        if self.done("load_c"):
            return
        self.tools("tables-create %s --tables c=C" % self.catalog_args())
        self.tools("load-c %s --table c --file %s/C/current.bin --entities %d --parallelism %d"
                   % (self.catalog_args(), self.steps_dir, self.n, self.a["compaction_parallelism"]), java_opts="-Xmx24g")
        self.mark("load_c")

    def iceberg_stats(self, state):
        key = "stats_%s" % state
        if self.done(key):
            return
        for table in ("a", "b", "c"):
            out = "%s/stats-%s-%s.csv" % (self.work, table, state)
            self.runner.run("mkdir -p %s && rm -f %s" % (self.work, out), quiet=True)
            self.tools("stats %s --table %s --state %s --label %s --out %s" % (self.catalog_args(), table, state, self.label, out))
            self.append_runner_csv(out, "iceberg_stats.csv")
        self.history(state)
        self.mark(key)

    def history(self, state):
        for table in ("a", "b", "c"):
            q = self.athena.run(ath.history_sql(table))
            r = self.athena.rows(q["QueryExecutionId"])[0]
            row = {"label": self.label, "table": table, "state": state, "row_count": r["row_count"],
                   "distinct_customer_sequence": r["distinct_customer_sequence"], "history_checksum": r["history_checksum"]}
            row.update(ath.stats_row(q))
            append_csv(os.path.join(self.out, "iceberg_history.csv"), row)
            log("  history %s %s: %s rows, checksum %s" % (table, state, r["row_count"], r["history_checksum"]))

    def maintain(self, action):
        key = "maintain_%s_%d" % (action, len([k for k in self.state.get("done", {}) if k.startswith("maintain_%s" % action)]))
        for table in ("a", "b", "c"):
            t0 = now_ms()
            self.tools("%s %s --table %s%s" % (action, self.catalog_args(), table,
                                              " --parallelism %d" % self.a["compaction_parallelism"] if action == "compact" else ""),
                       java_opts="-Xmx24g", timeout_s=6 * 3600)
            append_csv(os.path.join(self.out, "iceberg_maintenance.csv"), {
                "label": self.label, "table": table, "action": action, "start_epoch_ms": t0, "end_epoch_ms": now_ms()})
        self.mark(key)

    def drop_tables(self):
        for table in ("a", "b", "c"):
            self.tools("drop-table %s --table %s" % (self.catalog_args(), table))

    # -------------------------------------------------------------- athena

    def equivalence(self):
        if self.done("equivalence"):
            return
        attrs, entity, qs = ath.queries(self.cfg)
        sql = {(qid, design): s for qid, _, _, design, s, _ in qs if s is not None}
        for qid, left, right in ath.EQUIVALENCE_PAIRS:
            cols = ath.columns(qid, attrs)
            q = self.athena.run(ath.equivalence_sql(sql[(qid, left)], sql[(qid, right)], cols))
            r = self.athena.rows(q["QueryExecutionId"])[0]
            equal = r["rows_left"] == r["rows_right"] and r["differing_rows"] == "0"
            row = {"label": self.label, "query": qid, "designs": "%s vs %s" % (left, right), "columns": len(cols),
                   "rows_left": r["rows_left"], "rows_right": r["rows_right"], "differing_rows": r["differing_rows"],
                   "equal": "yes" if equal else "no"}
            row.update(ath.stats_row(q))
            append_csv(os.path.join(self.out, "equivalence.csv"), row)
            log("  %s %s vs %s: %s and %s rows, %s differing: %s" % (qid, left, right, r["rows_left"], r["rows_right"],
                                                                      r["differing_rows"], row["equal"]))
            if not equal:
                raise RuntimeError("designs %s and %s disagree on %s" % (left, right, qid))
        self.mark("equivalence")

    def athena_timing(self):
        if self.done("athena_timing"):
            return
        attrs, entity, qs = ath.queries(self.cfg)
        runs, warm = self.a["athena_runs"], self.a["athena_warmup_runs"]
        for qid, desc, scope, design, sql, reason in qs:
            if sql is None:
                continue
            for i in range(warm + runs):
                q = self.athena.run(sql)
                row = {"label": self.label, "query": qid, "design": design, "scope": scope,
                       "entity": entity if scope == "single" else "", "run": i - warm + 1 if i >= warm else 0,
                       "warmup": "yes" if i < warm else "no"}
                row.update(ath.stats_row(q))
                append_csv(os.path.join(self.out, "athena_runs.csv"), row)
            log("  %s %s: %d runs" % (qid, design, warm + runs))
        self.mark("athena_timing")

    # ---------------------------------------------------------- cloudwatch

    def msk_metrics(self):
        """Per-broker MSK metrics, one-minute averages, over each produce window: from the
        minute the producer started to the minute describeLogDirs found every replica settled."""
        if self.done("msk_metrics"):
            return
        cluster = self.stream()["msk_cluster_name"]
        lb = {r["topic"]: r for r in read_csv(os.path.join(self.out, "topic_log_bytes.csv"))}
        for p in read_csv(os.path.join(self.out, "producer_runs.csv")):
            start = int(p["produce_start_epoch_ms"]) // 60000 * 60000
            end = (int(lb[p["topic"]]["measured_epoch_ms"]) // 60000 + 1) * 60000
            queries = []
            for i, metric in enumerate(("ReplicationBytesInPerSec", "ReplicationBytesOutPerSec", "BytesInPerSec", "BytesOutPerSec")):
                for broker in ("1", "2", "3"):
                    queries.append({"Id": "m%d_%s" % (i, broker), "Label": "%s|%s" % (metric, broker), "ReturnData": True,
                                    "MetricStat": {"Metric": {"Namespace": "AWS/Kafka", "MetricName": metric, "Dimensions": [
                                        {"Name": "Cluster Name", "Value": cluster}, {"Name": "Broker ID", "Value": broker}]},
                                        "Period": 60, "Stat": "Average"}})
            r = aws("cloudwatch", "get-metric-data", "--metric-data-queries", json.dumps(queries),
                    "--start-time", start // 1000, "--end-time", end // 1000, region=self.region)
            for res in r["MetricDataResults"]:
                metric, broker = res["Label"].split("|")
                for ts, v in zip(res["Timestamps"], res["Values"]):
                    append_csv(os.path.join(self.out, "msk_metrics.csv"), {
                        "label": self.label, "topic": p["topic"], "metric": metric, "broker_id": broker,
                        "minute_utc": ts, "average_per_second": repr(v), "window_start_epoch_ms": start,
                        "window_end_epoch_ms": end})
        self.mark("msk_metrics")

    # -------------------------------------------------------------- spend

    def spend(self):
        b = aws("budgets", "describe-budget", "--account-id", aws("sts", "get-caller-identity")["Account"],
                "--budget-name", "audit-trail-bench-aws-scale")["Budget"]
        cs = b.get("CalculatedSpend", {})
        actual = cs.get("ActualSpend", {}).get("Amount")
        forecast = cs.get("ForecastedSpend", {}).get("Amount")
        log("budget %s USD; actual %s USD; forecast %s USD (as AWS Budgets last calculated)"
            % (b["BudgetLimit"]["Amount"], actual, forecast))
        # The operator's pause rule for this run: stop above 100 USD actual or 150 USD forecast.
        if (actual and float(actual) > 100) or (forecast and float(forecast) > 150):
            raise RuntimeError("pause: actual %s USD or forecast %s USD is past the pause threshold" % (actual, forecast))
        return actual, forecast

    def pipeline(self):
        self.spend()
        for design in self.a["designs"]:
            self.kafka(design, "zstd")
            self.flink(design)
            self.topic_delete(design, "zstd")
            if "none" in self.a["compressions"]:
                self.kafka(design, "none")
                self.topic_delete(design, "none")
            self.spend()
        self.msk_metrics()

    def environment(self):
        st = self.stream() if self.label else {}
        env = {"label": self.label, "config_file": self.cfg_path, "config": self.cfg, "region": self.region,
               "code_sha256": self.state.get("code_sha256"), "runner_instance_type": self.base["runner_instance_type"],
               "kafka_version": st.get("kafka_version"), "msk_broker_type": "kafka.m7g.large", "msk_brokers": 3,
               "flink_runtime": "FLINK-1_20", "flink_jar_key_suffix": st.get("flink_jar_key", "").split("/")[-1],
               "athena_engine": "Athena engine version 3", "written_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()}
        env["runner"] = self.runner.run("cd %s && java -version 2>&1 | head -1; python3.11 --version; nproc; "
                                        "grep MemTotal /proc/meminfo; lscpu | grep 'Model name'" % REPO_ON_RUNNER, quiet=True).strip().splitlines()
        with open(os.path.join(self.out, "environment.json"), "w") as fh:
            json.dump(env, fh, indent=2)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("step", choices=["setup", "generate", "generate-start", "kafka", "flink", "topic-delete", "pipeline",
                                     "msk-metrics", "load-c", "equivalence", "stats", "expire", "compact", "athena-timing",
                                     "drop-tables", "spend", "environment"])
    ap.add_argument("args", nargs="*")
    a = ap.parse_args()
    r = Run(a.config)
    step = a.step
    if step == "setup":
        r.setup()
    elif step == "generate":
        r.generate()
    elif step == "generate-start":
        r.generate(wait=False)
    elif step == "kafka":
        r.kafka(a.args[0], a.args[1])
    elif step == "flink":
        r.flink(a.args[0])
    elif step == "topic-delete":
        r.topic_delete(a.args[0], a.args[1])
    elif step == "pipeline":
        r.pipeline()
    elif step == "msk-metrics":
        r.msk_metrics()
    elif step == "load-c":
        r.load_c()
    elif step == "equivalence":
        r.equivalence()
    elif step == "stats":
        r.iceberg_stats(a.args[0])
    elif step in ("expire", "compact"):
        r.maintain(step)
    elif step == "athena-timing":
        r.athena_timing()
    elif step == "drop-tables":
        r.drop_tables()
    elif step == "spend":
        r.spend()
    elif step == "environment":
        r.environment()


if __name__ == "__main__":
    main()
