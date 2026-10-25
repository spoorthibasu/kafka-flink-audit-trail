#!/usr/bin/env python3
"""Proof that the AWS scale run's IAM permissions are gone and the CLI user's own remain.

Calls, as the CLI user, one read action from each statement group the run added (the two run
policies) and from the user's original policy (study/wikimedia/aws/cli-policy.json). Every run
call must be refused; every original call must be allowed. The run's resources no longer exist,
so a refused call shows the missing permission, not a missing resource. Writes the outcomes to
--out with the account ID replaced by a placeholder and exits non-zero on any unexpected result.

  python3 bench/aws/permissions_check.py --out results/aws/permissions_check.txt
"""
import argparse
import datetime
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from awscli import aws, aws_status  # noqa: E402

REGION = "us-east-1"
DENIED = re.compile(r"\((AccessDenied\w*|UnauthorizedOperation)\)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    account = aws("sts", "get-caller-identity")["Account"]
    run_calls = [
        ("run infra: EC2 Terraform describes", ["ec2", "describe-instance-type-offerings", "--max-results", "5"]),
        ("run infra: tagged-resource listing", ["resourcegroupstaggingapi", "get-resources", "--max-items", "1"]),
        ("run infra: run roles", ["iam", "get-role", "--role-name", "audit-trail-bench-runner"]),
        ("run infra: MSK", ["kafka", "get-bootstrap-brokers", "--cluster-arn",
                            "arn:aws:kafka:%s:%s:cluster/audit-trail-bench/x" % (REGION, account)]),
        ("run services: Managed Flink", ["kinesisanalyticsv2", "describe-application", "--application-name",
                                         "audit-trail-bench-a"]),
        ("run services: CloudWatch Logs", ["logs", "describe-log-groups", "--limit", "1"]),
        ("run services: Glue database", ["glue", "get-database", "--name", "audit_trail_bench_aws"]),
        ("run services: Athena workgroup", ["athena", "get-work-group", "--work-group", "audit-trail-bench"]),
        ("run services: CloudWatch metrics", ["cloudwatch", "list-metrics", "--namespace", "AWS/Kafka"]),
        ("run services: Budgets", ["budgets", "describe-budget", "--account-id", account, "--budget-name",
                                   "audit-trail-bench-aws-scale"]),
    ]
    own_calls = [
        ("original: EC2 describe in region", ["ec2", "describe-instances", "--max-items", "1"]),
        ("original: SSM describe", ["ssm", "describe-instance-information", "--max-items", "1"]),
        ("original: Price List", ["pricing", "describe-services", "--max-items", "1"]),
        ("original: bucket names", ["s3api", "list-buckets", "--query", "length(Buckets)"]),
    ]
    lines, unexpected = [], []
    for label, args in run_calls:
        rc, out, err = aws_status(*args, region=REGION)
        m = DENIED.search(err)
        ok = rc != 0 and m is not None
        lines.append("%-45s %s" % (label, ("refused (%s)" % m.group(1)) if ok else "NOT REFUSED"))
        if not ok:
            unexpected.append(label)
    for label, args in own_calls:
        rc, out, err = aws_status(*args, region=REGION)
        lines.append("%-45s %s" % (label, "allowed" if rc == 0 else "REFUSED"))
        if rc != 0:
            unexpected.append(label)
    text = "\n".join(["AWS scale run permission check, %s UTC, as the CLI user, region %s" % (
        datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"), REGION)] + lines
                     + ["", "unexpected: %s" % (", ".join(unexpected) if unexpected else "none")]) + "\n"
    text = text.replace(account, "<account>")
    os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
    with open(a.out, "w") as fh:
        fh.write(text)
    print(text)
    sys.exit(1 if unexpected else 0)


if __name__ == "__main__":
    main()
