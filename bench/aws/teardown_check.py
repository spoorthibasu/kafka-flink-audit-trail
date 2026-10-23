#!/usr/bin/env python3
"""Proof that the AWS scale run left nothing behind but the Wikimedia archive bucket.

Lists every resource tagged project=audit-trail-bench in the region (Resource Groups
Tagging API), then asks each service for each resource the run created by name. Writes the
answers to --out with the account ID and the archive bucket name replaced by placeholders,
and exits non-zero if anything other than the archive bucket (and the CLI user's own
policy, which predates the run) is still there.

  python3 bench/aws/teardown_check.py --out results/aws/teardown_listing.txt --archive-bucket <name>
"""
import argparse
import datetime
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from awscli import aws, aws_status  # noqa: E402

REGION = "us-east-1"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--archive-bucket", required=True, help="the bucket that must remain; never written to the file")
    ap.add_argument("--msk-cluster-arn", default="", help="the run's cluster ARN, from infra/.run or the stream state")
    a = ap.parse_args()
    account = aws("sts", "get-caller-identity")["Account"]
    lines, leftovers = [], []

    def note(check, result):
        lines.append("%-60s %s" % (check, result))

    tagged = aws("resourcegroupstaggingapi", "get-resources", "--tag-filters", "Key=project,Values=audit-trail-bench",
                 region=REGION)["ResourceTagMappingList"]
    allowed = {"arn:aws:s3:::%s" % a.archive_bucket, "arn:aws:iam::%s:policy/audit-trail-bench-cli" % account}
    note("tag:GetResources project=audit-trail-bench", "%d resources" % len(tagged))
    for t in tagged:
        arn = t["ResourceARN"]
        note("  tagged", arn)
        if arn not in allowed:
            leftovers.append(arn)

    not_found = ("NotFound", "NoSuchEntity", "EntityNotFound", "ResourceNotFound", "not found", "doesn't exist",
                 "does not exist")

    def absent(check, *args, region=REGION):
        """Not found only on a not-found error; success or any other error counts as a leftover."""
        rc, out, err = aws_status(*args, region=region)
        if rc == 0:
            note(check, "STILL PRESENT")
            leftovers.append(check)
        elif any(s in err for s in not_found):
            note(check, "not found (%s)" % err.strip().splitlines()[-1][:120])
        else:
            note(check, "ERROR: %s" % err.strip()[:200])
            leftovers.append(check)

    def empty(check, value):
        note(check, "none" if not value else "STILL PRESENT: %s" % value)
        if value:
            leftovers.append(check)

    empty("ec2 instances tagged project (not terminated)", aws(
        "ec2", "describe-instances", "--filters", "Name=tag:project,Values=audit-trail-bench",
        "Name=instance-state-name,Values=pending,running,stopping,stopped,shutting-down",
        "--query", "Reservations[].Instances[].InstanceId", region=REGION))
    empty("ec2 volumes tagged project", aws("ec2", "describe-volumes", "--filters", "Name=tag:project,Values=audit-trail-bench",
                                            "--query", "Volumes[].VolumeId", region=REGION))
    empty("security groups tagged project", aws("ec2", "describe-security-groups", "--filters",
                                                "Name=tag:project,Values=audit-trail-bench", "--query", "SecurityGroups[].GroupId",
                                                region=REGION))
    empty("vpc endpoints tagged project", aws("ec2", "describe-vpc-endpoints", "--filters",
                                              "Name=tag:project,Values=audit-trail-bench",
                                              "--query", "VpcEndpoints[?State!='deleted'].VpcEndpointId", region=REGION))
    if a.msk_cluster_arn:
        absent("msk cluster audit-trail-bench", "kafka", "describe-cluster-v2", "--cluster-arn", a.msk_cluster_arn)
    for app in ("audit-trail-bench-a", "audit-trail-bench-b"):
        absent("managed flink application %s" % app, "kinesisanalyticsv2", "describe-application", "--application-name", app)
    absent("glue database audit_trail_bench_aws", "glue", "get-database", "--name", "audit_trail_bench_aws")
    absent("athena workgroup audit-trail-bench", "athena", "get-work-group", "--work-group", "audit-trail-bench")
    empty("log groups /aws/kinesis-analytics/audit-trail-bench", aws(
        "logs", "describe-log-groups", "--log-group-name-prefix", "/aws/kinesis-analytics/audit-trail-bench",
        "--query", "logGroups[].logGroupName", region=REGION))
    buckets = aws("s3api", "list-buckets", "--query", "Buckets[].Name")
    run_buckets = [x for x in buckets if x.startswith("audit-trail-bench") and x != a.archive_bucket]
    empty("s3 buckets audit-trail-bench-* other than the archive", run_buckets)
    note("s3 archive bucket", "present" if a.archive_bucket in buckets else "MISSING")
    if a.archive_bucket not in buckets:
        leftovers.append("archive bucket missing")
    for role in ("audit-trail-bench-runner", "audit-trail-bench-flink", "AWSServiceRoleForKafka"):
        absent("iam role %s" % role, "iam", "get-role", "--role-name", role)
    absent("iam instance profile audit-trail-bench-runner", "iam", "get-instance-profile", "--instance-profile-name",
           "audit-trail-bench-runner")
    absent("budget audit-trail-bench-aws-scale", "budgets", "describe-budget", "--account-id", account,
           "--budget-name", "audit-trail-bench-aws-scale")

    text = "\n".join(["AWS scale run teardown check, %s UTC, region %s" % (
        datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"), REGION)] + lines
                     + ["", "leftovers: %s" % (", ".join(leftovers) if leftovers else "none")]) + "\n"
    text = text.replace(account, "<account>").replace(a.archive_bucket, "<archive bucket>")
    os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
    with open(a.out, "w") as fh:
        fh.write(text)
    print(text)
    sys.exit(1 if leftovers else 0)


if __name__ == "__main__":
    main()
