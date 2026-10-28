# AWS scale run: teardown plan

Everything the run creates carries the tag `project=audit-trail-bench` and lives in one of three Terraform
roots, created in this order and destroyed in reverse:

| Root | Creates | Destroyed |
|---|---|---|
| `infra/budget` | the AWS Budgets cost budget and its email alerts | last, once Cost Explorer has posted the run's costs |
| `infra/base` | run bucket `audit-trail-bench-run-<random>`, IAM roles `audit-trail-bench-runner` and `audit-trail-bench-flink` (with the permissions boundary), instance profile, security group, runner instance, Glue database `audit_trail_bench_aws`, Athena workgroup `audit-trail-bench`, log group `/aws/kinesis-analytics/audit-trail-bench` | after the Iceberg and Athena measurements and the download of every result |
| `infra/stream` | MSK cluster `audit-trail-bench`, S3 gateway endpoint, Glue (and STS, if needed) interface endpoints, Managed Flink applications `audit-trail-bench-a` and `audit-trail-bench-b` | right after the Kafka and Flink measurements, because it is the most expensive stage per hour |

The Wikimedia archive bucket is not in any Terraform root. The CLI user's run policy denies every Put and
Delete action on it for the whole run.

## Before anything is created

1. Record whether the MSK service-linked role exists: `aws iam get-role --role-name AWSServiceRoleForKafka`.
   If it does not, MSK creates it with the cluster and teardown deletes it.
2. Record the baseline listing of tagged resources:
   `aws resourcegroupstaggingapi get-resources --tag-filters Key=project,Values=audit-trail-bench`.
   The archive bucket is the only expected entry.

## Teardown steps

1. Stream stage. Stop both Flink applications (`aws kinesisanalyticsv2 stop-application --force`), read the
   MSK and Flink CloudWatch metrics into results/aws, then `terraform -chdir=infra/stream destroy`.
2. Base stage. Once results/aws holds every measured file, `terraform -chdir=infra/base destroy`. The bucket
   has `force_destroy`, so the Iceberg tables, Athena results and code archive go with it; the Glue database
   and its tables, the workgroup and the log group are deleted by the same destroy; the runner's root volume
   is deleted on termination and its NVMe instance store does not survive termination.
3. MSK service-linked role, only if step 1 of "Before anything is created" found none:
   `aws iam delete-service-linked-role --role-name AWSServiceRoleForKafka`, then
   `aws iam get-service-linked-role-deletion-status` until it reports SUCCEEDED.
4. Budget. After Cost Explorer shows the run's charges by service (usually 24 to 48 hours after step 2),
   `terraform -chdir=infra/budget destroy`.
5. Proof. `python3 bench/aws/teardown_check.py --out results/aws/teardown_listing.txt --archive-bucket <name>` repeats the tagged
   resource listing and asks each service for each named resource; every answer must be "not found",
   and the tagged listing must hold only the archive bucket. The file replaces the account ID and the
   archive bucket name with placeholders before it is written.
6. Permissions. In the IAM console: detach `audit-trail-bench-run-infra` and `audit-trail-bench-run-services`
   from `audit-trail-bench-cli`, delete both policies, then delete `audit-trail-bench-boundary`. Proof:
   `python3 bench/aws/permissions_check.py --out results/aws/permissions_check.txt` calls one action from
   each group the run policies granted, which must all be refused, and from the user's original policy,
   which must all be allowed. The user `audit-trail-bench-cli` and its original policy
   (study/wikimedia/aws/cli-policy.json) stay.
7. Local state. Delete `infra/*/terraform.tfstate*` and `infra/*/.terraform`; the state files hold the account
   ID and bucket name and are git-ignored throughout.

## What remains after teardown, at no charge

CloudWatch metric history (it ages out on its own and is not billed once no longer emitted), Athena query
history, Cost Explorer data, and the cost allocation tag activation in the Billing console.

## If something goes wrong mid-run

The budget emails at 50, 80 and 100 percent of actual spend and at 100 percent of forecast spend. On an
unexpected alert, destroy `infra/stream` first (1.41 USD per hour idle, 2.92 USD per hour with a Flink
application running, per COST_ESTIMATE.md), then decide about `infra/base` (0.54 USD per hour).
