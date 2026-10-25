# AWS scale run: cost estimate

Written by `infra/estimate.py` before any resource exists. This is an estimate, not a measurement: the run reports what Cost Explorer bills. Prices are on-demand us-east-1 prices from the AWS Price List Query API, fetched 2026-10-23 (`infra/prices.json` keeps the SKU, usage type and effective date of each). Dollar amounts are rounded up to the cent; GB are decimal and months are 720 hours, which both make the estimate err high.

Two plans differ only in the producer compression `none` measurement: 10,000,000 events per design (the first 100,000 entities) or the full 100,000,000.

## Plan: compression none on a 10,000,000 event subset

| Service | Item | Quantity | Unit | Price (USD) | Price key | Cost (USD) |
|---|---|---|---|---|---|---|
| Amazon EC2 | runner m8id.2xlarge (generator, producer, Iceberg maintenance) | 36 | instance-hours | 0.52208 | `ec2_m8id_2xlarge_hour` | 18.80 |
| Amazon EC2 | runner root volume, gp3 50 GB | 2.500 | GB-months | 0.08 | `ebs_gp3_gb_month` | 0.20 |
| Amazon VPC | runner public IPv4 address | 36 | hours | 0.005 | `public_ipv4_hour` | 0.18 |
| Amazon MSK | 3 x kafka.m7g.large brokers | 48 | broker-hours | 0.204 | `msk_m7g_large_broker_hour` | 9.80 |
| Amazon MSK | broker storage, 400 GB per broker | 26.667 | GB-months | 0.1 | `msk_storage_gb_month` | 2.67 |
| Managed Service for Apache Flink | parallelism 12 plus 1 orchestration KPU, while an application runs | 78 | KPU-hours | 0.11 | `msf_kpu_hour` | 8.58 |
| Managed Service for Apache Flink | running application storage, 50 GB per KPU | 5 | GB-months | 0.1 | `msf_running_storage_gb_month` | 0.50 |
| Amazon VPC | interface endpoints (Glue, and STS if the Flink app needs it), one AZ each | 32 | endpoint-hours | 0.01 | `vpc_endpoint_hour` | 0.32 |
| Amazon VPC | interface endpoint data processed | 1 | GB | 0.01 | `vpc_endpoint_gb` | 0.01 |
| Data transfer | cross-AZ client traffic: produce zstd and none topics, Flink reads zstd topics | 408.520 | GB (billed both ways) | 0.01 | `data_transfer_regional_gb` | 4.09 |
| Amazon S3 | Iceberg data and metadata at peak (before expiration removes replaced files) plus Athena results | 31.981 | GB-months | 0.023 | `s3_standard_gb_month` | 0.74 |
| Amazon S3 | PUT, COPY, POST, LIST requests | 200,000 | requests | 0.000005 | `s3_tier1_request` | 1.00 |
| Amazon S3 | GET requests (Athena and Iceberg reads) | 5,000,000 | requests | 0.0000004 | `s3_tier2_request` | 2.00 |
| Amazon Athena | 4 queries x 3 designs, 1 warm-up + 10 timed runs, equivalence checks, history checks, AWS smoke run, and one full repeat of the timed runs | 2.531 | TB scanned | 5 | `athena_tb_scanned` | 12.66 |
| Amazon CloudWatch | MSK PER_BROKER metrics (replication bytes) and Flink metrics | 4.500 | metric-months | 0.3 | `cloudwatch_metric_month` | 1.35 |
| Amazon CloudWatch | GetMetricData | 50,000 | metrics requested | 0.00001 | `cloudwatch_get_metric_data_metric` | 0.50 |
| Amazon CloudWatch | Flink logs ingested (checkpoint lines are read from them) | 3 | GB | 0.5 | `cloudwatch_logs_ingest_gb` | 1.50 |
| Amazon CloudWatch | Flink log storage | 0.300 | GB-months | 0.03 | `cloudwatch_logs_storage_gb_month` | 0.01 |
| AWS Glue | Data Catalog requests (Iceberg commits, Athena planning) | 20,000 | requests | 0.000001 | `glue_catalog_request` | 0.02 |
| AWS Cost Explorer | API requests for the cost report | 30 | requests | 0.01 | `cost_explorer_api_request` | 0.30 |
| AWS Budgets | one cost budget with email alerts, no actions | 5 | budget-days | 0 | `budgets_budget_day` | 0.00 |

| Service | Cost (USD) |
|---|---|
| Amazon EC2 | 19.00 |
| Amazon VPC | 0.51 |
| Amazon MSK | 12.46 |
| Managed Service for Apache Flink | 9.08 |
| Data transfer | 4.09 |
| Amazon S3 | 3.74 |
| Amazon Athena | 12.66 |
| Amazon CloudWatch | 3.36 |
| AWS Glue | 0.02 |
| AWS Cost Explorer | 0.30 |
| AWS Budgets | 0.00 |
| **Total** | **65.20** |

## Plan: compression none at full scale

| Service | Item | Quantity | Unit | Price (USD) | Price key | Cost (USD) |
|---|---|---|---|---|---|---|
| Amazon EC2 | runner m8id.2xlarge (generator, producer, Iceberg maintenance) | 38 | instance-hours | 0.52208 | `ec2_m8id_2xlarge_hour` | 19.84 |
| Amazon EC2 | runner root volume, gp3 50 GB | 2.639 | GB-months | 0.08 | `ebs_gp3_gb_month` | 0.22 |
| Amazon VPC | runner public IPv4 address | 38 | hours | 0.005 | `public_ipv4_hour` | 0.19 |
| Amazon MSK | 3 x kafka.m7g.large brokers | 54 | broker-hours | 0.204 | `msk_m7g_large_broker_hour` | 11.02 |
| Amazon MSK | broker storage, 400 GB per broker | 30 | GB-months | 0.1 | `msk_storage_gb_month` | 3.00 |
| Managed Service for Apache Flink | parallelism 12 plus 1 orchestration KPU, while an application runs | 78 | KPU-hours | 0.11 | `msf_kpu_hour` | 8.58 |
| Managed Service for Apache Flink | running application storage, 50 GB per KPU | 5 | GB-months | 0.1 | `msf_running_storage_gb_month` | 0.50 |
| Amazon VPC | interface endpoints (Glue, and STS if the Flink app needs it), one AZ each | 36 | endpoint-hours | 0.01 | `vpc_endpoint_hour` | 0.36 |
| Amazon VPC | interface endpoint data processed | 1 | GB | 0.01 | `vpc_endpoint_gb` | 0.01 |
| Data transfer | cross-AZ client traffic: produce zstd and none topics, Flink reads zstd topics | 757.200 | GB (billed both ways) | 0.01 | `data_transfer_regional_gb` | 7.58 |
| Amazon S3 | Iceberg data and metadata at peak (before expiration removes replaced files) plus Athena results | 31.981 | GB-months | 0.023 | `s3_standard_gb_month` | 0.74 |
| Amazon S3 | PUT, COPY, POST, LIST requests | 200,000 | requests | 0.000005 | `s3_tier1_request` | 1.00 |
| Amazon S3 | GET requests (Athena and Iceberg reads) | 5,000,000 | requests | 0.0000004 | `s3_tier2_request` | 2.00 |
| Amazon Athena | 4 queries x 3 designs, 1 warm-up + 10 timed runs, equivalence checks, history checks, AWS smoke run, and one full repeat of the timed runs | 2.531 | TB scanned | 5 | `athena_tb_scanned` | 12.66 |
| Amazon CloudWatch | MSK PER_BROKER metrics (replication bytes) and Flink metrics | 5 | metric-months | 0.3 | `cloudwatch_metric_month` | 1.50 |
| Amazon CloudWatch | GetMetricData | 50,000 | metrics requested | 0.00001 | `cloudwatch_get_metric_data_metric` | 0.50 |
| Amazon CloudWatch | Flink logs ingested (checkpoint lines are read from them) | 3 | GB | 0.5 | `cloudwatch_logs_ingest_gb` | 1.50 |
| Amazon CloudWatch | Flink log storage | 0.300 | GB-months | 0.03 | `cloudwatch_logs_storage_gb_month` | 0.01 |
| AWS Glue | Data Catalog requests (Iceberg commits, Athena planning) | 20,000 | requests | 0.000001 | `glue_catalog_request` | 0.02 |
| AWS Cost Explorer | API requests for the cost report | 30 | requests | 0.01 | `cost_explorer_api_request` | 0.30 |
| AWS Budgets | one cost budget with email alerts, no actions | 5 | budget-days | 0 | `budgets_budget_day` | 0.00 |

| Service | Cost (USD) |
|---|---|
| Amazon EC2 | 20.06 |
| Amazon VPC | 0.56 |
| Amazon MSK | 14.02 |
| Managed Service for Apache Flink | 9.08 |
| Data transfer | 7.58 |
| Amazon S3 | 3.74 |
| Amazon Athena | 12.66 |
| Amazon CloudWatch | 3.51 |
| AWS Glue | 0.02 |
| AWS Cost Explorer | 0.30 |
| AWS Budgets | 0.00 |
| **Total** | **71.50** |

## Hourly cost while resources exist

These rates show what an idle or forgotten stage costs. Usage charges (Athena scans, data transfer, S3 requests, log ingestion) come on top.

| Running | USD per hour |
|---|---|
| Base stage only (runner instance, its address and root volume) | 0.54 |
| Base and stream stages, no Flink application running (adds MSK brokers, broker storage, PER_BROKER metrics, endpoints) | 1.41 |
| Base and stream stages with one Flink application running (adds 13 KPU and running storage) | 2.92 |

## Inputs and their basis

- Kafka bytes per event, one copy, full model (results/pipeline_kafka.csv, local 3-broker run): A zstd 739.47561, B zstd 647.18917, A none 1923.07974, B none 982.59232. Scaled to 100,000,000 events: zstd topics 138.666 GB one copy for A and B together.
- Iceberg data bytes per row, full model, after the Flink commits (results/pipeline_iceberg.csv): A 893.36433, B 449.4125; C 625.076 (results/storage.csv, DuckDB zstd, 1,000 rows). Scaled to 100,000,000 rows (C: 1,000,000): 134.903 GB for A, B and C. Compaction writes a second copy until expiration deletes the first, so S3 holds up to twice that for up to 3 days.
- Athena bytes per query (upper bound, no file pruning): Q3 current state reads 55% of A's bytes and all of B's; Q1, Q2 and Q4 read the key columns plus email (and previous_email in A), taken as 3% of A and 4% of B. One pass over all 8 supported query and design pairs: 105.455 GB. Equivalence checks compare both directions with EXCEPT ALL, so each side is scanned twice.
- Generator: bench/benchmark.py generate(), unchanged, with an Avro step-file sink, measured at 106.09 microseconds per event on an Apple M3 (200,000 events): 2.95 hours for 100,000,000 events. The plan allows 4.5 hours on the runner.
- Runner hours (36, plus 2 for the full-scale none plan): setup and build 1, generation 4.5, waiting for MSK 0.75, AWS smoke run at bench-full scale 2, full Kafka and Flink runs 8, C table and Iceberg maintenance 3, Athena phase 4, margin 12.75.
- MSK hours (16, plus 2): creation 0.75, smoke 2, full runs 8, margin 5.25. Broker storage 400 GB each, so a full uncompressed A topic (192 GB per broker) fits and gp2-style volumes keep their baseline throughput.
- Managed Flink: 6 application-hours at 13 KPU. Basis: the local runs reached 22084.8056537102... (A) and 50735.6671740233... (B) events per second at parallelism 3 (results/pipeline_kafka.csv, full model, zstd). If one KPU does a quarter of one local slot, parallelism 12 needs about 1.3 hours for A and 0.6 hours for B; 6 hours covers that, the smoke run, and one repeat of A.
- Cross-AZ data transfer uses DataTransfer-Regional-Bytes at 0.01 USD per GB each way. The Price List also returns USE1-DataTransfer-xAZ-In-Bytes and -Out-Bytes at 0.00 effective 2026-06-01; the estimate keeps the higher price.
- MSK replication between brokers is not billed (MSK pricing page). DEFAULT-level MSK metrics are free; PER_BROKER metrics, needed for ReplicationBytesInPerSec and ReplicationBytesOutPerSec, are billed as CloudWatch metrics, taken as 60 per broker.
- Not estimated because they round to zero here: KMS requests for AWS managed keys, Glue catalog storage, Pricing API calls (free).
