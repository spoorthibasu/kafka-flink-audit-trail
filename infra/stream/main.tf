# Stream stage of the AWS scale run: the MSK cluster, the VPC endpoints the VPC-connected
# Flink applications need (S3 gateway, Glue interface), the application jar, and one
# Managed Service for Apache Flink application per design. Reads the base stage's outputs
# from its local state. Destroyed as soon as the Kafka and Flink measurements are done.
#
#   (cd bench/aws/flink-app && mvn -q -B package -DskipTests)
#   terraform -chdir=infra/stream init && terraform -chdir=infra/stream apply

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

variable "kafka_version" {
  description = "MSK Kafka version; KRaft mode. 3.7.x reached MSK end of support on 2026-09-01."
  type        = string
  default     = "3.9.x.kraft"
}

variable "broker_instance_type" {
  type    = string
  default = "kafka.m7g.large"
}

variable "broker_storage_gb" {
  type    = number
  default = 400
}

variable "flink_parallelism" {
  description = "Managed Flink caps parallelism at 4 for this account until AWS raises its KPU limit (CreateApplication says so)."
  type        = number
  default     = 4
}

variable "flink_parallelism_per_kpu" {
  type    = number
  default = 1
}

variable "checkpoint_interval_ms" {
  type    = number
  default = 60000
}

variable "min_pause_ms" {
  type    = number
  default = 30000
}

variable "sts_endpoint" {
  description = "Add an STS interface endpoint, only if the Flink application cannot get credentials without one."
  type        = bool
  default     = false
}

variable "app_jar" {
  type    = string
  default = "../../bench/aws/flink-app/target/audit-trail-aws-1.0.0.jar"
}

data "terraform_remote_state" "base" {
  backend = "local"
  config = {
    path = "${path.module}/../base/terraform.tfstate"
  }
}

locals {
  base = data.terraform_remote_state.base.outputs
  tags = {
    project = "audit-trail-bench"
    run     = local.base.run_tag
  }
}

provider "aws" {
  region = local.base.region
  default_tags {
    tags = local.tags
  }
}

# --------------------------------------------------------------------- MSK

resource "aws_msk_cluster" "run" {
  cluster_name           = "audit-trail-bench"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = 3
  enhanced_monitoring    = "PER_BROKER"

  broker_node_group_info {
    instance_type   = var.broker_instance_type
    client_subnets  = local.base.msk_subnet_ids
    security_groups = [local.base.security_group_id]
    storage_info {
      ebs_storage_info {
        volume_size = var.broker_storage_gb
      }
    }
  }

  # Plaintext and unauthenticated inside the VPC, as the local benchmark's brokers; the
  # security group admits only members of the run's group. Data at rest uses the AWS
  # managed key; brokers encrypt traffic between themselves.
  client_authentication {
    unauthenticated = true
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "PLAINTEXT"
      in_cluster    = true
    }
  }
}

# --------------------------------------------------------------- endpoints

data "aws_route_table" "main" {
  vpc_id = local.base.vpc_id
  filter {
    name   = "association.main"
    values = ["true"]
  }
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = local.base.vpc_id
  service_name      = "com.amazonaws.${local.base.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [data.aws_route_table.main.id]
}

resource "aws_vpc_endpoint" "glue" {
  vpc_id              = local.base.vpc_id
  service_name        = "com.amazonaws.${local.base.region}.glue"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true
  subnet_ids          = [local.base.runner_subnet_id]
  security_group_ids  = [local.base.security_group_id]
}

resource "aws_vpc_endpoint" "sts" {
  count               = var.sts_endpoint ? 1 : 0
  vpc_id              = local.base.vpc_id
  service_name        = "com.amazonaws.${local.base.region}.sts"
  vpc_endpoint_type   = "Interface"
  private_dns_enabled = true
  subnet_ids          = [local.base.runner_subnet_id]
  security_group_ids  = [local.base.security_group_id]
}

# ------------------------------------------------------------------- Flink

resource "aws_s3_object" "jar" {
  bucket      = local.base.bucket
  key         = "flink/audit-trail-aws-${filemd5(var.app_jar)}.jar"
  source      = var.app_jar
  source_hash = filemd5(var.app_jar)
}

resource "aws_kinesisanalyticsv2_application" "design" {
  for_each               = toset(["a", "b"])
  name                   = "audit-trail-bench-${each.key}"
  runtime_environment    = "FLINK-1_20"
  service_execution_role = local.base.flink_role_arn
  start_application      = false

  application_configuration {
    application_code_configuration {
      code_content_type = "ZIPFILE"
      code_content {
        s3_content_location {
          bucket_arn = local.base.bucket_arn
          file_key   = aws_s3_object.jar.key
        }
      }
    }

    # The run script sets topic, group.id and table before each start (UpdateApplication);
    # these are the values for the full-scale run.
    environment_properties {
      property_group {
        property_group_id = "job"
        property_map = {
          bootstrap  = aws_msk_cluster.run.bootstrap_brokers
          topic      = "full-${each.key}-zstd"
          "group.id" = "full-${each.key}"
          design     = upper(each.key)
          catalog    = "glue"
          warehouse  = local.base.warehouse
          database   = local.base.database
          table      = each.key
        }
      }
    }

    flink_application_configuration {
      checkpoint_configuration {
        configuration_type            = "CUSTOM"
        checkpointing_enabled         = true
        checkpoint_interval           = var.checkpoint_interval_ms
        min_pause_between_checkpoints = var.min_pause_ms
      }
      monitoring_configuration {
        configuration_type = "CUSTOM"
        log_level          = "INFO"
        metrics_level      = "APPLICATION"
      }
      parallelism_configuration {
        configuration_type   = "CUSTOM"
        parallelism          = var.flink_parallelism
        parallelism_per_kpu  = var.flink_parallelism_per_kpu
        auto_scaling_enabled = false
      }
    }

    application_snapshot_configuration {
      snapshots_enabled = false
    }

    vpc_configuration {
      subnet_ids         = local.base.msk_subnet_ids
      security_group_ids = [local.base.security_group_id]
    }
  }

  cloudwatch_logging_options {
    log_stream_arn = local.base.flink_log_stream_arn
  }

  lifecycle {
    ignore_changes = [application_configuration[0].environment_properties]
  }

  depends_on = [aws_vpc_endpoint.s3, aws_vpc_endpoint.glue]
}

# ----------------------------------------------------------------- outputs

output "msk_cluster_arn" {
  value = aws_msk_cluster.run.arn
}

output "msk_cluster_name" {
  value = aws_msk_cluster.run.cluster_name
}

output "bootstrap_brokers" {
  value = aws_msk_cluster.run.bootstrap_brokers
}

output "kafka_version" {
  value = aws_msk_cluster.run.kafka_version
}

output "flink_applications" {
  value = { for k, a in aws_kinesisanalyticsv2_application.design : upper(k) => a.name }
}

output "flink_jar_key" {
  value = aws_s3_object.jar.key
}
