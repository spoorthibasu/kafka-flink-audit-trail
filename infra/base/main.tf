# Base stage of the AWS scale run: the run bucket, the two IAM roles (both capped by the
# permissions boundary audit-trail-bench-boundary, which the account owner creates from
# infra/iam/role-boundary-policy.json), the security group, the runner instance, the Glue
# database, the Athena workgroup and the Flink log group. Everything is in the default VPC
# of the region and carries project=audit-trail-bench.
#
#   terraform -chdir=infra/base init && terraform -chdir=infra/base apply

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

variable "region" {
  type    = string
  default = "us-east-1"
}

variable "run" {
  description = "Value of the run tag on every resource of this run."
  type        = string
  default     = "aws-scale-1"
}

variable "runner_instance_type" {
  type    = string
  default = "m8id.2xlarge"
}

variable "runner_az_id" {
  description = "AZ ID for the runner; must offer the instance type and be one of the MSK subnets."
  type        = string
  default     = "use1-az1"
}

variable "msk_az_ids" {
  description = "Three AZ IDs that support MSK; the brokers and the Flink application use their default subnets."
  type        = list(string)
  default     = ["use1-az1", "use1-az2", "use1-az4"]
}

provider "aws" {
  region = var.region
  default_tags {
    tags = local.tags
  }
}

locals {
  tags = {
    project = "audit-trail-bench"
    run     = var.run
  }
  bucket    = "audit-trail-bench-run-${random_id.bucket.hex}"
  database  = "audit_trail_bench_aws"
  log_group = "/aws/kinesis-analytics/audit-trail-bench"
}

data "aws_caller_identity" "me" {}
data "aws_partition" "p" {}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnet" "msk" {
  for_each             = toset(var.msk_az_ids)
  vpc_id               = data.aws_vpc.default.id
  availability_zone_id = each.value
  default_for_az       = true
}

data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

# ------------------------------------------------------------------- bucket

resource "random_id" "bucket" {
  byte_length = 4
}

resource "aws_s3_bucket" "run" {
  bucket        = local.bucket
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "run" {
  bucket                  = aws_s3_bucket.run.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "run" {
  bucket = aws_s3_bucket.run.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "run" {
  bucket = aws_s3_bucket.run.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "run" {
  bucket = aws_s3_bucket.run.id
  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

# -------------------------------------------------------------------- roles

locals {
  boundary_arn = "arn:${data.aws_partition.p.partition}:iam::${data.aws_caller_identity.me.account_id}:policy/audit-trail-bench-boundary"
  s3_statements = [
    {
      Effect   = "Allow"
      Action   = ["s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads"]
      Resource = aws_s3_bucket.run.arn
    },
    {
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]
      Resource = "${aws_s3_bucket.run.arn}/*"
    },
  ]
  glue_resources = [
    "arn:${data.aws_partition.p.partition}:glue:${var.region}:${data.aws_caller_identity.me.account_id}:catalog",
    "arn:${data.aws_partition.p.partition}:glue:${var.region}:${data.aws_caller_identity.me.account_id}:database/${local.database}",
    "arn:${data.aws_partition.p.partition}:glue:${var.region}:${data.aws_caller_identity.me.account_id}:table/${local.database}/*",
  ]
}

resource "aws_iam_role" "runner" {
  name                 = "audit-trail-bench-runner"
  permissions_boundary = local.boundary_arn
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "ec2.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy" "runner" {
  name = "audit-trail-bench-runner"
  role = aws_iam_role.runner.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(local.s3_statements, [
      {
        Effect   = "Allow"
        Action   = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable", "glue:DeleteTable"]
        Resource = local.glue_resources
      },
      {
        Effect = "Allow"
        Action = [
          "ssm:UpdateInstanceInformation", "ssm:ListInstanceAssociations", "ssm:DescribeAssociation",
          "ssmmessages:CreateControlChannel", "ssmmessages:CreateDataChannel", "ssmmessages:OpenControlChannel",
          "ssmmessages:OpenDataChannel", "ec2messages:AcknowledgeMessage", "ec2messages:DeleteMessage",
          "ec2messages:FailMessage", "ec2messages:GetEndpoint", "ec2messages:GetMessages", "ec2messages:SendReply",
        ]
        Resource = "*"
      },
    ])
  })
}

resource "aws_iam_instance_profile" "runner" {
  name = "audit-trail-bench-runner"
  role = aws_iam_role.runner.name
}

resource "aws_iam_role" "flink" {
  name                 = "audit-trail-bench-flink"
  permissions_boundary = local.boundary_arn
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "kinesisanalytics.amazonaws.com" }
      Action    = "sts:AssumeRole"
      Condition = { StringEquals = { "aws:SourceAccount" = data.aws_caller_identity.me.account_id } }
    }]
  })
}

resource "aws_iam_role_policy" "flink" {
  name = "audit-trail-bench-flink"
  role = aws_iam_role.flink.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(local.s3_statements, [
      {
        Effect   = "Allow"
        Action   = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:CreateTable", "glue:UpdateTable"]
        Resource = local.glue_resources
      },
      {
        Effect   = "Allow"
        Action   = ["logs:DescribeLogStreams", "logs:PutLogEvents"]
        Resource = [aws_cloudwatch_log_group.flink.arn, "${aws_cloudwatch_log_group.flink.arn}:*"]
      },
      {
        Effect   = "Allow"
        Action   = "logs:DescribeLogGroups"
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "ec2:DescribeVpcs", "ec2:DescribeSubnets", "ec2:DescribeSecurityGroups", "ec2:DescribeDhcpOptions",
          "ec2:CreateNetworkInterface", "ec2:CreateNetworkInterfacePermission", "ec2:DescribeNetworkInterfaces",
          "ec2:DeleteNetworkInterface",
        ]
        Resource  = "*"
        Condition = { StringEquals = { "aws:RequestedRegion" = var.region } }
      },
    ])
  })
}

# ------------------------------------------------------------------ network

# One group for the brokers, the Flink application's network interfaces, the runner and
# the interface endpoints: members reach each other on any TCP port; nothing else gets in.
resource "aws_security_group" "run" {
  name        = "audit-trail-bench"
  description = "audit-trail-bench AWS scale run"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "members of this group"
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    self        = true
  }

  egress {
    description = "outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ------------------------------------------------------------------- runner

resource "aws_instance" "runner" {
  ami                         = data.aws_ssm_parameter.al2023.value
  instance_type               = var.runner_instance_type
  subnet_id                   = data.aws_subnet.msk[var.runner_az_id].id
  vpc_security_group_ids      = [aws_security_group.run.id]
  iam_instance_profile        = aws_iam_instance_profile.runner.name
  associate_public_ip_address = true
  user_data                   = file("${path.module}/runner-user-data.sh")

  metadata_options {
    http_tokens = "required"
  }

  root_block_device {
    volume_type           = "gp3"
    volume_size           = 50
    delete_on_termination = true
    tags                  = local.tags
  }

  tags = { Name = "audit-trail-bench-runner" }

  lifecycle {
    ignore_changes = [ami]
  }
}

# ------------------------------------------------------- catalog and queries

resource "aws_glue_catalog_database" "run" {
  name         = local.database
  location_uri = "s3://${aws_s3_bucket.run.bucket}/warehouse/${local.database}.db"
}

resource "aws_athena_workgroup" "run" {
  name          = "audit-trail-bench"
  force_destroy = true
  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false
    # 300 GiB, above the largest query the estimate expects (Q3 on A, about 50 GB).
    bytes_scanned_cutoff_per_query = 322122547200
    engine_version {
      selected_engine_version = "Athena engine version 3"
    }
    result_configuration {
      output_location = "s3://${aws_s3_bucket.run.bucket}/athena-results/"
    }
  }
}

resource "aws_cloudwatch_log_group" "flink" {
  name              = local.log_group
  retention_in_days = 7
}

resource "aws_cloudwatch_log_stream" "flink" {
  name           = "flink"
  log_group_name = aws_cloudwatch_log_group.flink.name
}

# ------------------------------------------------------------------ outputs

output "region" {
  value = var.region
}

output "bucket" {
  value = aws_s3_bucket.run.bucket
}

output "bucket_arn" {
  value = aws_s3_bucket.run.arn
}

output "warehouse" {
  value = "s3://${aws_s3_bucket.run.bucket}/warehouse"
}

output "database" {
  value = aws_glue_catalog_database.run.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.run.name
}

output "runner_instance_id" {
  value = aws_instance.runner.id
}

output "runner_instance_type" {
  value = aws_instance.runner.instance_type
}

output "security_group_id" {
  value = aws_security_group.run.id
}

output "vpc_id" {
  value = data.aws_vpc.default.id
}

output "msk_subnet_ids" {
  value = [for az in var.msk_az_ids : data.aws_subnet.msk[az].id]
}

output "runner_subnet_id" {
  value = data.aws_subnet.msk[var.runner_az_id].id
}

output "flink_role_arn" {
  value = aws_iam_role.flink.arn
}

output "flink_log_stream_arn" {
  value = "${aws_cloudwatch_log_group.flink.arn}:log-stream:${aws_cloudwatch_log_stream.flink.name}"
}

output "flink_log_group" {
  value = aws_cloudwatch_log_group.flink.name
}

output "run_tag" {
  value = var.run
}
