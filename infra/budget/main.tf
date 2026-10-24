# AWS Budgets cost budget for the AWS scale run. Created before anything else and
# destroyed last, after Cost Explorer has posted the run's charges (infra/TEARDOWN.md).
#
#   TF_VAR_alert_email=you@example.com terraform -chdir=infra/budget apply -var budget_usd=150

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
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

variable "budget_usd" {
  description = "Budget amount in USD; the alerts fire at 50, 80 and 100 percent of it."
  type        = string
}

variable "alert_email" {
  description = "Address the budget alerts go to. Pass it at apply time; it is not committed."
  type        = string
  sensitive   = true
}

variable "start" {
  description = "First day of the first monthly period."
  type        = string
  default     = "2026-10-01_00:00"
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      project = "audit-trail-bench"
      run     = var.run
    }
  }
}

# Account-wide, so charges that carry no tag (data transfer, CloudWatch metrics, API calls)
# count as well. Credits and refunds are excluded so the alerts track gross charges.
resource "aws_budgets_budget" "run" {
  name              = "audit-trail-bench-aws-scale"
  budget_type       = "COST"
  limit_amount      = var.budget_usd
  limit_unit        = "USD"
  time_unit         = "MONTHLY"
  time_period_start = var.start

  cost_types {
    include_credit  = false
    include_refund  = false
    include_support = false
  }

  dynamic "notification" {
    for_each = [50, 80, 100]
    content {
      comparison_operator        = "GREATER_THAN"
      threshold                  = notification.value
      threshold_type             = "PERCENTAGE"
      notification_type          = "ACTUAL"
      subscriber_email_addresses = [var.alert_email]
    }
  }

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 100
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}

output "budget_name" {
  value = aws_budgets_budget.run.name
}
