variable "project" {
  description = "Short name prefixed onto every resource. Lower case, no underscores: it ends up in an S3 bucket name."
  type        = string
  default     = "wikistream"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{2,20}$", var.project))
    error_message = "project must be 3-21 characters of lower-case letters, digits and hyphens, starting with a letter."
  }
}

variable "environment" {
  description = "Deployment stage. Only affects naming and tags; there is one of everything either way."
  type        = string
  default     = "prod"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of dev, staging, prod."
  }
}

variable "region" {
  description = "AWS region. Everything here is regional; there is no cross-region story."
  type        = string
  default     = "eu-central-1"
}

# --- Networking ------------------------------------------------------------
#
# This module does not create a VPC. MSK Serverless and EMR Serverless both need
# private subnets with a route to S3 and to each other, and every account that
# would host this already has a network with an owner who is not this module.
# Taking the network as an input also keeps the module composable and its blast
# radius small. See DECISIONS.md ADR-0035.

variable "vpc_id" {
  description = "Existing VPC to place the security groups in."
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnets for MSK Serverless and EMR Serverless. One per availability zone, at least two."
  type        = list(string)

  validation {
    condition     = length(var.private_subnet_ids) >= 2
    error_message = "MSK Serverless requires subnets in at least two availability zones."
  }
}

# --- Storage ---------------------------------------------------------------

variable "bronze_transition_days" {
  description = "Age at which bronze objects move to Infrequent Access. Bronze is the replay log: read often in week one, rarely after."
  type        = number
  default     = 30

  validation {
    condition     = var.bronze_transition_days >= 30
    error_message = "S3 charges a 30-day minimum for Standard-IA, so transitioning sooner costs more, not less."
  }
}

variable "bronze_retention_days" {
  description = "Backstop expiry for bronze data files, in days. 0 disables it, which is the default; read the comment in modules/s3 before changing it."
  type        = number
  default     = 0

  validation {
    condition     = var.bronze_retention_days == 0 || var.bronze_retention_days > var.bronze_transition_days
    error_message = "bronze_retention_days must be 0 (disabled) or later than bronze_transition_days."
  }
}

# --- Compute ---------------------------------------------------------------

variable "emr_release_label" {
  description = "EMR Serverless release. Determines the Spark version, which must match the one the jobs were written against."
  type        = string
  default     = "emr-7.9.0"
}

variable "emr_max_cpu" {
  description = "Ceiling on concurrent vCPU for the EMR Serverless application. This is the cost control that matters most."
  type        = number
  default     = 32
}

variable "emr_idle_timeout_minutes" {
  description = "Idle time before EMR Serverless releases its workers. Streaming jobs never idle, so this only bounds the cost of a stopped stream."
  type        = number
  default     = 15
}
