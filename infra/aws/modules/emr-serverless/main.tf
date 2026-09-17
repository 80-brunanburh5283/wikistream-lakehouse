# EMR Serverless in place of the `spark` container.
#
# The awkward part of this mapping, said plainly: EMR Serverless is built around
# jobs that finish, and a Structured Streaming query does not finish. It runs as a
# job run with no end, which works — the application keeps the workers warm and the
# driver alive — but it means the streaming job is billed for every vCPU-second it
# holds, and `auto_stop_configuration` never fires while it is healthy. So the cost
# model is closer to a provisioned cluster than the word "serverless" suggests, and
# docs/cost.md prices it that way rather than at the on-demand rate.
#
# The alternative worth naming is EMR on EKS, which is cheaper at sustained load
# and brings a Kubernetes cluster to operate. For one streaming query at roughly 50
# events per second, the cluster is the larger cost — in money and in attention.

variable "name_prefix" {
  description = "Prefix for the application name."
  type        = string
}

variable "release_label" {
  description = "EMR release, which fixes the Spark version."
  type        = string
}

variable "max_cpu" {
  description = "Ceiling on concurrent vCPU."
  type        = number
}

variable "idle_timeout_minutes" {
  description = "Idle time before workers are released."
  type        = number
}

variable "private_subnet_ids" {
  description = "Subnets the workers run in. Must reach MSK and S3."
  type        = list(string)
}

variable "security_group_ids" {
  description = "Security groups for the workers."
  type        = list(string)
}

variable "job_execution_role_arn" {
  description = "Role the job runs assume. Referenced in the output, not attached to the application."
  type        = string
}

resource "aws_emrserverless_application" "this" {
  name          = "${var.name_prefix}-streaming"
  release_label = var.release_label
  type          = "SPARK"

  # Pre-initialised capacity. Without it the first job run waits for workers to be
  # provisioned — a minute or two — which is invisible for a batch job and is
  # dropped events for a stream restart, because the Kafka retention clock does not
  # pause while Spark starts. One driver and two executors is what the local stack
  # runs with.
  initial_capacity {
    initial_capacity_type = "Driver"

    initial_capacity_config {
      worker_count = 1

      worker_configuration {
        cpu    = "2 vCPU"
        memory = "8 GB"
      }
    }
  }

  initial_capacity {
    initial_capacity_type = "Executor"

    initial_capacity_config {
      worker_count = 2

      worker_configuration {
        cpu    = "2 vCPU"
        memory = "8 GB"
        # Iceberg writes go through local scratch before the multipart upload, and
        # the default 20 GB is enough for a micro-batch of this size with room for
        # a compaction job running alongside.
        disk = "20 GB"
      }
    }
  }

  maximum_capacity {
    cpu    = "${var.max_cpu} vCPU"
    memory = "${var.max_cpu * 4} GB"
    disk   = "${var.max_cpu * 20} GB"
  }

  network_configuration {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = var.security_group_ids
  }

  auto_start_configuration {
    enabled = true
  }

  auto_stop_configuration {
    enabled = true
    # This only ever fires when the stream is already down. It is the guard against
    # paying for pre-initialised capacity through a weekend after a failed restart
    # nobody noticed, which is a bill this design can otherwise produce.
    idle_timeout_minutes = var.idle_timeout_minutes
  }

  tags = { Name = "${var.name_prefix}-streaming" }
}

output "application_id" {
  description = "Application id for `aws emr-serverless start-job-run --application-id`."
  value       = aws_emrserverless_application.this.id
}

output "job_execution_role_arn" {
  description = "Echoed back so the start-job-run command can be assembled from outputs alone."
  value       = var.job_execution_role_arn
}
