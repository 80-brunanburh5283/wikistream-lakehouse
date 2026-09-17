# MSK Serverless, which is the closest managed thing to the single-broker KRaft
# container in docker-compose.yml.
#
# Serverless rather than provisioned for two reasons. Cost: a provisioned cluster
# bills three brokers by the hour whether or not anything is producing, and this
# workload is roughly 50 events per second — a rounding error against the smallest
# broker that MSK offers. Operations: the local stack has no broker tuning, no
# partition rebalancing and no storage autoscaling to port, so provisioning a
# cluster would mean inventing operational decisions the project has never had to
# make. What Serverless costs instead is throughput: it caps at 200 MB/s ingress
# per cluster, and it only speaks SASL/IAM, so the client config changes.
#
# What is not here: the topic. MSK Serverless exposes no API for topic
# administration, so `wiki.recentchange` is created by a client — the same
# `scripts/create_topics.sh` with a different bootstrap string and an IAM SASL
# handler. A `null_resource` running the Kafka CLI would put a topic's existence
# into Terraform state without putting it under Terraform's control, which is
# worse than leaving it out.

variable "name_prefix" {
  description = "Prefix for the cluster name."
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnets, at least two, in different availability zones."
  type        = list(string)
}

variable "security_group_ids" {
  description = "Security groups for the cluster's VPC endpoints."
  type        = list(string)
}

resource "aws_msk_serverless_cluster" "this" {
  cluster_name = "${var.name_prefix}-events"

  vpc_config {
    subnet_ids         = var.private_subnet_ids
    security_group_ids = var.security_group_ids
  }

  client_authentication {
    sasl {
      iam {
        # The only option Serverless has. It is also the one worth having: there
        # is no password to rotate, no SCRAM secret in Secrets Manager, and topic
        # authorisation is expressed in the same IAM policy language as the S3
        # access next to it.
        enabled = true
      }
    }
  }

  tags = { Name = "${var.name_prefix}-events" }
}

output "cluster_arn" {
  description = "ARN of the MSK Serverless cluster, for the IAM policy that scopes topic access."
  value       = aws_msk_serverless_cluster.this.arn
}

output "bootstrap_brokers_sasl_iam" {
  description = "Bootstrap string for WS_KAFKA_BOOTSTRAP_SERVERS. Port 9098, SASL_SSL, AWS_MSK_IAM."
  value       = aws_msk_serverless_cluster.this.bootstrap_brokers_sasl_iam
}
