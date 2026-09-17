# Two security groups and nothing else.
#
# The interesting decision here is what is *not* in the egress rules. There is no
# `0.0.0.0/0` rule — the default in most examples, including AWS's own — so these
# workers cannot reach the internet at all. Everything they need is either inside
# the VPC or behind a VPC endpoint: MSK on 9098, S3 through the gateway endpoint's
# managed prefix list, and Glue, STS and CloudWatch Logs through interface
# endpoints that live in the VPC's own CIDR.
#
# That is worth the extra twenty lines because it is the one control that still
# holds after a Spark job has been compromised. A job that pulls a poisoned wheel
# from PyPI at runtime — which is how this class of thing actually happens — can
# read the whole lakehouse and has nowhere to send it.
#
# The cost is a prerequisite: the account must already have the S3 gateway endpoint
# and interface endpoints for Glue and STS. README.md lists them, and a job that
# hangs on `sts:AssumeRole` with no error is what a missing one looks like.

variable "name_prefix" {
  description = "Prefix for security group names."
  type        = string
}

variable "vpc_id" {
  description = "VPC the security groups belong to."
  type        = string
}

variable "kafka_iam_port" {
  description = "MSK Serverless bootstrap port. IAM authentication only listens on 9098; 9092 and 9094 are provisioned-cluster ports."
  type        = number
  default     = 9098
}

resource "aws_security_group" "msk_client" {
  name        = "${var.name_prefix}-msk-clients"
  description = "Attached to MSK Serverless. Accepts Kafka traffic from the EMR security group only."
  vpc_id      = var.vpc_id

  tags = { Name = "${var.name_prefix}-msk-clients" }
}

resource "aws_security_group" "emr" {
  name        = "${var.name_prefix}-emr-serverless"
  description = "Attached to EMR Serverless workers. Egress to MSK and to AWS APIs, nothing else."
  vpc_id      = var.vpc_id

  tags = { Name = "${var.name_prefix}-emr-serverless" }
}

# The rules are separate resources rather than inline `ingress`/`egress` blocks.
# Inline blocks are authoritative for the whole group, so any rule added out of
# band — by a console click during an incident — is silently deleted on the next
# apply. `aws_vpc_security_group_*_rule` also lets each rule carry its own
# description, which is what a reviewer reads six months later.

resource "aws_vpc_security_group_ingress_rule" "msk_from_emr" {
  security_group_id            = aws_security_group.msk_client.id
  referenced_security_group_id = aws_security_group.emr.id
  from_port                    = var.kafka_iam_port
  to_port                      = var.kafka_iam_port
  ip_protocol                  = "tcp"
  description                  = "Spark jobs consuming wiki.recentchange over SASL/IAM"
}

resource "aws_vpc_security_group_egress_rule" "emr_to_msk" {
  security_group_id            = aws_security_group.emr.id
  referenced_security_group_id = aws_security_group.msk_client.id
  from_port                    = var.kafka_iam_port
  to_port                      = var.kafka_iam_port
  ip_protocol                  = "tcp"
  description                  = "Kafka bootstrap and fetch"
}

resource "aws_vpc_security_group_egress_rule" "emr_to_s3" {
  security_group_id = aws_security_group.emr.id
  # A gateway endpoint has no address of its own; traffic to it is addressed to
  # S3's public ranges and intercepted by the route table. AWS publishes those
  # ranges as a managed prefix list, which is the only way to write this rule
  # without hard-coding a CIDR set that changes.
  prefix_list_id = data.aws_ec2_managed_prefix_list.s3.id
  from_port      = 443
  to_port        = 443
  ip_protocol    = "tcp"
  description    = "Iceberg data and metadata through the S3 gateway endpoint"
}

resource "aws_vpc_security_group_egress_rule" "emr_to_vpc_endpoints" {
  security_group_id = aws_security_group.emr.id
  # Interface endpoints get ENIs inside the VPC, so their addresses are in the
  # VPC's CIDR. Scoping to the CIDR rather than to each endpoint's security group
  # keeps this module from needing to know which endpoints the account provides;
  # the trade-off is that a worker can also talk to anything else in the VPC on
  # 443, which in a VPC dedicated to this pipeline is nothing.
  cidr_ipv4   = data.aws_vpc.this.cidr_block
  from_port   = 443
  to_port     = 443
  ip_protocol = "tcp"
  description = "Glue, STS and CloudWatch Logs through interface endpoints"
}

data "aws_vpc" "this" {
  id = var.vpc_id
}

data "aws_region" "current" {}

data "aws_ec2_managed_prefix_list" "s3" {
  name = "com.amazonaws.${data.aws_region.current.region}.s3"
}

output "msk_client_security_group_id" {
  description = "Security group to attach to the MSK Serverless cluster."
  value       = aws_security_group.msk_client.id
}

output "emr_security_group_id" {
  description = "Security group to attach to the EMR Serverless application."
  value       = aws_security_group.emr.id
}
