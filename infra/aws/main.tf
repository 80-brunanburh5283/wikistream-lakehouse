# The same pipeline as docker-compose.yml, expressed in managed AWS services.
#
# This file is the mapping, one module call per local container:
#
#   Kafka (KRaft, one broker)  -> MSK Serverless          modules/msk
#   MinIO                      -> S3, one bucket          modules/s3
#   Iceberg REST catalog       -> Glue Data Catalog       modules/glue
#   Spark Structured Streaming -> EMR Serverless          modules/emr-serverless
#   (nothing local)            -> IAM roles and policies  modules/iam
#   (nothing local)            -> two security groups     modules/network
#
# Two things in the local stack have no module here, and their absence is the
# point rather than an omission:
#
#   Trino. Athena is Trino, serverless, and it needs no infrastructure — a
#   workgroup and a results bucket prefix, both of which are three lines that
#   would add a service to the cost table without adding anything to the design.
#   docs/architecture.md says what changes in the dbt profile.
#
#   Dagster. There is no managed Dagster, so this would be either Dagster+ (a
#   paid product this repository will not require) or a container platform, which
#   is a second infrastructure story of its own. k8s/ is where that lives.

locals {
  name_prefix = "${var.project}-${var.environment}"

  # The topic and the table namespaces are the same strings the local stack uses,
  # from .env.example. They appear here because IAM policies name them: a policy
  # that grants access to "the topic" by wildcard is not least privilege.
  kafka_topic = "wiki.recentchange"
  namespaces  = ["bronze", "silver", "gold"]
}

module "network" {
  source = "./modules/network"

  name_prefix = local.name_prefix
  vpc_id      = var.vpc_id
}

module "s3" {
  source = "./modules/s3"

  name_prefix            = local.name_prefix
  bronze_transition_days = var.bronze_transition_days
  bronze_retention_days  = var.bronze_retention_days
}

module "msk" {
  source = "./modules/msk"

  name_prefix        = local.name_prefix
  private_subnet_ids = var.private_subnet_ids
  security_group_ids = [module.network.msk_client_security_group_id]
}

module "glue" {
  source = "./modules/glue"

  name_prefix   = local.name_prefix
  namespaces    = local.namespaces
  warehouse_uri = "s3://${module.s3.lakehouse_bucket_id}/warehouse"
}

module "iam" {
  source = "./modules/iam"

  name_prefix          = local.name_prefix
  lakehouse_bucket_arn = module.s3.lakehouse_bucket_arn
  logs_bucket_arn      = module.s3.logs_bucket_arn
  msk_cluster_arn      = module.msk.cluster_arn
  kafka_topic          = local.kafka_topic
  glue_database_names  = module.glue.database_names
}

module "emr_serverless" {
  source = "./modules/emr-serverless"

  name_prefix            = local.name_prefix
  release_label          = var.emr_release_label
  max_cpu                = var.emr_max_cpu
  idle_timeout_minutes   = var.emr_idle_timeout_minutes
  private_subnet_ids     = var.private_subnet_ids
  security_group_ids     = [module.network.emr_security_group_id]
  job_execution_role_arn = module.iam.emr_job_execution_role_arn
}
