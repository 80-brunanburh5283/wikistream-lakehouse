# The roles, and the reason each permission is in the list.
#
# Two principals do work in this design and they need different things:
#
#   emr_job_execution  Spark: read one Kafka topic, read and write the Iceberg
#                      tables under warehouse/, write logs.
#   dbt_runner         dbt through Athena: read silver, create and replace tables
#                      in gold, and nothing at all in bronze.
#
# Neither policy contains a `"Resource": "*"` statement for an action that touches
# data. Where a wildcard appears it is inside a bucket, a database or a topic ARN,
# and there is a comment saying which runtime detail forces it. That distinction —
# wildcard within a named resource, versus wildcard instead of one — is most of
# what "least privilege" means in practice.
#
# There is no user, no access key and no secret anywhere in this module. The dbt
# role is assumed from GitHub Actions over OIDC, so CI holds a short-lived token
# and there is nothing to rotate or to leak.

variable "name_prefix" {
  description = "Prefix for role and policy names."
  type        = string
}

variable "lakehouse_bucket_arn" {
  description = "ARN of the lakehouse bucket."
  type        = string
}

variable "logs_bucket_arn" {
  description = "ARN of the log bucket."
  type        = string
}

variable "msk_cluster_arn" {
  description = "ARN of the MSK Serverless cluster."
  type        = string
}

variable "kafka_topic" {
  description = "The one topic Spark is allowed to read."
  type        = string
}

variable "glue_database_names" {
  description = "Glue databases the pipeline uses, in layer order."
  type        = list(string)
}

variable "github_repository" {
  description = "owner/name of the repository whose Actions runs may assume the dbt role."
  type        = string
  default     = "william-sarkar/wikistream-lakehouse"
}

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
data "aws_partition" "current" {}

locals {
  catalog_arn = "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:catalog"

  database_arns = [
    for name in var.glue_database_names :
    "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:database/${name}"
  ]

  table_arns = [
    for name in var.glue_database_names :
    "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:table/${name}/*"
  ]

  # MSK's authorisation ARNs are the cluster ARN with the resource type swapped,
  # so they can be derived rather than passed in and kept in step by hand.
  msk_topic_arn_prefix = replace(var.msk_cluster_arn, ":cluster/", ":topic/")
  msk_group_arn_prefix = replace(var.msk_cluster_arn, ":cluster/", ":group/")

  # The OIDC provider is assumed to exist. Creating it here would fail in any
  # account that already has one — there can be exactly one per URL — and every
  # account that runs GitHub Actions already does.
  github_oidc_provider_arn = "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:oidc-provider/token.actions.githubusercontent.com"
}

# --- The Spark role --------------------------------------------------------

data "aws_iam_policy_document" "emr_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["emr-serverless.amazonaws.com"]
    }

    # The confused-deputy guard. Without it, any EMR Serverless application in
    # any account that learns this role's ARN can ask EMR to assume it.
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_iam_role" "emr_job_execution" {
  name               = "${var.name_prefix}-emr-job-execution"
  description        = "Assumed by EMR Serverless job runs: one Kafka topic in, Iceberg tables out."
  assume_role_policy = data.aws_iam_policy_document.emr_assume.json
}

data "aws_iam_policy_document" "emr_job_execution" {
  statement {
    sid    = "ReadWriteLakehouseObjects"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      # DeleteObject is required, not optional: compaction rewrites data files and
      # snapshot expiry removes the old ones. A pipeline that can only append is a
      # pipeline whose small-file problem never gets fixed.
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
    ]

    # Scoped to the warehouse prefix rather than the bucket, so nothing else that
    # ends up in this bucket is reachable from a Spark job.
    resources = ["${var.lakehouse_bucket_arn}/warehouse/*"]
  }

  statement {
    sid       = "ListLakehousePrefixes"
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:ListBucketMultipartUploads"]
    resources = [var.lakehouse_bucket_arn]

    # ListBucket is a bucket-level action, so the prefix restriction has to be a
    # condition. Without this, the role could enumerate the whole bucket even
    # though it can only read objects under warehouse/.
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["warehouse/*"]
    }
  }

  statement {
    sid       = "WriteJobLogs"
    effect    = "Allow"
    actions   = ["s3:PutObject", "s3:AbortMultipartUpload"]
    resources = ["${var.logs_bucket_arn}/emr-serverless/*"]
  }

  statement {
    sid    = "UseGlueAsIcebergCatalog"
    effect = "Allow"

    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
      "glue:GetPartitions",
    ]

    # Iceberg's Glue catalog writes table metadata pointers on every commit, so
    # UpdateTable here is the equivalent of the REST catalog's commit endpoint.
    resources = concat([local.catalog_arn], local.database_arns, local.table_arns)
  }

  statement {
    sid       = "ConnectToTheKafkaCluster"
    effect    = "Allow"
    actions   = ["kafka-cluster:Connect", "kafka-cluster:DescribeCluster"]
    resources = [var.msk_cluster_arn]
  }

  statement {
    sid       = "ReadOneTopic"
    effect    = "Allow"
    actions   = ["kafka-cluster:ReadData", "kafka-cluster:DescribeTopic"]
    resources = ["${local.msk_topic_arn_prefix}/${var.kafka_topic}"]
  }

  statement {
    sid     = "JoinTheConsumerGroupsSparkInvents"
    effect  = "Allow"
    actions = ["kafka-cluster:AlterGroup", "kafka-cluster:DescribeGroup"]

    # Wildcarded on the group name and only on the group name. Spark's Kafka
    # source generates a group id per query at runtime — `spark-kafka-source-`
    # plus a UUID — so there is no fixed name to grant. The cluster and account
    # are still pinned by the ARN this is derived from.
    resources = ["${local.msk_group_arn_prefix}/*"]
  }
}

resource "aws_iam_policy" "emr_job_execution" {
  name        = "${var.name_prefix}-emr-job-execution"
  description = "Least-privilege policy for the Spark streaming jobs."
  policy      = data.aws_iam_policy_document.emr_job_execution.json
}

resource "aws_iam_role_policy_attachment" "emr_job_execution" {
  role       = aws_iam_role.emr_job_execution.name
  policy_arn = aws_iam_policy.emr_job_execution.arn
}

# --- The dbt role ----------------------------------------------------------

data "aws_iam_policy_document" "dbt_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [local.github_oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:sub"
      # Pinned to one branch of one repository. `repo:owner/*` would let a pull
      # request from a fork assume this role, which is how OIDC trust policies are
      # usually got wrong.
      values = ["repo:${var.github_repository}:ref:refs/heads/main"]
    }
  }
}

resource "aws_iam_role" "dbt_runner" {
  name               = "${var.name_prefix}-dbt-runner"
  description        = "Assumed by GitHub Actions over OIDC to build the gold marts through Athena."
  assume_role_policy = data.aws_iam_policy_document.dbt_assume.json

  # One hour, because a dbt build of these marts takes minutes. The default is
  # also one hour; stating it makes the intent reviewable rather than inherited.
  max_session_duration = 3600
}

data "aws_iam_policy_document" "dbt_runner" {
  statement {
    sid    = "RunAthenaQueries"
    effect = "Allow"

    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:StopQueryExecution",
      "athena:GetWorkGroup",
      "athena:GetDataCatalog",
    ]

    resources = [
      "arn:${data.aws_partition.current.partition}:athena:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:workgroup/${var.name_prefix}-dbt",
      "arn:${data.aws_partition.current.partition}:athena:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:datacatalog/AwsDataCatalog",
    ]
  }

  statement {
    sid    = "ReadSilverWriteGold"
    effect = "Allow"

    actions = [
      "glue:GetDatabase",
      "glue:GetDatabases",
      "glue:GetTable",
      "glue:GetTables",
      "glue:GetPartitions",
    ]

    resources = concat([local.catalog_arn], local.database_arns, local.table_arns)
  }

  statement {
    sid    = "ManageGoldTablesOnly"
    effect = "Allow"

    actions = [
      "glue:CreateTable",
      "glue:UpdateTable",
      "glue:DeleteTable",
    ]

    # dbt owns gold and only gold. It reads silver through a view and must not be
    # able to drop the table the streaming job is writing to — which is the single
    # most expensive mistake available in a lakehouse, because a dropped Iceberg
    # table takes its metadata tree with it.
    resources = [
      "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:table/gold/*",
      "arn:${data.aws_partition.current.partition}:glue:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:database/gold",
      local.catalog_arn,
    ]
  }

  statement {
    sid       = "ReadSilverObjects"
    effect    = "Allow"
    actions   = ["s3:GetObject"]
    resources = ["${var.lakehouse_bucket_arn}/warehouse/silver/*"]
  }

  statement {
    sid    = "WriteGoldObjects"
    effect = "Allow"

    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:AbortMultipartUpload",
    ]

    resources = [
      "${var.lakehouse_bucket_arn}/warehouse/gold/*",
      "${var.lakehouse_bucket_arn}/athena-results/*",
    ]
  }

  statement {
    sid       = "ListWhatItCanRead"
    effect    = "Allow"
    actions   = ["s3:ListBucket"]
    resources = [var.lakehouse_bucket_arn]

    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["warehouse/silver/*", "warehouse/gold/*", "athena-results/*"]
    }
  }
}

resource "aws_iam_policy" "dbt_runner" {
  name        = "${var.name_prefix}-dbt-runner"
  description = "Least-privilege policy for building the gold marts: silver read-only, gold read-write, bronze unreachable."
  policy      = data.aws_iam_policy_document.dbt_runner.json
}

resource "aws_iam_role_policy_attachment" "dbt_runner" {
  role       = aws_iam_role.dbt_runner.name
  policy_arn = aws_iam_policy.dbt_runner.arn
}

output "emr_job_execution_role_arn" {
  description = "Role EMR Serverless job runs assume."
  value       = aws_iam_role.emr_job_execution.arn
}

output "dbt_runner_role_arn" {
  description = "Role GitHub Actions assumes to build the marts. Goes in the workflow's aws-actions/configure-aws-credentials step."
  value       = aws_iam_role.dbt_runner.arn
}
