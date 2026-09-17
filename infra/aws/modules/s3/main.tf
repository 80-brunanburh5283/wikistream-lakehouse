# Two buckets: the lakehouse itself, and one for EMR's logs and S3's own access
# logs.
#
# The lifecycle rules are the part of this module worth reading. An Iceberg table
# is not a directory of files that age out — it is a set of files a metadata tree
# still points at — so the usual "expire objects after N days" rule is a way to
# turn a working table into a query that fails with FileNotFoundException. What
# is safe, and what is not, is written on each rule below.
#
# The controls are written out once per bucket rather than looped with `for_each`
# over a map of bucket ids. The loop was the first version and it was shorter, and
# `trivy config` reported both buckets as having no public access block, no
# versioning and no encryption — because it cannot follow `bucket = each.value`
# back to the resource it names. A control a static analyser cannot see is a
# control the next reviewer has to verify by hand, and the duplication is eight
# lines a piece.

variable "name_prefix" {
  description = "Prefix for bucket names. Buckets are globally named, so the account id is appended."
  type        = string
}

variable "bronze_transition_days" {
  description = "Age at which bronze data files move to Standard-IA."
  type        = number
}

variable "bronze_retention_days" {
  description = "Backstop expiry for bronze data files. 0 disables the rule."
  type        = number
}

data "aws_caller_identity" "current" {}

locals {
  # S3 bucket names are global, so a prefix alone collides with anyone else who
  # picked the same project name. The account id is not a secret and it makes the
  # name deterministic, which `terraform import` needs and a random suffix breaks.
  lakehouse_bucket = "${var.name_prefix}-lakehouse-${data.aws_caller_identity.current.account_id}"
  logs_bucket      = "${var.name_prefix}-logs-${data.aws_caller_identity.current.account_id}"
}

# --- The lakehouse bucket --------------------------------------------------

resource "aws_s3_bucket" "lakehouse" {
  bucket = local.lakehouse_bucket

  tags = { Name = local.lakehouse_bucket }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    # ACLs off entirely. Nothing in this design grants cross-account access, and
    # an ACL is the one permission model that does not show up in an IAM policy
    # review.
    object_ownership = "BucketOwnerEnforced"
  }
}

# trivy:ignore:AVD-AWS-0132 Deliberate: SSE-KMS bills a KMS request per object, and this table is public data written in small files.
resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    apply_server_side_encryption_by_default {
      # AES256, not aws:kms, and this is the one control here that a scanner
      # objects to. A streaming Iceberg table writes a lot of small objects — one
      # data file and several metadata files per commit, every 30 seconds — and
      # SSE-KMS charges a KMS request per object write and per read. The data is
      # public Wikimedia activity, so a customer-managed key buys a key policy and
      # an audit trail for data that is already public, and charges per file for
      # ever. If this table held anything private the answer would flip, and
      # docs/cost.md shows the arithmetic.
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  versioning_configuration {
    # Versioning is here as an undo button for a mistaken `DELETE FROM` or a
    # botched compaction, not as an archive — see the noncurrent expiry below.
    status = "Enabled"
  }
}

resource "aws_s3_bucket_logging" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  # Server access logging is free to deliver and costs only the storage of the
  # logs, which the log bucket's lifecycle rule caps at 30 days. It is also the
  # only record of who read these objects that does not require CloudTrail data
  # events, which are billed per request and would cost more than the lakehouse.
  target_bucket = aws_s3_bucket.logs.id
  target_prefix = "s3-access-logs/lakehouse/"
}

data "aws_iam_policy_document" "lakehouse_deny_insecure_transport" {
  statement {
    sid    = "DenyUnencryptedTransport"
    effect = "Deny"

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.lakehouse.arn,
      "${aws_s3_bucket.lakehouse.arn}/*",
    ]

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  policy = data.aws_iam_policy_document.lakehouse_deny_insecure_transport.json
}

# --- The log bucket --------------------------------------------------------

# trivy:ignore:AVD-AWS-0089 A log bucket that logs its own access logs its own log writes, for ever.
resource "aws_s3_bucket" "logs" {
  bucket = local.logs_bucket

  tags = { Name = local.logs_bucket }
}

resource "aws_s3_bucket_public_access_block" "logs" {
  bucket                  = aws_s3_bucket.logs.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "logs" {
  bucket = aws_s3_bucket.logs.id

  rule {
    # BucketOwnerEnforced is also what lets S3's logging service deliver here
    # without an ACL grant: modern access logging uses a bucket policy instead.
    object_ownership = "BucketOwnerEnforced"
  }
}

# trivy:ignore:AVD-AWS-0132 Same decision as the lakehouse bucket above.
resource "aws_s3_bucket_server_side_encryption_configuration" "logs" {
  bucket = aws_s3_bucket.logs.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "logs" {
  bucket = aws_s3_bucket.logs.id

  versioning_configuration {
    status = "Enabled"
  }
}

data "aws_iam_policy_document" "logs_bucket" {
  statement {
    sid    = "DenyUnencryptedTransport"
    effect = "Deny"

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.logs.arn,
      "${aws_s3_bucket.logs.arn}/*",
    ]

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }

  statement {
    sid    = "AllowServerAccessLogDelivery"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["logging.s3.amazonaws.com"]
    }

    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.logs.arn}/s3-access-logs/*"]

    # Both conditions are the confused-deputy guard for log delivery: without
    # them, S3 in any account could be pointed at this prefix.
    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = [aws_s3_bucket.lakehouse.arn]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_s3_bucket_policy" "logs" {
  bucket = aws_s3_bucket.logs.id
  policy = data.aws_iam_policy_document.logs_bucket.json
}

# --- Lifecycle -------------------------------------------------------------

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  # Small objects cost more to transition than the transition saves. 128 KB is
  # S3's own break-even and, since Iceberg metadata files are all well under it,
  # this also stops the transition rules touching metadata even by accident.
  transition_default_minimum_object_size = "all_storage_classes_128K"

  rule {
    id     = "abort-incomplete-multipart-uploads"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      # A Spark executor killed mid-write leaves the parts behind, and they are
      # billed as storage while being invisible to every `ls`. This is the single
      # highest-value lifecycle rule on any bucket Spark writes to.
      days_after_initiation = 7
    }
  }

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      # Versioning is the undo button; 30 days is how long an undo is useful.
      # Without this rule every compaction — which rewrites files by design —
      # leaves its inputs behind for ever and storage grows without bound.
      noncurrent_days = 30
    }
  }

  rule {
    id     = "bronze-data-to-infrequent-access"
    status = "Enabled"

    filter {
      # Data files only. Transitioning `metadata/` would put the manifests that
      # every query plan reads into a storage class with a higher per-request
      # price, so planning would get slower and more expensive to save a few
      # cents on files measured in kilobytes.
      prefix = "warehouse/bronze/data/"
    }

    transition {
      days          = var.bronze_transition_days
      storage_class = "STANDARD_IA"
    }
  }

  # Off by default, and the default is the recommendation. S3 expiry works on
  # object age; Iceberg's own retention works on snapshots and rows. If this rule
  # deletes a file the current snapshot still references, every query against the
  # table fails until the file is restored from a noncurrent version — which is
  # why the two must be enabled together, table retention first. The pairing is
  # written out in docs/runbook.md under "Bronze is growing without bound".
  dynamic "rule" {
    for_each = var.bronze_retention_days > 0 ? [var.bronze_retention_days] : []

    content {
      id     = "bronze-data-backstop-expiry"
      status = "Enabled"

      filter {
        prefix = "warehouse/bronze/data/"
      }

      expiration {
        days = rule.value
      }
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "logs" {
  bucket = aws_s3_bucket.logs.id

  transition_default_minimum_object_size = "all_storage_classes_128K"

  rule {
    id     = "expire-logs"
    status = "Enabled"

    filter {}

    expiration {
      # Long enough to debug last week's incident, short enough that nobody has
      # to explain a log bill.
      days = 30
    }

    noncurrent_version_expiration {
      noncurrent_days = 7
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

output "lakehouse_bucket_id" {
  description = "Name of the lakehouse bucket, which is also the Iceberg warehouse root."
  value       = aws_s3_bucket.lakehouse.id
}

output "lakehouse_bucket_arn" {
  description = "ARN of the lakehouse bucket, for the EMR job execution policy."
  value       = aws_s3_bucket.lakehouse.arn
}

output "logs_bucket_arn" {
  description = "ARN of the log bucket, for the EMR job execution policy."
  value       = aws_s3_bucket.logs.arn
}
