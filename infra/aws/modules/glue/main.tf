# Glue Data Catalog in place of the Iceberg REST catalog container.
#
# One database per namespace, which is exactly what the local stack has — bronze,
# silver, gold — so `lakehouse.silver.edits` becomes `glue_catalog.silver.edits`
# and nothing else in the SQL moves.
#
# Glue rather than a self-hosted REST catalog because everything that reads these
# tables on AWS already speaks Glue: Athena has no other option, EMR ships the
# `GlueCatalog` implementation, and Redshift Spectrum and Lake Formation both build
# on it. Running the REST fixture on Fargate instead would mean operating a
# stateful service whose only job is to be the thing Glue already is. What Glue
# costs is portability — it is the one component here with no local equivalent, so
# the catalog configuration is genuinely different in the two deployments rather
# than differing only by an endpoint. docs/architecture.md spells out the property
# changes.
#
# No table resources. Iceberg registers its own tables through the catalog API on
# first write, and an `aws_glue_catalog_table` declaring the schema would be a
# second, immediately stale copy of the contract that lives in
# docs/data-contracts.md and in the Spark schema.

variable "name_prefix" {
  description = "Prefix used in each database's description, not in its name."
  type        = string
}

variable "namespaces" {
  description = "Iceberg namespaces to create as Glue databases. Names match the local stack."
  type        = list(string)
}

variable "warehouse_uri" {
  description = "s3:// root the tables live under. Recorded as the database location."
  type        = string
}

resource "aws_glue_catalog_database" "this" {
  for_each = toset(var.namespaces)

  name = each.value
  # The database name is the bare namespace so the three-part table identifier is
  # identical in both deployments. The prefix goes in the description instead,
  # which is where someone looking at a shared account will look for an owner.
  description  = "${var.name_prefix} lakehouse: ${each.value} layer, Iceberg tables managed by Spark"
  location_uri = "${var.warehouse_uri}/${each.value}"
}

output "database_names" {
  description = "Glue database names, for the IAM policy that scopes catalog access."
  value       = [for database in aws_glue_catalog_database.this : database.name]
}
