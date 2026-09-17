# The outputs are chosen so that the things a person would otherwise copy out of
# the console — the bootstrap string, the warehouse root, the application id — come
# out of `terraform output` instead. Nothing sensitive is here: every value is an
# ARN, a bucket name or a DNS name, none of which is a credential.

output "kafka_bootstrap_servers" {
  description = "Value for WS_KAFKA_BOOTSTRAP_SERVERS. Requires security.protocol=SASL_SSL and the AWS MSK IAM callback handler."
  value       = module.msk.bootstrap_brokers_sasl_iam
}

output "warehouse_uri" {
  description = "Value for the Iceberg catalog warehouse property, replacing s3://lakehouse/warehouse."
  value       = "s3://${module.s3.lakehouse_bucket_id}/warehouse"
}

output "glue_databases" {
  description = "Glue databases standing in for the local REST catalog's namespaces."
  value       = module.glue.database_names
}

output "emr_application_id" {
  description = "EMR Serverless application the streaming jobs are submitted to."
  value       = module.emr_serverless.application_id
}

output "emr_job_execution_role_arn" {
  description = "--execution-role-arn for aws emr-serverless start-job-run."
  value       = module.iam.emr_job_execution_role_arn
}

output "dbt_runner_role_arn" {
  description = "Role for the GitHub Actions job that would build the marts. Keyless, assumed over OIDC."
  value       = module.iam.dbt_runner_role_arn
}
