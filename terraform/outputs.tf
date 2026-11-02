output "bucket_name" {
  description = "S3 bucket holding transformed metrics under metrics/."
  value       = aws_s3_bucket.metrics.bucket
}

output "metric_stream_name" {
  value = aws_cloudwatch_metric_stream.this.name
}

output "firehose_name" {
  value = aws_kinesis_firehose_delivery_stream.metrics.name
}

output "transform_lambda_name" {
  value = aws_lambda_function.transform.function_name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.metrics.name
}

output "glue_database" {
  value = aws_glue_catalog_database.metrics.name
}

output "streamed_namespaces" {
  value = length(var.include_namespaces) == 0 ? ["<all namespaces>"] : var.include_namespaces
}