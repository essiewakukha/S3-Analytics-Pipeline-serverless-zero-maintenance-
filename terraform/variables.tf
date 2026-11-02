variable "aws_region" {
  description = "Region to deploy into. Metric streams are regional."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Prefix for every resource name."
  type        = string
  default     = "cw-metrics-pipeline"
}

variable "include_namespaces" {
  description = <<-EOT
    CloudWatch namespaces to stream. Filtering is at NAMESPACE level only, so any
    metric added to these namespaces later is streamed automatically.
    Set to [] to stream every namespace in the account/region.
  EOT
  type        = list(string)
  default     = ["PortfolioDemo/Services", "AWS/Lambda", "AWS/ApplicationELB"]
}

variable "service_dimension_keys" {
  description = "Dimension names (in priority order) the transform uses to derive the owning service."
  type        = list(string)
  default     = ["Service", "ServiceName", "FunctionName", "LoadBalancer", "ClusterName"]
}

variable "firehose_buffer_seconds" {
  description = "How long Firehose buffers before writing to S3 (60-900). Lower = fresher, more objects."
  type        = number
  default     = 60
}

variable "alarm_sns_topic_arn" {
  description = "Optional SNS topic for pipeline-health alarms. Leave empty for no notifications."
  type        = string
  default     = ""
}

variable "force_destroy_bucket" {
  description = "Allow `terraform destroy` to delete a non-empty bucket. Convenient for demos; disable for real data."
  type        = bool
  default     = true
}