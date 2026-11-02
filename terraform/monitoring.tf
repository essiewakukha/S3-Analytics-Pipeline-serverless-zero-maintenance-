# ---------------------------------------------------------------------------
# Pipeline health: know when the export itself breaks.
# ---------------------------------------------------------------------------
locals {
  alarm_actions = var.alarm_sns_topic_arn == "" ? [] : [var.alarm_sns_topic_arn]
}

# Oldest record in Firehose older than 15 minutes => delivery to S3 is stuck.
resource "aws_cloudwatch_metric_alarm" "firehose_freshness" {
  alarm_name          = "${local.name}-firehose-data-freshness"
  alarm_description   = "Metrics are not reaching S3 (Firehose DataFreshness > 15 min)."
  namespace           = "AWS/Firehose"
  metric_name         = "DeliveryToS3.DataFreshness"
  dimensions          = { DeliveryStreamName = aws_kinesis_firehose_delivery_stream.metrics.name }
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 3
  threshold           = 900
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
}

# Transform Lambda failing => records land in errors/ instead of metrics/.
resource "aws_cloudwatch_metric_alarm" "transform_errors" {
  alarm_name          = "${local.name}-transform-errors"
  alarm_description   = "Firehose transformation Lambda is erroring."
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  dimensions          = { FunctionName = aws_lambda_function.transform.function_name }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.alarm_actions
}