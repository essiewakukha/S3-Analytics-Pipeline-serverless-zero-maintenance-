data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  name        = var.project_name
  account_id  = data.aws_caller_identity.current.account_id
  bucket_name = "${var.project_name}-${local.account_id}-${data.aws_region.current.name}"
}

# ---------------------------------------------------------------------------
# S3: destination for transformed metrics (plus Firehose errors and Athena results)
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "metrics" {
  bucket        = local.bucket_name
  force_destroy = var.force_destroy_bucket
}

resource "aws_s3_bucket_public_access_block" "metrics" {
  bucket                  = aws_s3_bucket.metrics.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "metrics" {
  bucket = aws_s3_bucket.metrics.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "metrics" {
  bucket = aws_s3_bucket.metrics.id

  rule {
    id     = "tier-metrics"
    status = "Enabled"
    filter { prefix = "metrics/" }
    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }
  }

  rule {
    id     = "expire-errors"
    status = "Enabled"
    filter { prefix = "errors/" }
    expiration { days = 30 }
  }

  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    filter { prefix = "athena-results/" }
    expiration { days = 7 }
  }
}

# ---------------------------------------------------------------------------
# Lambda: Firehose record transformation
# ---------------------------------------------------------------------------
data "archive_file" "transform" {
  type        = "zip"
  source_file = "${path.module}/../lambda/transform.py"
  output_path = "${path.module}/build/transform.zip"
}

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "transform_lambda" {
  name               = "${local.name}-transform-lambda"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "transform_logs" {
  role       = aws_iam_role.transform_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_cloudwatch_log_group" "transform" {
  name              = "/aws/lambda/${local.name}-transform"
  retention_in_days = 14
}

resource "aws_lambda_function" "transform" {
  function_name    = "${local.name}-transform"
  role             = aws_iam_role.transform_lambda.arn
  runtime          = "python3.12"
  handler          = "transform.handler"
  filename         = data.archive_file.transform.output_path
  source_code_hash = data.archive_file.transform.output_base64sha256
  timeout          = 60
  memory_size      = 256

  environment {
    variables = {
      SERVICE_DIMENSION_KEYS = join(",", var.service_dimension_keys)
      LOG_LEVEL              = "INFO"
    }
  }

  depends_on = [aws_cloudwatch_log_group.transform, aws_iam_role_policy_attachment.transform_logs]
}

# ---------------------------------------------------------------------------
# Firehose: buffers, transforms (via Lambda), compresses and writes to S3
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_log_group" "firehose" {
  name              = "/aws/kinesisfirehose/${local.name}-delivery"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_stream" "firehose_s3" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}

data "aws_iam_policy_document" "firehose_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [local.account_id]
    }
  }
}

resource "aws_iam_role" "firehose" {
  name               = "${local.name}-firehose"
  assume_role_policy = data.aws_iam_policy_document.firehose_assume.json
}

data "aws_iam_policy_document" "firehose" {
  statement {
    sid = "WriteToBucket"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:PutObject",
    ]
    resources = [aws_s3_bucket.metrics.arn, "${aws_s3_bucket.metrics.arn}/*"]
  }

  statement {
    sid       = "InvokeTransform"
    actions   = ["lambda:InvokeFunction", "lambda:GetFunctionConfiguration"]
    resources = [aws_lambda_function.transform.arn, "${aws_lambda_function.transform.arn}:*"]
  }

  statement {
    sid       = "DeliveryLogs"
    actions   = ["logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.firehose.arn}:*"]
  }
}

resource "aws_iam_role_policy" "firehose" {
  name   = "firehose-delivery"
  role   = aws_iam_role.firehose.id
  policy = data.aws_iam_policy_document.firehose.json
}

resource "aws_kinesis_firehose_delivery_stream" "metrics" {
  name        = "${local.name}-delivery"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose.arn
    bucket_arn = aws_s3_bucket.metrics.arn

    # Hive-style partitions so Athena partition projection can prune by time.
    prefix              = "metrics/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
    error_output_prefix = "errors/!{firehose:error-output-type}/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/"

    buffering_size     = 64
    buffering_interval = var.firehose_buffer_seconds
    compression_format = "GZIP"

    processing_configuration {
      enabled = true
      processors {
        type = "Lambda"
        parameters {
          parameter_name  = "LambdaArn"
          parameter_value = "${aws_lambda_function.transform.arn}:$LATEST"
        }
        parameters {
          parameter_name  = "BufferSizeInMBs"
          parameter_value = "1"
        }
        parameters {
          parameter_name  = "BufferIntervalInSeconds"
          parameter_value = "60"
        }
      }
    }

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = aws_cloudwatch_log_stream.firehose_s3.name
    }
  }

  depends_on = [aws_iam_role_policy.firehose]
}

# ---------------------------------------------------------------------------
# CloudWatch Metric Stream: continuous, push-based export of metrics
# ---------------------------------------------------------------------------
data "aws_iam_policy_document" "metric_stream_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["streams.metrics.cloudwatch.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }
  }
}

resource "aws_iam_role" "metric_stream" {
  name               = "${local.name}-metric-stream"
  assume_role_policy = data.aws_iam_policy_document.metric_stream_assume.json
}

data "aws_iam_policy_document" "metric_stream" {
  statement {
    actions   = ["firehose:PutRecord", "firehose:PutRecordBatch"]
    resources = [aws_kinesis_firehose_delivery_stream.metrics.arn]
  }
}

resource "aws_iam_role_policy" "metric_stream" {
  name   = "put-to-firehose"
  role   = aws_iam_role.metric_stream.id
  policy = data.aws_iam_policy_document.metric_stream.json
}

resource "aws_cloudwatch_metric_stream" "this" {
  name          = "${local.name}-stream"
  role_arn      = aws_iam_role.metric_stream.arn
  firehose_arn  = aws_kinesis_firehose_delivery_stream.metrics.arn
  output_format = "json"

  # Namespace-level filters only (no metric_names), so NEW metrics in these
  # namespaces are included automatically. Empty list => stream everything.
  dynamic "include_filter" {
    for_each = toset(var.include_namespaces)
    content {
      namespace = include_filter.value
    }
  }

  depends_on = [aws_iam_role_policy.metric_stream]
}