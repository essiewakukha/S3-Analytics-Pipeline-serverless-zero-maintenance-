# ---------------------------------------------------------------------------
# Glue Data Catalog + Athena: query the transformed metrics with SQL.
# Partition projection means no crawlers and no MSCK REPAIR - new hourly
# partitions are queryable the moment Firehose writes them.
# ---------------------------------------------------------------------------
resource "aws_glue_catalog_database" "metrics" {
  name = replace("${local.name}_db", "-", "_")
}

resource "aws_glue_catalog_table" "metrics" {
  name          = "metrics"
  database_name = aws_glue_catalog_database.metrics.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"            = "json"
    "compressionType"           = "gzip"
    "projection.enabled"        = "true"
    "projection.year.type"      = "integer"
    "projection.year.range"     = "2024,2099"
    "projection.month.type"     = "integer"
    "projection.month.range"    = "1,12"
    "projection.month.digits"   = "2"
    "projection.day.type"       = "integer"
    "projection.day.range"      = "1,31"
    "projection.day.digits"     = "2"
    "projection.hour.type"      = "integer"
    "projection.hour.range"     = "0,23"
    "projection.hour.digits"    = "2"
    "storage.location.template" = "s3://${aws_s3_bucket.metrics.bucket}/metrics/year=$${year}/month=$${month}/day=$${day}/hour=$${hour}/"
  }

  partition_keys {
    name = "year"
    type = "string"
  }
  partition_keys {
    name = "month"
    type = "string"
  }
  partition_keys {
    name = "day"
    type = "string"
  }
  partition_keys {
    name = "hour"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.metrics.bucket}/metrics/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
      parameters = {
        "ignore.malformed.json" = "true"
      }
    }

    columns {
      name = "metric_stream_name"
      type = "string"
    }
    columns {
      name = "account_id"
      type = "string"
    }
    columns {
      name = "region"
      type = "string"
    }
    columns {
      name = "namespace"
      type = "string"
    }
    columns {
      name = "metric_name"
      type = "string"
    }
    columns {
      name = "service"
      type = "string"
    }
    columns {
      name = "dimensions"
      type = "map<string,string>"
    }
    columns {
      name = "unit"
      type = "string"
    }
    columns {
      name = "epoch_ms"
      type = "bigint"
    }
    columns {
      name = "event_time"
      type = "string"
    }
    columns {
      name = "stat_min"
      type = "double"
    }
    columns {
      name = "stat_max"
      type = "double"
    }
    columns {
      name = "stat_sum"
      type = "double"
    }
    columns {
      name = "sample_count"
      type = "double"
    }
    columns {
      name = "stat_avg"
      type = "double"
    }
    columns {
      name = "percentiles"
      type = "map<string,double>"
    }
  }
}

resource "aws_athena_workgroup" "metrics" {
  name          = "${local.name}-wg"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration = true
    result_configuration {
      output_location = "s3://${aws_s3_bucket.metrics.bucket}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}