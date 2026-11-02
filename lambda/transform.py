"""
Amazon Data Firehose transformation Lambda for CloudWatch Metric Stream records.

Input: Firehose batches whose `data` field is base64-encoded, newline-delimited
JSON written by a CloudWatch Metric Stream (output_format = "json").
Each line looks like:

    {"metric_stream_name": "...", "account_id": "...", "region": "us-east-1",
     "namespace": "PortfolioDemo/Services", "metric_name": "Latency",
     "dimensions": {"Service": "checkout"}, "timestamp": 1730534400000,
     "value": {"max": 210.0, "min": 12.0, "sum": 1480.0, "count": 12.0},
     "unit": "Milliseconds"}

Output: one flat, analytics-friendly JSON object per metric line, with derived
fields (service, ISO event time, average, percentiles), ready for Athena.

Design notes
- The function never filters by metric name. Whatever the metric stream sends
  is transformed, so new metrics flow through with zero code or config change.
- A bad line is skipped and logged; the good lines in the same record are kept.
- If no line in a record can be parsed, the record is marked ProcessingFailed so
  Firehose writes the original payload to the errors/ prefix (no silent loss).
"""

import base64
import json
import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# Dimension keys checked (in order) to decide which service a metric belongs to.
SERVICE_DIMENSION_KEYS = tuple(
    key.strip()
    for key in os.environ.get(
        "SERVICE_DIMENSION_KEYS",
        "Service,ServiceName,FunctionName,LoadBalancer,ClusterName",
    ).split(",")
    if key.strip()
)

BASE_STATS = ("min", "max", "sum", "count")


def derive_service(namespace: str, dimensions: dict) -> str:
    """Return the owning service: first matching dimension, else the namespace."""
    for key in SERVICE_DIMENSION_KEYS:
        if dimensions.get(key):
            return dimensions[key]
    return namespace


def transform_metric(metric: dict) -> dict:
    """Flatten one metric-stream JSON object into an analytics record."""
    value = metric["value"]
    if not isinstance(value, dict):
        raise TypeError("value must be an object")

    timestamp_ms = int(metric["timestamp"])
    dimensions = {str(k): str(v) for k, v in (metric.get("dimensions") or {}).items()}

    count = value.get("count")
    total = value.get("sum")
    average = (total / count) if (count and total is not None) else None

    # Any extra statistics configured on the stream (p90, p99, tm99 ...).
    percentiles = {k: v for k, v in value.items() if k not in BASE_STATS}

    return {
        "metric_stream_name": metric.get("metric_stream_name"),
        "account_id": metric.get("account_id"),
        "region": metric.get("region"),
        "namespace": metric["namespace"],
        "metric_name": metric["metric_name"],
        "service": derive_service(metric["namespace"], dimensions),
        "dimensions": dimensions,
        "unit": metric.get("unit"),
        "epoch_ms": timestamp_ms,
        "event_time": datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z"),
        "stat_min": value.get("min"),
        "stat_max": value.get("max"),
        "stat_sum": total,
        "sample_count": count,
        "stat_avg": average,
        "percentiles": percentiles,
    }


def _encode(text: str) -> str:
    return base64.b64encode(text.encode("utf-8")).decode("utf-8")


def handler(event, context):  # noqa: ARG001 - Lambda signature
    output = []
    stats = {"records": 0, "lines_in": 0, "lines_out": 0, "lines_bad": 0, "records_failed": 0}

    for record in event.get("records", []):
        stats["records"] += 1
        record_id = record["recordId"]

        try:
            payload = base64.b64decode(record["data"]).decode("utf-8")
        except (ValueError, UnicodeDecodeError):
            stats["records_failed"] += 1
            output.append({"recordId": record_id, "result": "ProcessingFailed", "data": record["data"]})
            continue

        transformed = []
        record_lines = 0
        for line in payload.splitlines():
            line = line.strip()
            if not line:
                continue
            record_lines += 1
            stats["lines_in"] += 1
            try:
                transformed.append(json.dumps(transform_metric(json.loads(line)), separators=(",", ":")))
            except (ValueError, KeyError, TypeError, AttributeError) as exc:
                stats["lines_bad"] += 1
                logger.warning("Skipping malformed metric line in %s: %s", record_id, exc)

        if transformed:
            stats["lines_out"] += len(transformed)
            output.append(
                {"recordId": record_id, "result": "Ok", "data": _encode("\n".join(transformed) + "\n")}
            )
        elif record_lines:
            # Had content but nothing parseable: send original to errors/ prefix.
            stats["records_failed"] += 1
            output.append({"recordId": record_id, "result": "ProcessingFailed", "data": record["data"]})
        else:
            output.append({"recordId": record_id, "result": "Dropped", "data": record["data"]})

    logger.info(json.dumps({"transform_stats": stats}))
    return {"records": output}