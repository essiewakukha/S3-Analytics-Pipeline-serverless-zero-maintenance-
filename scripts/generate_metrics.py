#!/usr/bin/env python3
"""
Publish realistic custom application metrics for a few demo services.

Use --new-metric to also publish a metric with a never-seen-before name. It
will show up in S3/Athena a few minutes later with NO change to the metric
stream, Firehose, Lambda or Glue table - the core requirement of the project.

    python scripts/generate_metrics.py --minutes 10
    python scripts/generate_metrics.py --minutes 5 --new-metric
"""

import argparse
import random
import time
import uuid

import boto3

SERVICES = {
    # service: (base latency ms, requests per tick, error probability)
    "checkout": (120, 40, 0.02),
    "search": (45, 120, 0.005),
    "payments": (210, 25, 0.04),
}


def build_metric_data(service: str, extra_metric: str | None) -> list[dict]:
    base_latency, base_requests, error_rate = SERVICES[service]
    requests = max(1, int(random.gauss(base_requests, base_requests * 0.15)))
    errors = sum(random.random() < error_rate for _ in range(requests))
    dims = [{"Name": "Service", "Value": service}]

    data = [
        {
            "MetricName": "Latency",
            "Dimensions": dims,
            "Values": [max(1.0, random.gauss(base_latency, base_latency * 0.25)) for _ in range(10)],
            "Unit": "Milliseconds",
        },
        {"MetricName": "RequestCount", "Dimensions": dims, "Value": requests, "Unit": "Count"},
        {"MetricName": "ErrorCount", "Dimensions": dims, "Value": errors, "Unit": "Count"},
    ]
    if extra_metric:
        data.append({"MetricName": extra_metric, "Dimensions": dims, "Value": random.randint(1, 100), "Unit": "Count"})
    return data


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--namespace", default="PortfolioDemo/Services")
    parser.add_argument("--region", default=None, help="Defaults to your AWS CLI/profile region.")
    parser.add_argument("--minutes", type=float, default=5)
    parser.add_argument("--interval", type=int, default=15, help="Seconds between publishes.")
    parser.add_argument("--new-metric", action="store_true", help="Also publish a brand-new metric name.")
    args = parser.parse_args()

    cloudwatch = boto3.client("cloudwatch", region_name=args.region)
    extra = f"Experiment_{uuid.uuid4().hex[:6]}" if args.new_metric else None
    if extra:
        print(f"Publishing NEW metric '{extra}' - look for it in Athena in ~3-5 minutes.")

    deadline = time.time() + args.minutes * 60
    ticks = 0
    while time.time() < deadline:
        for service in SERVICES:
            cloudwatch.put_metric_data(Namespace=args.namespace, MetricData=build_metric_data(service, extra))
        ticks += 1
        print(f"tick {ticks}: published metrics for {', '.join(SERVICES)}")
        time.sleep(args.interval)

    print("Done.")


if __name__ == "__main__":
    main()