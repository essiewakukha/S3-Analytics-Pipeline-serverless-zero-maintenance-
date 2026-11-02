-- Run in the Athena workgroup created by Terraform (output: athena_workgroup).
-- Database name defaults to cw_metrics_pipeline_db.
-- Always filter on year/month/day(/hour) so partition projection prunes the scan.

-- 1. Sanity check: newest rows landed in S3 --------------------------------
SELECT namespace, metric_name, service, event_time, stat_avg, unit
FROM cw_metrics_pipeline_db.metrics
WHERE year = date_format(current_date, '%Y')
  AND month = date_format(current_date, '%m')
  AND day = date_format(current_date, '%d')
ORDER BY epoch_ms DESC
LIMIT 20;

-- 2. Proof of auto-inclusion: every metric seen, and when it first appeared.
--    Run generate_metrics.py --new-metric and the Experiment_xxxxxx metric
--    appears here with no pipeline change.
SELECT namespace,
       metric_name,
       min(from_iso8601_timestamp(event_time)) AS first_seen,
       count(*)                                AS datapoints
FROM cw_metrics_pipeline_db.metrics
WHERE year = date_format(current_date, '%Y')
  AND month = date_format(current_date, '%m')
GROUP BY namespace, metric_name
ORDER BY first_seen DESC;

-- 3. Hourly latency per service (weighted average, worst max, p99 if streamed)
SELECT service,
       date_trunc('hour', from_iso8601_timestamp(event_time)) AS hour_utc,
       sum(stat_sum) / nullif(sum(sample_count), 0)           AS avg_latency_ms,
       max(stat_max)                                          AS max_latency_ms,
       max(element_at(percentiles, 'p99'))                    AS p99_latency_ms
FROM cw_metrics_pipeline_db.metrics
WHERE metric_name = 'Latency'
  AND year = date_format(current_date, '%Y')
  AND month = date_format(current_date, '%m')
  AND day = date_format(current_date, '%d')
GROUP BY 1, 2
ORDER BY hour_utc DESC, service;

-- 4. Error rate per service ------------------------------------------------
WITH totals AS (
  SELECT service,
         sum(CASE WHEN metric_name = 'ErrorCount'   THEN stat_sum END) AS errors,
         sum(CASE WHEN metric_name = 'RequestCount' THEN stat_sum END) AS requests
  FROM cw_metrics_pipeline_db.metrics
  WHERE namespace = 'PortfolioDemo/Services'
    AND year = date_format(current_date, '%Y')
    AND month = date_format(current_date, '%m')
    AND day = date_format(current_date, '%d')
  GROUP BY service
)
SELECT service, requests, errors,
       round(100.0 * errors / nullif(requests, 0), 2) AS error_rate_pct
FROM totals
ORDER BY error_rate_pct DESC;

-- 5. AWS-managed metrics flow through the same table (e.g. Lambda duration)
SELECT dimensions['FunctionName'] AS function_name,
       sum(stat_sum) / nullif(sum(sample_count), 0) AS avg_duration_ms,
       max(stat_max) AS max_duration_ms
FROM cw_metrics_pipeline_db.metrics
WHERE namespace = 'AWS/Lambda' AND metric_name = 'Duration'
  AND year = date_format(current_date, '%Y')
  AND month = date_format(current_date, '%m')
  AND day = date_format(current_date, '%d')
GROUP BY 1
ORDER BY avg_duration_ms DESC;