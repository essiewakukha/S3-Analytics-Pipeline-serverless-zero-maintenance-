import base64
import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "lambda"))

import transform  # noqa: E402

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "sample_metric_stream.jsonl"


def _event(*payloads: str) -> dict:
    return {
        "records": [
            {"recordId": f"r{i}", "data": base64.b64encode(p.encode()).decode()}
            for i, p in enumerate(payloads)
        ]
    }


def _decode(record: dict) -> list[dict]:
    text = base64.b64decode(record["data"]).decode()
    assert text.endswith("\n"), "each Firehose output record must end with a newline"
    return [json.loads(line) for line in text.splitlines()]


def test_transforms_every_line_in_fixture():
    payload = FIXTURE.read_text()
    result = transform.handler(_event(payload), None)

    (record,) = result["records"]
    assert record["result"] == "Ok"
    rows = _decode(record)
    assert len(rows) == len([l for l in payload.splitlines() if l.strip()])


def test_flattens_stats_and_derives_fields():
    payload = FIXTURE.read_text().splitlines()[0]
    (row,) = _decode(transform.handler(_event(payload), None)["records"][0])

    assert row["service"] == "checkout"
    assert row["metric_name"] == "Latency"
    assert row["stat_avg"] == 1480.0 / 12.0
    assert row["event_time"] == "2026-11-02T08:00:00.000Z"
    assert row["percentiles"] == {"p99": 205.5}


def test_new_unknown_metric_passes_through_without_config():
    """The core requirement: a metric nobody has seen before is not filtered out."""
    line = json.dumps(
        {
            "namespace": "PortfolioDemo/Services",
            "metric_name": "Experiment_a1b2c3",
            "dimensions": {"Service": "search"},
            "timestamp": 1793606400000,
            "value": {"min": 1, "max": 1, "sum": 1, "count": 1},
            "unit": "Count",
        }
    )
    (row,) = _decode(transform.handler(_event(line), None)["records"][0])
    assert row["metric_name"] == "Experiment_a1b2c3"


def test_aws_namespace_falls_back_to_dimension_or_namespace():
    lam = json.dumps(
        {"namespace": "AWS/Lambda", "metric_name": "Duration", "dimensions": {"FunctionName": "orders-api"},
         "timestamp": 1793606400000, "value": {"min": 3, "max": 9, "sum": 12, "count": 2}, "unit": "Milliseconds"}
    )
    bare = json.dumps(
        {"namespace": "AWS/Usage", "metric_name": "CallCount", "dimensions": {},
         "timestamp": 1793606400000, "value": {"min": 1, "max": 1, "sum": 1, "count": 1}, "unit": "None"}
    )
    rows = _decode(transform.handler(_event(lam + "\n" + bare), None)["records"][0])
    assert rows[0]["service"] == "orders-api"
    assert rows[1]["service"] == "AWS/Usage"


def test_zero_count_gives_null_average():
    line = json.dumps(
        {"namespace": "X", "metric_name": "Y", "timestamp": 1793606400000,
         "value": {"min": 0, "max": 0, "sum": 0, "count": 0}}
    )
    (row,) = _decode(transform.handler(_event(line), None)["records"][0])
    assert row["stat_avg"] is None


def test_bad_line_is_skipped_good_lines_kept():
    good = FIXTURE.read_text().splitlines()[0]
    result = transform.handler(_event(good + "\n{not json}\n" + good), None)
    (record,) = result["records"]
    assert record["result"] == "Ok"
    assert len(_decode(record)) == 2


def test_fully_unparseable_record_goes_to_error_prefix():
    result = transform.handler(_event("{broken\n{\"namespace\": \"x\"}"), None)
    assert result["records"][0]["result"] == "ProcessingFailed"


def test_empty_record_is_dropped_and_record_ids_preserved():
    result = transform.handler(_event("", FIXTURE.read_text()), None)
    assert [r["recordId"] for r in result["records"]] == ["r0", "r1"]
    assert result["records"][0]["result"] == "Dropped"
    assert result["records"][1]["result"] == "Ok"