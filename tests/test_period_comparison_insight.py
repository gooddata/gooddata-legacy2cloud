# (C) 2026 GoodData Corporation
"""Unit tests for PeriodComparisonInsight.create_insight_object_from_kpi.

Covers the edge case where a Platform KPI has comparisonType != "none" but no
widget-level dateDataSet. Cloud headline insights require a date dataset for
period comparison, so the comparison must be dropped during migration.
"""

import logging

import pytest

from gooddata_platform2cloud.insights.period_comparison_insight import (
    PeriodComparisonInsight,
)


def _make_kpi(comparison_type: str, with_date_dataset: bool) -> dict:
    kpi: dict = {
        "kpi": {
            "content": {
                "metric": "/gdc/md/proj/obj/1",
                "comparisonType": comparison_type,
            },
            "meta": {
                "title": "Revenue KPI",
                "summary": "Revenue KPI description",
            },
        }
    }
    if with_date_dataset:
        kpi["kpi"]["content"]["dateDataSet"] = "/gdc/md/proj/obj/date_ds"
    return kpi


def _make_ctx(mocker):
    ctx = mocker.MagicMock()

    def get_object(uri: str) -> dict:
        if uri == "/gdc/md/proj/obj/1":
            return {"metric": {"meta": {"identifier": "metric.revenue"}}}
        if uri == "/gdc/md/proj/obj/date_ds":
            return {
                "dataSet": {
                    "content": {"identifierPrefix": "date_prefix"},
                    "meta": {"category": "DataSet"},
                }
            }
        raise KeyError(uri)

    ctx.platform_client.get_object.side_effect = get_object
    ctx.metric_mappings.search_mapping_identifier.return_value = "cloud.metric.revenue"
    ctx.ldm_mappings.search_mapping_identifier.return_value = "cloud.date_ds"
    return ctx


def _build(kpi: dict, mocker) -> dict | None:
    inst = PeriodComparisonInsight(
        ctx=_make_ctx(mocker),
        platform_definition=kpi,
        new_insight_id="new-insight-id",
        cloud_filters=[],
    )
    return inst.create_insight_object_from_kpi()


@pytest.mark.parametrize("comparison_type", ["previousPeriod", "lastYear"])
def test_drop_comparison_when_no_date_dataset(
    comparison_type: str, caplog: pytest.LogCaptureFixture, mocker
) -> None:
    kpi = _make_kpi(comparison_type, with_date_dataset=False)

    with caplog.at_level(logging.WARNING, logger="migration"):
        result = _build(kpi, mocker)

    assert result is not None
    content = result["data"]["attributes"]["content"]

    # Only the primary measure bucket — no secondary comparison bucket.
    assert len(content["buckets"]) == 1
    assert content["buckets"][0]["localIdentifier"] == "measures"

    # Comparison disabled in properties.
    assert content["properties"]["controls"]["comparison"] == {"enabled": False}

    # Warning logged naming the KPI and the original comparison type.
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "Revenue KPI" in warnings[0].getMessage()
    assert comparison_type in warnings[0].getMessage()


def test_previous_period_with_date_dataset_keeps_comparison(
    caplog: pytest.LogCaptureFixture, mocker
) -> None:
    kpi = _make_kpi("previousPeriod", with_date_dataset=True)

    with caplog.at_level(logging.WARNING, logger="migration"):
        result = _build(kpi, mocker)

    assert result is not None
    content = result["data"]["attributes"]["content"]

    # Primary + secondary measure buckets.
    assert len(content["buckets"]) == 2
    assert content["buckets"][1]["localIdentifier"] == "secondary_measures"

    # Comparison enabled in properties.
    assert content["properties"]["controls"]["comparison"]["enabled"] is True

    # No drop warning.
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings == []


def test_comparison_none_without_date_dataset_no_warning(
    caplog: pytest.LogCaptureFixture, mocker
) -> None:
    kpi = _make_kpi("none", with_date_dataset=False)

    with caplog.at_level(logging.WARNING, logger="migration"):
        result = _build(kpi, mocker)

    assert result is not None
    content = result["data"]["attributes"]["content"]

    assert len(content["buckets"]) == 1
    assert content["properties"]["controls"]["comparison"] == {"enabled": False}

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings == []
