# (C) 2026 GoodData Corporation
"""Unit tests for PeriodComparisonInsight.create_insight_object_from_kpi.

Covers the edge case where a Legacy KPI has comparisonType != "none" but no
widget-level dateDataSet. Cloud headline insights require a date dataset for
period comparison, so the comparison must be dropped during migration.
"""

import logging

import pytest

from gooddata_legacy2cloud.insights.period_comparison_insight import (
    PeriodComparisonInsight,
)


def _make_kpi(
    comparison_type: str,
    with_date_dataset: bool,
    summary: str = "Revenue KPI description",
    description_config: dict | None = None,
) -> dict:
    kpi: dict = {
        "kpi": {
            "content": {
                "metric": "/gdc/md/proj/obj/1",
                "comparisonType": comparison_type,
            },
            "meta": {
                "title": "Revenue KPI",
                "summary": summary,
            },
        }
    }
    if with_date_dataset:
        kpi["kpi"]["content"]["dateDataSet"] = "/gdc/md/proj/obj/date_ds"
    if description_config is not None:
        kpi["kpi"]["content"]["configuration"] = {"description": description_config}
    return kpi


def _make_ctx(mocker, metric_summary: str = ""):
    ctx = mocker.MagicMock()

    def get_object(uri: str) -> dict:
        if uri == "/gdc/md/proj/obj/1":
            return {
                "metric": {
                    "meta": {
                        "identifier": "metric.revenue",
                        "summary": metric_summary,
                    }
                }
            }
        if uri == "/gdc/md/proj/obj/date_ds":
            return {
                "dataSet": {
                    "content": {"identifierPrefix": "date_prefix"},
                    "meta": {"category": "DataSet"},
                }
            }
        raise KeyError(uri)

    ctx.legacy_client.get_object.side_effect = get_object
    ctx.metric_mappings.search_mapping_identifier.return_value = "cloud.metric.revenue"
    ctx.ldm_mappings.search_mapping_identifier.return_value = "cloud.date_ds"
    return ctx


def _build(kpi: dict, mocker, metric_summary: str = "") -> dict | None:
    inst = PeriodComparisonInsight(
        ctx=_make_ctx(mocker, metric_summary=metric_summary),
        legacy_definition=kpi,
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


def test_missing_metric_raises_value_error(mocker) -> None:
    """When the KPI's metric was not migrated, the metric mapping lookup raises
    ValueError. The dashboard migration relies on this to substitute a rich text
    placeholder instead of skipping the whole dashboard (SVS-1333)."""
    kpi = _make_kpi("none", with_date_dataset=False)
    ctx = _make_ctx(mocker)
    ctx.metric_mappings.search_mapping_identifier.side_effect = ValueError(
        "Search Cloud Id - Unknown Cloud identifier metric.revenue"
    )
    insight = PeriodComparisonInsight(
        ctx=ctx,
        legacy_definition=kpi,
        new_insight_id="new-insight-id",
        cloud_filters=[],
    )

    with pytest.raises(ValueError):
        insight.get()


def test_custom_description_uses_kpi_summary(mocker) -> None:
    """description source=kpi, visible=true -> kpi.meta.summary is used as-is."""
    kpi = _make_kpi(
        "none",
        with_date_dataset=False,
        summary="Custom KPI description",
        description_config={"source": "kpi", "visible": True},
    )

    result = _build(kpi, mocker, metric_summary="Metric description")

    assert result is not None
    assert result["data"]["attributes"]["description"] == "Custom KPI description"


def test_explicit_inherited_description_uses_metric_summary(mocker) -> None:
    """description source=metric, visible=true -> fetch the metric's own summary."""
    kpi = _make_kpi(
        "none",
        with_date_dataset=False,
        summary="",
        description_config={"source": "metric", "visible": True},
    )

    result = _build(kpi, mocker, metric_summary="Metric description")

    assert result is not None
    assert result["data"]["attributes"]["description"] == "Metric description"


def test_implicit_inherited_description_uses_metric_summary(mocker) -> None:
    """Absent kpi.content.configuration.description defaults to inherited-from-metric."""
    kpi = _make_kpi(
        "none",
        with_date_dataset=False,
        summary="",
        description_config=None,
    )

    result = _build(kpi, mocker, metric_summary="Metric description")

    assert result is not None
    assert result["data"]["attributes"]["description"] == "Metric description"


def test_no_description_when_not_visible(mocker) -> None:
    """description visible=false -> description is empty regardless of source."""
    kpi = _make_kpi(
        "none",
        with_date_dataset=False,
        summary="",
        description_config={"source": "metric", "visible": False},
    )

    result = _build(kpi, mocker, metric_summary="Metric description")

    assert result is not None
    assert result["data"]["attributes"]["description"] == ""
