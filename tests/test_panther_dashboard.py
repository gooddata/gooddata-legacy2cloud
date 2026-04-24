# (C) 2026 GoodData Corporation
"""
Unit tests for CloudDashboard methods.

These tests focus on isolated unit testing of specific methods,
separate from the integration tests in test_dashboards.py.
"""

import pytest

from gooddata_legacy2cloud.dashboards.cloud_dashboard import CloudDashboard


def test_resolve_widget_type_kpi_widget_returns_kpi(mocker):
    """KPI widget objects should return 'kpi' type."""
    mock_ctx = mocker.MagicMock()

    dashboard = object.__new__(CloudDashboard)
    dashboard.ctx = mock_ctx

    widget_object = {"kpi": {"meta": {"identifier": "some_kpi"}}}

    result = dashboard._resolve_widget_type(widget_object)

    assert result == "kpi"
    mock_ctx.legacy_client.get_object.assert_not_called()


def test_resolve_widget_type_headline_visualization_returns_kpi(mocker):
    """Headline visualization widgets should return 'kpi' type."""
    mock_ctx = mocker.MagicMock()

    mock_ctx.legacy_client.get_object.side_effect = [
        # First call: get visualization object
        {
            "visualizationObject": {
                "content": {"visualizationClass": {"uri": "/gdc/md/ws/obj/123"}}
            }
        },
        # Second call: get visualization class
        {
            "visualizationClass": {
                "content": {},
                "meta": {
                    "identifier": "gdc.visualization.headline",
                    "title": "Headline",
                    "tags": "",
                    "deprecated": "0",
                    "category": "visualizationClass",
                    "isProduction": 1,
                    "created": "2020-01-01 00:00:00",
                    "contributor": "/gdc/account/profile/test",
                    "updated": "2020-01-01 00:00:00",
                    "summary": "",
                    "author": "/gdc/account/profile/test",
                    "uri": "/gdc/md/ws/obj/123",
                },
            }
        },
    ]

    dashboard = object.__new__(CloudDashboard)
    dashboard.ctx = mock_ctx

    widget_object = {
        "visualizationWidget": {"content": {"visualization": "/gdc/md/ws/obj/456"}}
    }

    result = dashboard._resolve_widget_type(widget_object)

    assert result == "kpi"
    assert mock_ctx.legacy_client.get_object.call_count == 2


def test_resolve_widget_type_non_headline_visualization_returns_insight(mocker):
    """Non-headline visualization widgets should return 'insight' type."""
    mock_ctx = mocker.MagicMock()

    mock_ctx.legacy_client.get_object.side_effect = [
        # First call: get visualization object
        {
            "visualizationObject": {
                "content": {"visualizationClass": {"uri": "/gdc/md/ws/obj/123"}}
            }
        },
        # Second call: get visualization class (column chart)
        {
            "visualizationClass": {
                "content": {},
                "meta": {
                    "identifier": "gdc.visualization.column",
                    "title": "Column Chart",
                    "tags": "",
                    "deprecated": "0",
                    "category": "visualizationClass",
                    "isProduction": 1,
                    "created": "2020-01-01 00:00:00",
                    "contributor": "/gdc/account/profile/test",
                    "updated": "2020-01-01 00:00:00",
                    "summary": "",
                    "author": "/gdc/account/profile/test",
                    "uri": "/gdc/md/ws/obj/123",
                },
            }
        },
    ]

    dashboard = object.__new__(CloudDashboard)
    dashboard.ctx = mock_ctx

    widget_object = {
        "visualizationWidget": {"content": {"visualization": "/gdc/md/ws/obj/456"}}
    }

    result = dashboard._resolve_widget_type(widget_object)

    assert result == "insight"


def test_resolve_widget_type_without_class_returns_insight(mocker):
    """Visualization without visualizationClass should return 'insight' (fallback)."""
    mock_ctx = mocker.MagicMock()

    mock_ctx.legacy_client.get_object.return_value = {
        "visualizationObject": {
            "content": {
                "buckets": []
                # No visualizationClass field
            }
        }
    }

    dashboard = object.__new__(CloudDashboard)
    dashboard.ctx = mock_ctx

    widget_object = {
        "visualizationWidget": {"content": {"visualization": "/gdc/md/ws/obj/456"}}
    }

    result = dashboard._resolve_widget_type(widget_object)

    assert result == "insight"
    mock_ctx.legacy_client.get_object.assert_called_once()


def test_resolve_widget_type_unknown_raises_value_error(mocker):
    """Unknown widget types should raise ValueError."""
    mock_ctx = mocker.MagicMock()

    dashboard = object.__new__(CloudDashboard)
    dashboard.ctx = mock_ctx

    widget_object = {"unknownType": {"some": "data"}}

    with pytest.raises(ValueError, match="Unknown widget type"):
        dashboard._resolve_widget_type(widget_object)
