# (C) 2026 GoodData Corporation
"""
Integration tests for pixel perfect dashboard migration.

These tests verify the end-to-end transformation of Legacy pixel perfect dashboards
to Cloud responsive dashboards.
"""

import pytest
from pytest import CaptureFixture

from gooddata_legacy2cloud.helpers import PP_FILTER_CONTEXT_PREFIX
from gooddata_legacy2cloud.pp_dashboards.utils import sanitize_string
from tests.test_utils import load_json

LEGACY_OBJECTS_DIR = "tests/data/pixel_perfect_dashboards/legacy_objects"


@pytest.mark.parametrize("case_file_name", ["simple_two_tabs"])
def test_pp_dashboard_migration(
    case_file_name,
    pp_dashboards_builder,
    mock_legacy_pp_dashboards,
    mock_cloud_pp_api,
    capsys: CaptureFixture[str],
):
    """Test the transformation of Legacy pixel perfect dashboard to Cloud.

    This test will be fully implemented after the Builder pattern is in place (Phase 2).

    To add test cases:
    1. Add case name to parametrize list
    2. Create test data files in tests/data/pixel_perfect_dashboards/test_cases/
       - <case_name>_legacy.json - Legacy PP dashboard
       - <case_name>_cloud.json - Expected Cloud dashboard
    """

    assert case_file_name == "simple_two_tabs"
    legacy_dashboards = [mock_legacy_pp_dashboards]

    # skip_deploy=True so tests don't depend on deployment API calls
    pp_dashboards_builder.process_legacy_dashboards(
        legacy_dashboards=legacy_dashboards,
        skip_deploy=True,
        overwrite_existing=False,
    )

    cloud_dashboards = pp_dashboards_builder.get_cloud_dashboards()

    # 2 tabs => 1 dashboard with 2 tabs
    assert len(cloud_dashboards) == 1
    assert len(cloud_dashboards[0].attributes.content.tabs) == 2

    # Tabbed PP dashboards should NOT have a dashboard-level/global filterContextRef.
    # Filter context is attached per-tab instead.
    assert cloud_dashboards[0].attributes.content.filterContextRef is None
    dumped = cloud_dashboards[0].model_dump(exclude_none=True)
    assert "filterContextRef" not in dumped["attributes"]["content"]

    # Smoke-check IDs and titles
    for dashboard in cloud_dashboards:
        assert dashboard.id.startswith("ppdash")
        assert dashboard.attributes.title.startswith("[PP]")

    captured = capsys.readouterr()
    output = captured.out + captured.err
    assert "ERROR" not in output


def test_home_tab_not_filtered(
    pp_dashboards_builder,
    mock_legacy_pp_dashboards,
    mock_cloud_pp_api,
    capsys: CaptureFixture[str],
):
    """Test that tabs named 'Home' are NOT filtered out after bug fix."""
    legacy_dashboard = load_json(f"{LEGACY_OBJECTS_DIR}/pp_dashboard_home_tab.json")

    pp_dashboards_builder.process_legacy_dashboards(
        legacy_dashboards=[legacy_dashboard],
        skip_deploy=True,
        overwrite_existing=False,
    )

    cloud_dashboards = pp_dashboards_builder.get_cloud_dashboards()
    assert len(cloud_dashboards) == 1
    assert len(cloud_dashboards[0].attributes.content.tabs) == 1
    assert cloud_dashboards[0].attributes.content.tabs[0].title == "Home"

    captured = capsys.readouterr()
    output = captured.out + captured.err
    assert "ERROR" not in output


def test_tab_with_text_items_only(
    pp_dashboards_builder,
    mock_legacy_pp_dashboards,
    mock_cloud_pp_api,
):
    """Test migration of tab containing only textItems."""
    legacy_dashboard = load_json(f"{LEGACY_OBJECTS_DIR}/pp_dashboard_text_only.json")

    pp_dashboards_builder.process_legacy_dashboards(
        legacy_dashboards=[legacy_dashboard],
        skip_deploy=True,
        overwrite_existing=False,
    )

    cloud_dashboards = pp_dashboards_builder.get_cloud_dashboards()
    assert len(cloud_dashboards) == 1

    # At least one richText widget should be created (short text should be skipped)
    items = [
        item
        for section in cloud_dashboards[0].attributes.content.tabs[0].layout.sections
        for item in section.items
    ]
    assert any(getattr(i.widget, "type", None) == "richText" for i in items)


def test_tab_with_filters(
    pp_dashboards_builder,
    mock_legacy_pp_dashboards,
    mock_cloud_pp_api,
):
    """Test migration of tab with filter items."""
    legacy_dashboard = load_json(f"{LEGACY_OBJECTS_DIR}/pp_dashboard_with_filters.json")

    pp_dashboards_builder.process_legacy_dashboards(
        legacy_dashboards=[legacy_dashboard],
        skip_deploy=True,
        overwrite_existing=False,
    )

    cloud_dashboards = pp_dashboards_builder.get_cloud_dashboards()
    assert len(cloud_dashboards) == 1
    dashboard = cloud_dashboards[0]

    assert len(dashboard.attributes.content.tabs) >= 1

    # Filter context ref should be attached per-tab (ID prefix is critical for safe cleanup)
    fc_ref = dashboard.attributes.content.tabs[0].filterContextRef
    assert fc_ref.identifier.id.startswith("ppctx_")
    assert dashboard.id in fc_ref.identifier.id

    # Ensure filter context payload used the same ID (create_filter_context is mocked)
    call_args = mock_cloud_pp_api.create_filter_context.call_args
    assert call_args is not None
    payload = call_args.args[0]
    assert payload["data"]["id"] == fc_ref.identifier.id

    # Ensure we don't emit an empty global filterContextRef on the dashboard content
    assert dashboard.attributes.content.filterContextRef is None
    dumped = dashboard.model_dump(exclude_none=True)
    assert "filterContextRef" not in dumped["attributes"]["content"]


def test_dashboard_with_multiple_tabs(
    pp_dashboards_builder,
    mock_legacy_pp_dashboards,
    mock_cloud_pp_api,
):
    """Test migration of dashboard with 3+ tabs."""
    legacy_dashboard = load_json(f"{LEGACY_OBJECTS_DIR}/pp_dashboard_multi_tab.json")

    pp_dashboards_builder.process_legacy_dashboards(
        legacy_dashboards=[legacy_dashboard],
        skip_deploy=True,
        overwrite_existing=False,
    )

    cloud_dashboards = pp_dashboards_builder.get_cloud_dashboards()
    assert len(cloud_dashboards) == 1
    assert len(cloud_dashboards[0].attributes.content.tabs) == 3


def test_legacy_split_flag_keeps_old_behavior(
    pp_dashboards_builder_legacy_split,
    mock_legacy_pp_dashboards,
    mock_cloud_pp_api,
):
    """Test that legacy split-tabs behavior still produces one dashboard per tab."""
    pp_dashboards_builder_legacy_split.process_legacy_dashboards(
        legacy_dashboards=[mock_legacy_pp_dashboards],
        skip_deploy=True,
        overwrite_existing=False,
    )

    cloud_dashboards = pp_dashboards_builder_legacy_split.get_cloud_dashboards()
    assert len(cloud_dashboards) == 2
    assert all(not d.attributes.content.tabs for d in cloud_dashboards)


def test_tabbed_dashboard_creates_filter_context_per_tab_and_keeps_empty_tabs(
    pp_dashboards_builder,
    mock_legacy_pp_dashboards,
    mock_cloud_pp_api,
):
    """Test tabbed dashboard keeps empty tabs and creates filter context per migrated tab."""
    legacy_dashboard = load_json(
        f"{LEGACY_OBJECTS_DIR}/pp_dashboard_real_multi_tab_min.json"
    )

    pp_dashboards_builder.process_legacy_dashboards(
        legacy_dashboards=[legacy_dashboard],
        skip_deploy=True,
        overwrite_existing=False,
    )

    cloud_dashboards = pp_dashboards_builder.get_cloud_dashboards()
    assert len(cloud_dashboards) == 1

    dashboard = cloud_dashboards[0]
    assert len(dashboard.attributes.content.tabs) == 3
    assert mock_cloud_pp_api.create_filter_context.call_count == 3

    tabs_by_title = {t.title: t for t in dashboard.attributes.content.tabs}
    assert set(tabs_by_title.keys()) == {"Reports tab", "Empty tab", "Headline tab"}

    # Empty tab must be kept and should have an empty layout
    empty_tab = tabs_by_title["Empty tab"]
    assert empty_tab.localIdentifier == sanitize_string("tab_empty")
    assert empty_tab.layout.sections == []

    # Non-empty tabs should contain at least one item in layout sections
    for title in ("Reports tab", "Headline tab"):
        tab = tabs_by_title[title]
        assert tab.layout.sections
        assert any(section.items for section in tab.layout.sections)

    # Each tab gets a deterministic filter context id: ppctx_<dashboardId>_<tabLocalId>
    for tab in dashboard.attributes.content.tabs:
        expected_fc_id = sanitize_string(
            f"{PP_FILTER_CONTEXT_PREFIX}_{dashboard.id}_{tab.localIdentifier}"
        )
        assert tab.filterContextRef is not None
        assert tab.filterContextRef.identifier.id == expected_fc_id


def test_tabbed_dashboard_skips_unsupported_tab_but_keeps_valid_tabs(
    pp_dashboards_builder,
    mock_legacy_pp_dashboards,
    mock_cloud_pp_api,
):
    """Test that unsupported tabs are skipped while other tabs still migrate."""
    legacy_dashboard = load_json(
        f"{LEGACY_OBJECTS_DIR}/pp_dashboard_real_unsupported_tab_min.json"
    )

    pp_dashboards_builder.process_legacy_dashboards(
        legacy_dashboards=[legacy_dashboard],
        skip_deploy=True,
        overwrite_existing=False,
    )

    cloud_dashboards = pp_dashboards_builder.get_cloud_dashboards()
    assert len(cloud_dashboards) == 1

    dashboard = cloud_dashboards[0]
    assert [t.title for t in dashboard.attributes.content.tabs] == ["Valid tab"]
    assert mock_cloud_pp_api.create_filter_context.call_count == 1


def test_tabbed_dashboard_all_tabs_unsupported_is_skipped(
    pp_dashboards_builder,
    mock_legacy_pp_dashboards,
    mock_cloud_pp_api,
    caplog: pytest.LogCaptureFixture,
):
    """Test that a dashboard is skipped when no tabs can be migrated."""
    legacy_dashboard = load_json(
        f"{LEGACY_OBJECTS_DIR}/pp_dashboard_real_all_tabs_unsupported_min.json"
    )

    pp_dashboards_builder.process_legacy_dashboards(
        legacy_dashboards=[legacy_dashboard],
        skip_deploy=True,
        overwrite_existing=False,
    )

    cloud_dashboards = pp_dashboards_builder.get_cloud_dashboards()
    assert cloud_dashboards == []
    assert mock_cloud_pp_api.create_filter_context.call_count == 0

    assert "Skipping dashboard" in caplog.text
