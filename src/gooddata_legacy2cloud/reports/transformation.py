# (C) 2026 GoodData Corporation
"""
This module converts the Legacy report definition into a Cloud visualizationObject
used for reporting. It assumes that the input uses the "reportDefinition" key exclusively.
For each metric, the detailed Legacy object is retrieved and its Legacy identifier is extracted;
for metrics the conversion uses ctx.metric_mappings, whereas for attributes (and display forms)
the conversion uses ctx.ldm_mappings.
Debug logging is added to trace each step.
"""

import logging
from typing import Any

from gooddata_legacy2cloud.helpers import (
    REPORT_INSIGHT_PREFIX,
    get_cloud_id,
    parse_legacy_tags,
)
from gooddata_legacy2cloud.reports.charts import process_chart_report
from gooddata_legacy2cloud.reports.data_classes import (
    ContextWithWarnings,
    ReportContext,
)
from gooddata_legacy2cloud.reports.grid_processing import process_grid_report
from gooddata_legacy2cloud.reports.warning_collector import WarningCollector

# Configure logger for this module.
logger = logging.getLogger("migration")
logger.setLevel(logging.DEBUG)

# Prefix for title of all migrated PixelPerfect reports
REPORT_TITLE_PREFIX = "[PP] "


def set_report_title_prefix(prefix):
    """
    Sets the prefix to use for migrated report titles.

    Args:
        prefix (str): The prefix to use, or empty string to disable prefix
    """
    global REPORT_TITLE_PREFIX
    REPORT_TITLE_PREFIX = prefix


def transform_legacy_report(
    ctx: ReportContext, legacy_report: dict, warning_collector: WarningCollector
) -> dict:
    """
    Converts a Legacy report definition into a Cloud visualizationObject.

    Args:
        ctx: Shared context containing Legacy and Cloud APIs and mappings
        legacy_report (dict): The Legacy report definition to transform
        warning_collector: The warning collector for this specific report transformation

    Returns:
        dict: The Cloud visualizationObject
    """
    # Create a context proxy that provides warning methods to filters
    ctx_with_warnings = ContextWithWarnings(ctx, warning_collector)
    # Get the top-level report definition and meta
    report = legacy_report.get("reportDefinition", {})
    meta = report.get("meta", {})
    content = report.get("content", {})
    report_format = content.get("format", "grid")

    # Process the report based on its format
    if report_format == "chart":
        # Process chart report
        cloud_content = process_chart_report(ctx_with_warnings, report, content)
    else:
        # Process grid report (table, one number, etc.)
        cloud_content = process_grid_report(
            ctx_with_warnings, report, content, report_format
        )

    # Build the top-level Cloud identifier using the new lookup logic.
    legacy_title = meta.get("title", "Migrated Visualization")
    legacy_identifier = meta.get("identifier", "unknown")
    legacy_summary = meta.get("summary", "")

    if ctx.keep_original_ids:
        top_level_id = legacy_identifier
        if REPORT_TITLE_PREFIX:
            legacy_title = REPORT_TITLE_PREFIX + legacy_title
    else:
        top_level_id = get_cloud_id(legacy_title, legacy_identifier)
        # If this is a report migration, prepend the report insight prefix.
        if meta.get("category") in ["report", "reportDefinition"]:
            top_level_id = REPORT_INSIGHT_PREFIX + "__" + top_level_id
            if REPORT_TITLE_PREFIX:  # Only add prefix if it's not an empty string
                legacy_title = REPORT_TITLE_PREFIX + legacy_title

    # Log the mapping from Legacy to Cloud identifier
    ctx.mapping_logger.write_identifier_relation(
        meta.get("identifier", "unknown"), top_level_id
    )

    cloud_json: dict[str, Any] = {
        "data": {
            "id": top_level_id,
            "type": "visualizationObject",
            "attributes": {
                "title": legacy_title,
                "description": legacy_summary,
                "tags": parse_legacy_tags(meta),
                "content": cloud_content,
            },
        }
    }

    # Prepend warnings or errors if any were accumulated for this report.
    warnings_list = warning_collector.get_warnings()
    errors_list = warning_collector.get_errors()
    if errors_list or warnings_list:
        old_title = legacy_title
        if errors_list:
            new_prefix = "[ERROR] "
        elif (
            warning_collector.has_warnings() and not ctx.suppress_warnings
        ):  # Only add [WARN] prefix if there are warnings with severity "warning"
            new_prefix = "[WARN] "
        else:
            new_prefix = ""  # No prefix for info-only warnings or suppressed warnings

        if new_prefix and not old_title.startswith(new_prefix):
            cloud_json["data"]["attributes"]["title"] = new_prefix + old_title

        old_description = legacy_summary
        messages_str = ""

        if errors_list:
            messages_str += "**migration errors:**\n"
            for msg in errors_list:
                messages_str += "* " + msg + "\n"

        if warnings_list and not ctx.suppress_warnings:
            messages_str += "**migration warnings:**\n"
            for msg in warnings_list:
                messages_str += "* " + msg + "\n"

        if messages_str:  # Only update description if we have messages
            cloud_json["data"]["attributes"]["description"] = (
                messages_str + "\n---\n" + old_description
            )

    return cloud_json
