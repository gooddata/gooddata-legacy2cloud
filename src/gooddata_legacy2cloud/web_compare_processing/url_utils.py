# (C) 2026 GoodData Corporation
"""
URL utilities for web comparison reports.
"""

from typing import Any, Dict, Optional

# URL templates for Legacy and Cloud UI links
# Legacy URL templates
LEGACY_REPORT_URL = "{legacy_domain}#s=/gdc/workspaces/{legacy_workspace}|analysisPage|head|/gdc/md/{legacy_workspace}/obj/{legacy_obj_id}"
LEGACY_REPORT_EMBEDDED_URL = "{legacy_domain}reportWidget.html#workspace=/gdc/workspaces/{legacy_workspace}&report=/gdc/md/{legacy_workspace}/obj/{legacy_obj_id}&title=yes"

LEGACY_INSIGHT_URL = "{legacy_domain}analyze/#/{legacy_workspace}/{legacy_id}/edit"
LEGACY_INSIGHT_EMBEDDED_URL = (
    "{legacy_domain}analyze/embedded/#/{legacy_workspace}/{legacy_id}/edit"
)

LEGACY_DASHBOARD_URL = (
    "{legacy_domain}dashboards/#/workspace/{legacy_workspace}/dashboard/{legacy_id}"
)
LEGACY_DASHBOARD_EMBEDDED_URL = "{legacy_domain}dashboards/embedded/#/workspace/{legacy_workspace}/dashboard/{legacy_id}"

# Cloud URL templates
CLOUD_REPORT_URL = "{cloud_domain}analyze/#/{cloud_workspace}/{cloud_id}/edit"
CLOUD_REPORT_EMBEDDED_URL = (
    "{cloud_domain}analyze/embedded/#/{cloud_workspace}/{cloud_id}"
)

CLOUD_INSIGHT_URL = "{cloud_domain}analyze/#/{cloud_workspace}/{cloud_id}/edit"
CLOUD_INSIGHT_EMBEDDED_URL = (
    "{cloud_domain}analyze/embedded/#/{cloud_workspace}/{cloud_id}"
)

CLOUD_DASHBOARD_URL = (
    "{cloud_domain}dashboards/#/workspace/{cloud_workspace}/dashboard/{cloud_id}"
)
CLOUD_DASHBOARD_EMBEDDED_URL = "{cloud_domain}dashboards/embedded/#/workspace/{cloud_workspace}/dashboard/{cloud_id}"


class ObjectUrls:
    """Container for all URL types related to an object."""

    def __init__(
        self,
        legacy_url: str = "",
        cloud_url: Optional[str] = None,
        legacy_embedded_url: Optional[str] = None,
        cloud_embedded_url: Optional[str] = None,
    ):
        """
        Initialize URL container.

        Args:
            legacy_url: Regular Legacy URL
            cloud_url: Regular Cloud URL
            legacy_embedded_url: Embedded Legacy URL
            cloud_embedded_url: Embedded Cloud URL
        """
        self.legacy_url = legacy_url
        self.cloud_url = cloud_url
        self.legacy_embedded_url = legacy_embedded_url
        self.cloud_embedded_url = cloud_embedded_url


def generate_urls(
    legacy_id: Optional[str],
    legacy_obj_id: Optional[str],
    cloud_id: Optional[str],
    object_type: str,
    legacy_domain: str,
    legacy_workspace: str,
    cloud_domain: str,
    cloud_workspace: str,
    entry_info: Optional[Dict[str, Any]] = None,
) -> ObjectUrls:
    """
    Generate Legacy and Cloud URLs for comparison.

    Args:
        legacy_id: Legacy object ID
        legacy_obj_id: Legacy numeric object ID (from URI)
        cloud_id: Cloud object ID
        object_type: Type of object (report, insight, dashboard)
        legacy_domain: Legacy domain
        legacy_workspace: Legacy workspace ID
        cloud_domain: Cloud domain
        cloud_workspace: Cloud workspace ID
        entry_info: Optional entry info for debugging

    Returns:
        ObjectUrls instance containing all URL types
    """
    # For debugging
    entry_desc = "Unknown entry"
    if entry_info:
        entry_desc = f"Entry #{entry_info.get('entry_num', '?')}"
        if "title" in entry_info:
            entry_desc += f" [{entry_info['title']}]"
        if "line_info" in entry_info and "title_line" in entry_info["line_info"]:
            entry_desc += f" (line {entry_info['line_info']['title_line'] + 1})"

    # Initialize URLs
    legacy_url = ""
    cloud_url = None
    legacy_embedded_url = None
    cloud_embedded_url = None

    # Generate Legacy URLs
    if legacy_obj_id or legacy_id:
        if object_type == "report":
            # Reports require legacy_obj_id specifically
            # TODO invoke identifiers to translate legacy_id to legacy_obj_id in batch for reports
            if legacy_obj_id:
                # Use report URL templates
                legacy_url = LEGACY_REPORT_URL.format(
                    legacy_domain=legacy_domain,
                    legacy_workspace=legacy_workspace,
                    legacy_obj_id=legacy_obj_id,
                    legacy_id=legacy_id,
                )

                legacy_embedded_url = LEGACY_REPORT_EMBEDDED_URL.format(
                    legacy_domain=legacy_domain,
                    legacy_workspace=legacy_workspace,
                    legacy_obj_id=legacy_obj_id,
                    legacy_id=legacy_id,
                )

        elif object_type == "insight":
            # Use insight URL templates
            legacy_url = LEGACY_INSIGHT_URL.format(
                legacy_domain=legacy_domain,
                legacy_workspace=legacy_workspace,
                legacy_obj_id=legacy_obj_id,
                legacy_id=legacy_id,
            )

            legacy_embedded_url = LEGACY_INSIGHT_EMBEDDED_URL.format(
                legacy_domain=legacy_domain,
                legacy_workspace=legacy_workspace,
                legacy_obj_id=legacy_obj_id,
                legacy_id=legacy_id,
            )
        elif object_type == "dashboard":
            # Use dashboard URL templates
            legacy_url = LEGACY_DASHBOARD_URL.format(
                legacy_domain=legacy_domain,
                legacy_workspace=legacy_workspace,
                legacy_obj_id=legacy_obj_id,
                legacy_id=legacy_id,
            )

            legacy_embedded_url = LEGACY_DASHBOARD_EMBEDDED_URL.format(
                legacy_domain=legacy_domain,
                legacy_workspace=legacy_workspace,
                legacy_obj_id=legacy_obj_id,
                legacy_id=legacy_id,
            )
    else:
        print(
            f"Warning: No legacy_obj_id or legacy_id available for generating Legacy URLs - {entry_desc}"
        )

    # Generate Cloud URLs
    if cloud_id:
        if object_type == "report":
            # Use report URL templates
            cloud_url = CLOUD_REPORT_URL.format(
                cloud_domain=cloud_domain,
                cloud_workspace=cloud_workspace,
                cloud_id=cloud_id,
            )

            cloud_embedded_url = CLOUD_REPORT_EMBEDDED_URL.format(
                cloud_domain=cloud_domain,
                cloud_workspace=cloud_workspace,
                cloud_id=cloud_id,
            )
        elif object_type == "insight":
            # Use insight URL templates
            cloud_url = CLOUD_INSIGHT_URL.format(
                cloud_domain=cloud_domain,
                cloud_workspace=cloud_workspace,
                cloud_id=cloud_id,
            )

            cloud_embedded_url = CLOUD_INSIGHT_EMBEDDED_URL.format(
                cloud_domain=cloud_domain,
                cloud_workspace=cloud_workspace,
                cloud_id=cloud_id,
            )
        elif object_type == "dashboard":
            # Use dashboard URL templates
            cloud_url = CLOUD_DASHBOARD_URL.format(
                cloud_domain=cloud_domain,
                cloud_workspace=cloud_workspace,
                cloud_id=cloud_id,
            )

            cloud_embedded_url = CLOUD_DASHBOARD_EMBEDDED_URL.format(
                cloud_domain=cloud_domain,
                cloud_workspace=cloud_workspace,
                cloud_id=cloud_id,
            )

    return ObjectUrls(legacy_url, cloud_url, legacy_embedded_url, cloud_embedded_url)
