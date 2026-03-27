# (C) 2026 GoodData Corporation
"""
URL utilities for web comparison reports.
"""

from typing import Any, Dict, Optional

# URL templates for Platform and Cloud UI links
# Platform URL templates
PLATFORM_REPORT_URL = "{platform_domain}#s=/gdc/workspaces/{platform_workspace}|analysisPage|head|/gdc/md/{platform_workspace}/obj/{platform_obj_id}"
PLATFORM_REPORT_EMBEDDED_URL = "{platform_domain}reportWidget.html#workspace=/gdc/workspaces/{platform_workspace}&report=/gdc/md/{platform_workspace}/obj/{platform_obj_id}&title=yes"

PLATFORM_INSIGHT_URL = (
    "{platform_domain}analyze/#/{platform_workspace}/{platform_id}/edit"
)
PLATFORM_INSIGHT_EMBEDDED_URL = (
    "{platform_domain}analyze/embedded/#/{platform_workspace}/{platform_id}/edit"
)

PLATFORM_DASHBOARD_URL = "{platform_domain}dashboards/#/workspace/{platform_workspace}/dashboard/{platform_id}"
PLATFORM_DASHBOARD_EMBEDDED_URL = "{platform_domain}dashboards/embedded/#/workspace/{platform_workspace}/dashboard/{platform_id}"

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
        platform_url: str = "",
        cloud_url: Optional[str] = None,
        platform_embedded_url: Optional[str] = None,
        cloud_embedded_url: Optional[str] = None,
    ):
        """
        Initialize URL container.

        Args:
            platform_url: Regular Platform URL
            cloud_url: Regular Cloud URL
            platform_embedded_url: Embedded Platform URL
            cloud_embedded_url: Embedded Cloud URL
        """
        self.platform_url = platform_url
        self.cloud_url = cloud_url
        self.platform_embedded_url = platform_embedded_url
        self.cloud_embedded_url = cloud_embedded_url


def generate_urls(
    platform_id: Optional[str],
    platform_obj_id: Optional[str],
    cloud_id: Optional[str],
    object_type: str,
    platform_domain: str,
    platform_workspace: str,
    cloud_domain: str,
    cloud_workspace: str,
    entry_info: Optional[Dict[str, Any]] = None,
) -> ObjectUrls:
    """
    Generate Platform and Cloud URLs for comparison.

    Args:
        platform_id: Platform object ID
        platform_obj_id: Platform numeric object ID (from URI)
        cloud_id: Cloud object ID
        object_type: Type of object (report, insight, dashboard)
        platform_domain: Platform domain
        platform_workspace: Platform workspace ID
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
    platform_url = ""
    cloud_url = None
    platform_embedded_url = None
    cloud_embedded_url = None

    # Generate Platform URLs
    if platform_obj_id or platform_id:
        if object_type == "report":
            # Reports require platform_obj_id specifically
            # TODO invoke identifiers to translate platform_id to platform_obj_id in batch for reports
            if platform_obj_id:
                # Use report URL templates
                platform_url = PLATFORM_REPORT_URL.format(
                    platform_domain=platform_domain,
                    platform_workspace=platform_workspace,
                    platform_obj_id=platform_obj_id,
                    platform_id=platform_id,
                )

                platform_embedded_url = PLATFORM_REPORT_EMBEDDED_URL.format(
                    platform_domain=platform_domain,
                    platform_workspace=platform_workspace,
                    platform_obj_id=platform_obj_id,
                    platform_id=platform_id,
                )

        elif object_type == "insight":
            # Use insight URL templates
            platform_url = PLATFORM_INSIGHT_URL.format(
                platform_domain=platform_domain,
                platform_workspace=platform_workspace,
                platform_obj_id=platform_obj_id,
                platform_id=platform_id,
            )

            platform_embedded_url = PLATFORM_INSIGHT_EMBEDDED_URL.format(
                platform_domain=platform_domain,
                platform_workspace=platform_workspace,
                platform_obj_id=platform_obj_id,
                platform_id=platform_id,
            )
        elif object_type == "dashboard":
            # Use dashboard URL templates
            platform_url = PLATFORM_DASHBOARD_URL.format(
                platform_domain=platform_domain,
                platform_workspace=platform_workspace,
                platform_obj_id=platform_obj_id,
                platform_id=platform_id,
            )

            platform_embedded_url = PLATFORM_DASHBOARD_EMBEDDED_URL.format(
                platform_domain=platform_domain,
                platform_workspace=platform_workspace,
                platform_obj_id=platform_obj_id,
                platform_id=platform_id,
            )
    else:
        print(
            f"Warning: No platform_obj_id or platform_id available for generating Platform URLs - {entry_desc}"
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

    return ObjectUrls(
        platform_url, cloud_url, platform_embedded_url, cloud_embedded_url
    )
