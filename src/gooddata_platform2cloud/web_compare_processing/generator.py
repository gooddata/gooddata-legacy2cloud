# (C) 2026 GoodData Corporation
"""
Utility for generating web comparison HTML pages from ComparisonResult objects.
"""

import datetime
import os
import shutil
from typing import Dict, List, Optional

from jinja2 import Environment, FileSystemLoader

from gooddata_platform2cloud.web_compare_processing.comparison_result import (
    STATUS_TITLES,
    ComparisonResult,
)


class ComparisonGenerator:
    """Generates HTML comparison pages from ComparisonResult objects using Jinja2 templates."""

    def __init__(self, result: ComparisonResult):
        """
        Initialize the generator with a ComparisonResult.

        Args:
            result: The ComparisonResult containing comparison data
        """
        self.result = result

        # Setup Jinja2 template environment
        template_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "templates"
        )
        self.env = Environment(loader=FileSystemLoader(template_dir))

    def generate_html(
        self,
        output_path: str,
        resources_path: str = "resources",
        sidebar_data: Optional[Dict] = None,
        log_metadata=None,
    ) -> None:
        """
        Generate an HTML comparison page and save it to the specified path.

        Args:
            output_path: Path where the HTML file should be saved
            resources_path: Relative path to the resources directory
            sidebar_data: Optional dictionary containing navigation data for the sidebar
            log_metadata: Optional metadata extracted from log file headers
        """
        # Setup directories and resources
        self._setup_directories(output_path, resources_path)

        # Prepare the data for the HTML template
        template_data = self._prepare_template_data(
            resources_path, sidebar_data, log_metadata
        )

        # Render the template and write to file
        self._render_and_write_template(output_path, template_data)

        # print(f"Generated: '{output_path}'")

    def _setup_directories(self, output_path: str, resources_path: str) -> str:
        """
        Set up the necessary directories and copy resources.

        Args:
            output_path: Path where the HTML file will be saved
            resources_path: Relative path to the resources directory

        Returns:
            Path to the resources directory
        """
        # Create output directory
        output_dir = os.path.dirname(output_path)
        os.makedirs(output_dir, exist_ok=True)

        # Determine resources directory
        if resources_path.startswith(".."):
            # We're in a prefixed directory, resources will be in parent dir
            resources_dir = os.path.dirname(output_dir)
        else:
            # We're in the main output directory
            resources_dir = output_dir

        # Define target resources path
        target_resources_dir = os.path.join(resources_dir, "resources")

        # Copy resources directory if it doesn't exist or is empty
        is_empty = (
            os.path.exists(target_resources_dir)
            and len(os.listdir(target_resources_dir)) == 0
        )
        if not os.path.exists(target_resources_dir) or is_empty:
            # Get the path to the template resources directory
            resources_template_dir = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "templates", "resources"
            )

            # Copy the entire resources directory in one operation
            try:
                if os.path.exists(target_resources_dir):
                    shutil.rmtree(
                        target_resources_dir
                    )  # Remove the empty directory first
                shutil.copytree(resources_template_dir, target_resources_dir)
            except Exception as e:
                print(f"ERROR: Failed to copy resources: {e}")

        return resources_dir

    def _render_and_write_template(self, output_path: str, template_data: Dict) -> None:
        """
        Render the template and write it to the output file.

        Args:
            output_path: Path where the HTML file should be saved
            template_data: Dictionary containing data for the template
        """
        template = self.env.get_template("comparison.html")
        html_content = template.render(**template_data)

        with open(output_path, "w") as f:
            f.write(html_content)

    def _prepare_template_data(
        self,
        resources_path: str = "resources",
        sidebar_data: Optional[Dict] = None,
        log_metadata=None,
    ) -> Dict:
        """
        Prepare the data for the HTML template.

        Args:
            resources_path: Relative path to the resources directory
            sidebar_data: Optional dictionary containing navigation data for the sidebar
            log_metadata: Optional metadata extracted from log file headers

        Returns:
            Dictionary containing data for the template
        """
        # Prepare basic template data (object info, metadata, etc.)
        data = self._prepare_basic_data(sidebar_data, log_metadata, resources_path)

        # Prepare table rows data
        missing_workspace_info = data.get("missing_workspace_info", False)
        data["table_rows"] = self._prepare_table_rows(missing_workspace_info)

        # Add status titles for use in the template
        data["status_titles"] = STATUS_TITLES

        # Add sidebar navigation data if provided
        if sidebar_data:
            data.update(self._prepare_sidebar_data(sidebar_data, data["object_type"]))

        return data

    def _prepare_basic_data(
        self, sidebar_data: Optional[Dict], log_metadata, resources_path: str
    ) -> Dict:
        """
        Prepare basic template data including object info, metadata, and connection parameters.

        Args:
            sidebar_data: Optional sidebar navigation data
            log_metadata: Optional log file metadata
            resources_path: Relative path to the resources directory

        Returns:
            Dictionary containing basic template data
        """
        # Get object type and display information
        object_type = self.result.object_type.lower()
        display_type = object_type.capitalize()
        if not object_type.endswith("s"):
            display_type = display_type + "s"

        # Check for missing workspace info
        is_prefixed_client = sidebar_data and sidebar_data.get("is_prefixed", False)
        missing_migration_info = (
            hasattr(self.result, "missing_migration_info")
            and self.result.missing_migration_info
        )

        # Only consider missing workspace info if either:
        # 1. It's a prefixed client without migration info headers, or
        # 2. The result object has the missing_migration_info flag set AND it's a prefixed client
        # For unprefixed clients, we'll always show workspace IDs (from log file or from .env)
        missing_workspace_info = is_prefixed_client and missing_migration_info

        # Get generation timestamp for the current time when the report is generated.
        generation_time = self._get_generation_time(log_metadata)

        # Update result with metadata if available
        self._update_result_with_metadata(log_metadata)

        # Prepare basic template data
        return {
            "object_type": object_type,
            "display_type": display_type,
            "generation_time": generation_time,
            "summary": self.result.summary,
            "resources_path": resources_path,
            "platform_domain": self.result.platform_domain,
            "platform_workspace": self.result.platform_workspace,
            "cloud_domain": self.result.cloud_domain,
            "cloud_workspace": self.result.cloud_workspace,
            "missing_workspace_info": missing_workspace_info,
            "log_metadata": (
                self._format_log_metadata(log_metadata) if log_metadata else None
            ),
        }

    def _get_generation_time(self, log_metadata) -> str:
        """
        Get generation timestamp for the current time when the report is generated.

        Args:
            log_metadata: Optional log file metadata (not used anymore - we now use separate fields)

        Returns:
            Formatted timestamp string for the current generation time
        """
        # Always use current time for generation timestamp
        # Use isoformat() to match the timestamp format used in log files
        return datetime.datetime.now().isoformat()

    def _update_result_with_metadata(self, log_metadata) -> None:
        """
        Update the result object with metadata if available.

        Args:
            log_metadata: Optional log file metadata
        """
        if not log_metadata:
            return

        if log_metadata.platform_hostname:
            self.result.platform_domain = log_metadata.platform_hostname
        if log_metadata.platform_ws:
            self.result.platform_workspace = log_metadata.platform_ws
        if log_metadata.cloud_hostname:
            self.result.cloud_domain = log_metadata.cloud_hostname
        if log_metadata.cloud_ws:
            self.result.cloud_workspace = log_metadata.cloud_ws

    def _format_log_metadata(self, log_metadata) -> Dict:
        """
        Format log metadata for template use.

        Args:
            log_metadata: Log file metadata

        Returns:
            Dictionary with formatted metadata
        """
        return {
            "timestamp": log_metadata.timestamp,
            "platform_hostname": log_metadata.platform_hostname,
            "platform_ws": log_metadata.platform_ws,
            "cloud_hostname": log_metadata.cloud_hostname,
            "cloud_ws": log_metadata.cloud_ws,
            "client_prefix": log_metadata.client_prefix,
        }

    def _prepare_table_rows(self, missing_workspace_info: bool) -> List[Dict]:
        """
        Prepare table row data for the template.

        Args:
            missing_workspace_info: Boolean indicating if workspace info is missing

        Returns:
            List of dictionaries containing row data
        """
        table_rows = []

        # Use the module-level status titles
        for item in self.result.items:
            status_class = item.status.value
            display_title = (
                item.platform_title
            )  # Always use Platform title from the first line of the log
            status_title = STATUS_TITLES.get(status_class, "Unknown status")

            # Create row data
            row = self._create_row(
                item, status_class, status_title, display_title, missing_workspace_info
            )
            table_rows.append(row)

        return table_rows

    def _create_row(
        self,
        item,
        status_class: str,
        status_title: str,
        display_title: str,
        missing_workspace_info: bool,
    ) -> Dict:
        """
        Create a table row with or without links based on workspace info availability.

        Args:
            item: ComparisonItem
            status_class: Status class value
            status_title: Status title for tooltip
            display_title: Display title for the row
            missing_workspace_info: Whether workspace info is missing

        Returns:
            Dictionary containing row data
        """
        # Base row data (common to both cases)
        row = {
            "ordinal_number": (
                item.ordinal_number if item.ordinal_number is not None else ""
            ),
            "status": status_class,
            "status_title": status_title,
            "title": display_title,
            "platform_id": item.platform_id,
            "cloud_id": item.cloud_id if item.cloud_id else "Not migrated",
            "description": (
                item.cloud_description if item.cloud_description else item.details or ""
            ),
        }

        # If we have an error status but no description at all, only then use the status title as fallback
        if status_class == "error" and not row["description"]:
            row["description"] = f"Error: {status_title}"

        # Add URL fields based on workspace info availability
        if missing_workspace_info:
            # No URLs when workspace info is missing
            row.update(
                {
                    "platform_url": "",
                    "cloud_url": "",
                    "platform_embedded_url": "",
                    "cloud_embedded_url": "",
                }
            )
        else:
            # Include URLs when workspace info is available
            row.update(
                {
                    "platform_url": item.platform_url,
                    "cloud_url": item.cloud_url if item.cloud_url else "",
                    "platform_embedded_url": (
                        item.platform_embedded_url if item.platform_embedded_url else ""
                    ),
                    "cloud_embedded_url": (
                        item.cloud_embedded_url if item.cloud_embedded_url else ""
                    ),
                }
            )

        return row

    def _prepare_sidebar_data(self, sidebar_data: Dict, object_type: str) -> Dict:
        """
        Prepare sidebar data for the template.

        Args:
            sidebar_data: Dictionary containing sidebar data
            object_type: Object type

        Returns:
            Dictionary containing sidebar data
        """
        return {
            "current_type": sidebar_data.get("current_type", object_type),
            "current_prefix": sidebar_data.get("current_prefix", ""),
            "is_prefixed": sidebar_data.get("is_prefixed", False),
            "available_prefixes": sidebar_data.get("available_prefixes", {}),
        }


class WebCompareUtils:
    """Utility functions for web comparison generation."""

    @staticmethod
    def generate_comparison_html(
        result: ComparisonResult,
        output_path: str,
        resources_path: str = "resources",
        sidebar_data: dict | None = None,
        log_metadata=None,
    ) -> None:
        """
        Generate an HTML comparison page from a ComparisonResult and save it to the specified path.

        Args:
            result: The ComparisonResult containing comparison data
            output_path: Path where the HTML file should be saved
            resources_path: Relative path to the resources directory (default: 'resources')
            sidebar_data: Optional dictionary containing navigation data for the sidebar
            log_metadata: Optional metadata extracted from log file headers
        """
        generator = ComparisonGenerator(result)
        generator.generate_html(output_path, resources_path, sidebar_data, log_metadata)

    @staticmethod
    def write_presence_indicator(
        indicator_path: str, object_type: str, prefix: str = ""
    ) -> None:
        """
        Write a simple JS file indicating the presence of this report type.

        Args:
            indicator_path: Full path where to write the JS indicator file
            object_type: Type of report (insights, dashboards, reports)
            prefix: Optional prefix for the filename
        """
        # Standardize to generate consistent file names - always use lowercase without 's'
        normalized_type = WebCompareUtils._normalize_type(object_type)

        # Create the parent directory if it doesn't exist
        indicator_dir = os.path.dirname(indicator_path)
        os.makedirs(indicator_dir, exist_ok=True)

        # Create JS content - a simple function that returns true
        js_content = f"""// Presence indicator for {object_type} web compare
function {normalized_type}_present() {{
    return true;
}}
"""
        # Write the content to file
        with open(indicator_path, "w") as f:
            f.write(js_content)

    @staticmethod
    def _normalize_type(object_type: str) -> str:
        """
        Normalize object type to singular form.

        Args:
            object_type: Object type string

        Returns:
            Normalized type in singular form
        """
        normalized_type = object_type.lower()
        if normalized_type.endswith("s"):
            normalized_type = normalized_type[:-1]
        return normalized_type
