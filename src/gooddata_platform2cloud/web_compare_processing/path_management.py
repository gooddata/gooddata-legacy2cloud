# (C) 2026 GoodData Corporation
"""
Path management utilities for web comparison reports.
"""

import os
from typing import Dict, List, Optional, Tuple

from gooddata_platform2cloud.web_compare_processing.file_utils import FilePathBuilder
from gooddata_platform2cloud.web_compare_processing.generator import WebCompareUtils


class PathManager:
    """Manages paths for HTML generation and indicators."""

    def __init__(self, output_dir: str):
        """
        Initialize the path manager.

        Args:
            output_dir: Base output directory
        """
        self.output_dir = output_dir

    def prepare_for_html_generation(
        self,
        object_type: str,
        client_prefix: str,
        all_prefixes: Optional[Dict[str, List[str]]],
    ) -> Tuple[str, str, Dict]:
        """
        Prepare paths and data for HTML generation.

        Args:
            object_type: Object type
            client_prefix: Client prefix
            all_prefixes: Dictionary of prefixes by object type

        Returns:
            Tuple of (output path, resources relative path, sidebar data)
        """
        # Create resources directory at the root of output_dir
        resources_dir = os.path.join(self.output_dir, "resources")
        os.makedirs(resources_dir, exist_ok=True)

        # Store the prefix string
        prefix = client_prefix if client_prefix else ""

        # Normalize object type to plural form for filename
        object_type_plural = object_type + "s"

        # Determine the target directory for HTML and indicators
        if prefix:
            # If prefix exists, create a subdirectory with that prefix
            target_dir = os.path.join(self.output_dir, prefix.rstrip("_"))
            os.makedirs(target_dir, exist_ok=True)

            # Create indicators directory in the prefix subdirectory
            indicators_dir = os.path.join(target_dir, "indicators")
            os.makedirs(indicators_dir, exist_ok=True)

            # Relative path to resources from the prefix directory
            resources_rel_path = "../resources"
        else:
            # If no prefix, use the main output directory
            target_dir = self.output_dir

            # Create indicators directory in the main output directory
            indicators_dir = os.path.join(target_dir, "indicators")
            os.makedirs(indicators_dir, exist_ok=True)

            # Relative path to resources from the main directory
            resources_rel_path = "resources"

        # Generate output filename
        if prefix:
            # Include the prefix in the filename for prefixed clients (e.g., client1_insights_web_compare.html)
            output_file = f"{prefix.rstrip('_')}_{object_type_plural}_web_compare.html"
        else:
            # Standard naming for unprefixed files
            output_file = f"{object_type_plural}_web_compare.html"

        output_path = os.path.join(target_dir, output_file)

        # Prepare sidebar navigation data
        sidebar_data = {
            "current_type": object_type,
            "current_prefix": prefix.rstrip("_") if prefix else "",
            "is_prefixed": bool(prefix),
            "available_prefixes": {},
        }

        # Add prefix information if we have it
        if all_prefixes:
            # Include all prefixes for each object type, sorted alphabetically
            for obj_type, prefixes in all_prefixes.items():
                if prefixes:
                    # Sort the prefixes alphabetically
                    sidebar_data["available_prefixes"][obj_type] = sorted(prefixes)

        return output_path, resources_rel_path, sidebar_data

    def create_indicator(self, object_type: str, client_prefix: str) -> None:
        """
        Create indicator file for the processed log.

        Args:
            object_type: Object type
            client_prefix: Client prefix
        """
        # Normalize object type to plural form for filename
        object_type_plural = object_type + "s"

        # Store the prefix string
        prefix = client_prefix if client_prefix else ""
        # Ensure prefix doesn't end with underscore to avoid inconsistencies
        clean_prefix = prefix.rstrip("_")

        # Determine indicator directory
        if prefix:
            indicator_dir = os.path.join(self.output_dir, clean_prefix, "indicators")
        else:
            indicator_dir = os.path.join(self.output_dir, "indicators")

        # Write presence indicator - use clean_prefix
        indicator_path = os.path.join(indicator_dir, f"{clean_prefix}{object_type}.js")
        WebCompareUtils.write_presence_indicator(
            indicator_path, object_type_plural, clean_prefix
        )

    def get_additional_data_paths(
        self, log_file_path: str, object_type: str, client_prefix: str
    ) -> Tuple[str, str]:
        """
        Get paths for additional data files.

        Args:
            log_file_path: Path to the log file
            object_type: Object type
            client_prefix: Client prefix

        Returns:
            Tuple of (failed publishing path, skipped objects path)
        """
        failed_publishing_path = FilePathBuilder.get_failed_publishing_path(
            log_file_path, object_type, client_prefix
        )
        skipped_objects_path = FilePathBuilder.get_skipped_objects_path(
            log_file_path, object_type, client_prefix
        )
        return failed_publishing_path, skipped_objects_path
