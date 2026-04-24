# (C) 2026 GoodData Corporation
"""
Inheritance management for web comparison reports.
"""

from gooddata_legacy2cloud.web_compare_processing.comparison_result import (
    ComparisonItem,
    ComparisonResult,
    ComparisonStatus,
)
from gooddata_legacy2cloud.web_compare_processing.metadata_extractor import (
    ConnectionParamsDict,
)
from gooddata_legacy2cloud.web_compare_processing.url_utils import generate_urls


class InheritanceManager:
    """Manages inherited objects between prefixed and unprefixed results."""

    def __init__(self):
        """Initialize the inheritance manager."""
        self.unprefixed_objects = {}

    def store_unprefixed_result(
        self, object_type: str, result: ComparisonResult
    ) -> None:
        """
        Store an unprefixed result for later inheritance.

        Args:
            object_type: Object type
            result: ComparisonResult to store
        """
        if object_type not in self.unprefixed_objects:
            self.unprefixed_objects[object_type] = result

    def get_unprefixed_result(self, object_type: str) -> ComparisonResult | None:
        """
        Get the unprefixed result for the specified object type.

        Args:
            object_type: Object type

        Returns:
            ComparisonResult or None if not found
        """
        return self.unprefixed_objects.get(object_type)

    def add_inherited_objects(
        self,
        prefixed_result: ComparisonResult,
        unprefixed_result: ComparisonResult,
        connection_params: ConnectionParamsDict,
    ) -> None:
        """
        Add inherited objects from unprefixed result to prefixed result.

        Args:
            prefixed_result: ComparisonResult for prefixed log file
            unprefixed_result: ComparisonResult for unprefixed log file
            connection_params: Connection parameters for URL generation
        """
        # Get existing legacy IDs to avoid duplicates
        existing_legacy_ids = {item.legacy_id for item in prefixed_result.items}

        # Reset ordinal number for inherited items
        inherited_count = 1

        # Add inherited items
        for unprefixed_item in unprefixed_result.items:
            # Skip if this legacy ID already exists in prefixed result
            if unprefixed_item.legacy_id in existing_legacy_ids:
                continue

            # Generate all URLs in a single call
            urls = generate_urls(
                unprefixed_item.legacy_id,
                None,  # legacy_obj_id
                unprefixed_item.cloud_id,
                prefixed_result.object_type,
                connection_params["legacy_domain"],
                connection_params["legacy_ws"],
                connection_params["cloud_domain"],
                connection_params["cloud_ws"],
            )

            # Create a new item with inherited status
            inherited_item = ComparisonItem(
                legacy_id=unprefixed_item.legacy_id,
                legacy_title=unprefixed_item.legacy_title,
                legacy_url=urls.legacy_url,
                cloud_id=unprefixed_item.cloud_id,
                cloud_title=unprefixed_item.cloud_title,
                cloud_url=urls.cloud_url,
                status=ComparisonStatus.INHERITED,
                ordinal_number=f"~{inherited_count}",
                details=unprefixed_item.details,
                cloud_description=unprefixed_item.cloud_description,
                legacy_embedded_url=urls.legacy_embedded_url,
                cloud_embedded_url=urls.cloud_embedded_url,
            )

            # Add the item and update summary
            prefixed_result.add_item(inherited_item)
            inherited_count += 1

    def has_unprefixed_result(self, object_type: str) -> bool:
        """
        Check if there's an unprefixed result for the specified object type.

        Args:
            object_type: Object type to check

        Returns:
            True if unprefixed result exists, False otherwise
        """
        return object_type in self.unprefixed_objects
