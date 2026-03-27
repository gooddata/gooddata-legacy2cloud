# (C) 2026 GoodData Corporation
"""
Data classes and utilities for object comparison results.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set


class ComparisonStatus(Enum):
    """Status of an object in the comparison."""

    SUCCESS = "success"  # Successfully migrated
    WARNING = "warning"  # Migrated with warnings
    ERROR = "error"  # Migration failed
    SKIPPED = "skipped"  # Migration skipped - object already exists
    API_ERROR = "api-error"  # Error during API publishing
    INHERITED = "inherited"  # Inherited from unprefixed log file


# Status title mapping for tooltips and UI display
STATUS_TITLES = {
    "success": "Successfully migrated.",
    "warning": "Migrated with warnings. Requires attention.",
    "skipped": "Deployment skipped - object already existed.",
    "api-error": "Deployment failed - API error.",
    "error": "Migration Failed",
    "inherited": "Not migrated directly but inherited from a workspce hierarchy.",
}


@dataclass
class ComparisonItem:
    """
    Represents a single item in the comparison result.

    Attributes:
        platform_id: Identifier of the object in Platform
        platform_title: Title of the object in Platform
        platform_url: URL to the object in Platform
        cloud_id: Identifier of the object in Cloud (if migrated)
        cloud_title: Title of the object in Cloud (if migrated)
        cloud_url: URL to the object in Cloud (if migrated)
        status: Status of the migration (success, warning, error, etc.)
        ordinal_number: Optional ordinal number for sorting
        details: Optional details about the migration
        cloud_description: Optional description of the object in Cloud
        platform_embedded_url: Optional URL for embedding the Platform object in an iframe
        cloud_embedded_url: Optional URL for embedding the Cloud object in an iframe
    """

    platform_id: str
    platform_title: str
    platform_url: str
    cloud_id: Optional[str]
    cloud_title: Optional[str]
    cloud_url: Optional[str]
    status: ComparisonStatus
    ordinal_number: Optional[int | str] = None
    details: Optional[str] = None
    cloud_description: Optional[str] = None

    # URLs for iframe embedding
    platform_embedded_url: Optional[str] = None
    cloud_embedded_url: Optional[str] = None


@dataclass
class ComparisonSummary:
    """
    Summary statistics for a comparison.

    Attributes:
        total_count: Total number of items
        success_count: Number of successfully migrated items
        warning_count: Number of items migrated with warnings
        error_count: Number of items that failed to migrate
        skipped_count: Number of items skipped because they already exist
        api_error_count: Number of items that failed due to API errors
        inherited_count: Number of items inherited from unprefixed logs
    """

    total_count: int = 0
    success_count: int = 0
    warning_count: int = 0
    error_count: int = 0
    skipped_count: int = 0
    api_error_count: int = 0
    inherited_count: int = 0

    def increment_by_status(self, status: ComparisonStatus) -> None:
        """
        Increment the appropriate counter based on the status.

        Args:
            status: The status to increment the counter for
        """
        if status == ComparisonStatus.SUCCESS:
            self.success_count += 1
        elif status == ComparisonStatus.WARNING:
            self.warning_count += 1
        elif status == ComparisonStatus.ERROR:
            self.error_count += 1
        elif status == ComparisonStatus.SKIPPED:
            self.skipped_count += 1
        elif status == ComparisonStatus.API_ERROR:
            self.api_error_count += 1
        elif status == ComparisonStatus.INHERITED:
            self.inherited_count += 1


@dataclass
class ComparisonResult:
    """
    Complete comparison result including items and summary.

    Attributes:
        object_type: Type of objects being compared (e.g., 'report', 'dashboard')
        platform_domain: Domain of the Platform environment
        platform_workspace: Workspace ID in the Platform environment
        cloud_domain: Domain of the Cloud environment
        cloud_workspace: Workspace ID in the Cloud environment
        items: List of comparison items
        summary: Summary statistics for the comparison
        debug_info: Dictionary of debug information
    """

    object_type: str
    platform_domain: str
    platform_workspace: str
    cloud_domain: str
    cloud_workspace: str
    items: List[ComparisonItem] = field(default_factory=list)
    summary: ComparisonSummary = field(default_factory=ComparisonSummary)
    debug_info: Dict[str, Any] = field(default_factory=dict)

    def add_item(self, item: ComparisonItem) -> None:
        """
        Add a comparison item and update summary statistics.

        Args:
            item: The comparison item to add
        """
        self.items.append(item)
        self.summary.total_count += 1
        self.summary.increment_by_status(item.status)

    def update_summary(self) -> None:
        """
        Recalculate summary statistics based on the current items.

        This is useful after modifying the items list directly or after
        importing items from another source.
        """
        # Reset all counters
        self.summary = ComparisonSummary()
        self.summary.total_count = len(self.items)

        # Recalculate all counters by iterating through items
        for item in self.items:
            self.summary.increment_by_status(item.status)

    def filter_items(
        self, status_filter: Optional[ComparisonStatus] = None
    ) -> List[ComparisonItem]:
        """
        Filter items by status.

        Args:
            status_filter: Optional status to filter by

        Returns:
            Filtered list of comparison items
        """
        if status_filter is None:
            return self.items

        return [item for item in self.items if item.status == status_filter]

    def get_ids_by_status(self, status: ComparisonStatus) -> Set[str]:
        """
        Get all Platform IDs for items with a specific status.

        Args:
            status: Status to filter by

        Returns:
            Set of Platform IDs
        """
        return {item.platform_id for item in self.items if item.status == status}
