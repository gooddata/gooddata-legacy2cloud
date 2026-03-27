# (C) 2026 GoodData Corporation
"""
Element prefetching utility for batch fetching element values from Platform.

This module provides functionality to collect element URIs from objects,
batch them by attribute, and fetch their values in bulk to populate cache.
"""

import json
import logging
import re
from collections import defaultdict
from typing import Any

from gooddata_platform2cloud.backends.platform.client import PlatformClient
from gooddata_platform2cloud.metrics.display_form_utils import get_primary_display_form

logger = logging.getLogger("migration")

# Maximum number of elements per metric
ELEMENTS_PER_METRIC = 50


class ElementPrefetcher:
    """
    Handles batch fetching of element values to populate cache before processing.

    This class collects element URIs from objects, groups them by attribute,
    and fetches their values in batches to minimize API calls.
    """

    def __init__(self, platform_client: PlatformClient):
        """
        Initialize the ElementPrefetcher.

        Args:
            platform_client: Platform API instance for making requests
        """
        self.platform_client = platform_client
        self.element_uri_pattern = re.compile(
            r"(/gdc/md/[^/]+/obj/(\d+))/elements\?id=(\d+)"
        )
        self.elements_by_attribute = defaultdict(set)
        self.successfully_fetched = 0
        self.failed_to_fetch = 0
        self.unmapped_uris = []
        self.created_metric_uris = []

    def collect_element_uris_from_objects(self, objects: list[dict[str, Any]]) -> None:
        """
        Scan through objects and collect all element URIs using string-based regex matching.

        Processes each object individually to avoid creating large JSON strings
        with thousands of objects. More efficient than recursive structural traversal.

        Args:
            objects: List of Platform objects (insights, reports, dashboards, etc.)
        """
        for obj in objects:
            # Serialize each object individually to manage memory efficiently
            json_str = json.dumps(obj)

            # Find all element URIs in this object using optimized regex
            for match in self.element_uri_pattern.finditer(json_str):
                full_uri = match.group(0)
                attribute_uri = match.group(1)
                # Store the full element URI, grouped by attribute URI
                self.elements_by_attribute[attribute_uri].add(full_uri)

    def prefetch_and_cache(self, batch_size: int = ELEMENTS_PER_METRIC) -> None:
        """
        Fetch element values in batches and populate the cache.

        Args:
            batch_size: Maximum number of elements to fetch per API call (default: ELEMENTS_PER_METRIC)
        """
        logger.info("----Looking Up Used Element Values----")

        if not self.elements_by_attribute:
            logger.info("No element values found in objects")
            return

        total_attributes = len(self.elements_by_attribute)
        total_elements = sum(len(uris) for uris in self.elements_by_attribute.values())

        logger.info(
            "Found %s distinct element values of %s attributes",
            total_elements,
            total_attributes,
        )

        logger.info(
            "----Fetching Used Element Values (of %s attributes)----",
            total_attributes,
        )
        logger.info("Fetching element values")

        for attribute_uri, element_uris in self.elements_by_attribute.items():
            self._fetch_elements_for_attribute(
                attribute_uri, list(element_uris), batch_size
            )

        logger.info(
            "Done (%s elements fetched, %s failed)",
            self.successfully_fetched,
            self.failed_to_fetch,
        )

    def _fetch_elements_for_attribute(
        self,
        attribute_uri: str,
        element_uris: list[str],
        batch_size: int,
    ) -> None:
        """
        Fetch elements for a single attribute in batches.

        This method:
        1. Determines the primary display form ID for the attribute
        2. Splits element URIs into batches
        3. Fetches each batch via the Platform API
        4. Processes responses and updates cache

        Args:
            attribute_uri: The attribute URI (e.g., "/gdc/md/workspace/obj/123")
            element_uris: List of element URIs for this attribute
            batch_size: Maximum elements per batch
        """
        total_elements = len(element_uris)

        # Step 1: Get the primary display form ID (required for Platform API)
        try:
            attribute_obj = self.platform_client.get_object(attribute_uri)
            display_forms = (
                attribute_obj.get("attribute", {})
                .get("content", {})
                .get("displayForms", [])
            )
            primary_display_form = get_primary_display_form(display_forms)
            if primary_display_form:
                uri = primary_display_form.get("meta", {}).get("uri", "")
                display_form_id = uri.rstrip("/").split("/")[-1] if uri else None
            else:
                display_form_id = None
        except Exception:
            display_form_id = None

        if not display_form_id:
            # Cannot fetch without display form - mark all as failed
            self.failed_to_fetch += total_elements
            self.unmapped_uris.extend(element_uris)
            return

        # Step 2: Split elements into batches and fetch each batch
        num_batches = (total_elements + batch_size - 1) // batch_size
        for batch_idx in range(num_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, total_elements)
            batch_uris = element_uris[start_idx:end_idx]

            # Step 3: Fetch and process this batch
            try:
                result = self.platform_client.fetch_valid_elements(
                    display_form_id, batch_uris
                )
                self._process_valid_elements_response(result, batch_uris)
            except Exception:
                # Mark this batch as failed
                self.failed_to_fetch += len(batch_uris)
                self.unmapped_uris.extend(batch_uris)

    def _process_valid_elements_response(
        self, response: dict[str, Any], requested_uris: list[str]
    ) -> None:
        """
        Process the validElements API response and populate cache.

        Args:
            response: The API response containing element data
            requested_uris: List of URIs that were requested
        """
        fetched_count = 0
        fetched_uris = set()

        # Process items with titles
        items = response.get("validElements", {}).get("items", [])
        for item in items:
            element = item.get("element", {})
            uri = element.get("uri")
            title = element.get("title")

            if uri and title is not None:
                # Store to cache (same structure as validation cache)
                self.platform_client.cache_attribute_elements[uri] = title
                fetched_uris.add(uri)
                fetched_count += 1

        # Track URIs that weren't returned or don't have titles
        failed_uris = [uri for uri in requested_uris if uri not in fetched_uris]
        failed_count = len(failed_uris)

        if failed_uris:
            self.unmapped_uris.extend(failed_uris)

        self.successfully_fetched += fetched_count
        self.failed_to_fetch += failed_count

    def get_statistics(self) -> dict[str, Any]:
        """
        Get statistics about the prefetching process.

        Returns:
            Dictionary with statistics
        """
        return {
            "total_attributes": len(self.elements_by_attribute),
            "total_elements": sum(
                len(uris) for uris in self.elements_by_attribute.values()
            ),
            "successfully_fetched": self.successfully_fetched,
            "failed_to_fetch": self.failed_to_fetch,
            "unmapped_uris": len(self.unmapped_uris),
        }

    def _group_unmapped_by_attribute(self) -> dict[str, list[str]]:
        """
        Group unmapped element URIs by their attribute URI.

        Returns:
            Dictionary mapping attribute_uri -> list of element_uris
        """
        unmapped_by_attribute = defaultdict(list)
        for uri in self.unmapped_uris:
            match = self.element_uri_pattern.match(uri)
            if match:
                attribute_uri = match.group(1)
                unmapped_by_attribute[attribute_uri].append(uri)
        return unmapped_by_attribute

    def _get_sanitized_attribute_title(
        self, attribute_uri: str, attribute_id: str
    ) -> str:
        """
        Get and sanitize attribute title for use in metric names.

        Args:
            attribute_uri: URI of the attribute
            attribute_id: ID of the attribute (used as fallback)

        Returns:
            Sanitized attribute title safe for use in metric names
        """
        try:
            attribute_obj = self.platform_client.get_object(attribute_uri)
            title = (
                attribute_obj.get("attribute", {})
                .get("meta", {})
                .get("title", f"attr_{attribute_id}")
            )
            # Sanitize: replace non-alphanumeric characters with underscores
            return re.sub(r"[^a-zA-Z0-9_]", "_", title)
        except Exception:
            return f"attr_{attribute_id}"

    def _build_metric_name(
        self, attribute_title: str, attribute_id: str, page: int, total_pages: int
    ) -> str:
        """
        Build metric name with optional pagination suffix.

        Args:
            attribute_title: Sanitized attribute title
            attribute_id: Attribute object ID
            page: Current page number (0-based)
            total_pages: Total number of pages

        Returns:
            Formatted metric name
        """
        if total_pages > 1:
            return (
                f"__migration_elements_{attribute_title}_{attribute_id}_page{page + 1}"
            )
        return f"__migration_elements_{attribute_title}_{attribute_id}"

    def _create_single_metric(
        self,
        metric_name: str,
        attribute_uri: str,
        element_uris: list[str],
        metric_index: int,
    ) -> bool:
        """
        Create a single metric for a batch of element URIs.

        Args:
            metric_name: Name for the metric
            attribute_uri: URI of the attribute
            element_uris: List of element URIs to include in the metric
            metric_index: 1-based index of this metric

        Returns:
            True if metric was created successfully, False otherwise
        """
        logger.info(
            "Creating %s: %s: %s elements ..",
            metric_index,
            metric_name,
            len(element_uris),
        )

        try:
            element_refs = ", ".join(f"[{uri}]" for uri in element_uris)
            maql_expression = f"SELECT 1 WHERE [{attribute_uri}] IN ({element_refs})"

            metric_uri = self._create_platform_metric(metric_name, maql_expression)
            self.created_metric_uris.append(metric_uri)
            logger.info("Done")
            return True
        except Exception as e:
            logger.error("ERROR: %s", e)
            return False

    def _create_metrics_for_attribute(
        self, attribute_uri: str, element_uris: list[str], metric_counter: list[int]
    ) -> int:
        """
        Create metrics for a single attribute's unmapped elements.

        Args:
            attribute_uri: The attribute URI (e.g., "/gdc/md/workspace/obj/123")
            element_uris: List of element URIs for this attribute
            metric_counter: List with single integer for tracking global metric count

        Returns:
            Number of metrics successfully created
        """
        if not element_uris:
            return 0

        # Extract attribute ID from URI for metric naming (e.g., "/gdc/md/ws/obj/123" -> "123")
        attribute_id = attribute_uri.rstrip("/").split("/")[-1]

        # Get sanitized attribute title for metric naming
        attribute_title = self._get_sanitized_attribute_title(
            attribute_uri, attribute_id
        )

        # Calculate pagination
        num_metrics = (
            len(element_uris) + ELEMENTS_PER_METRIC - 1
        ) // ELEMENTS_PER_METRIC

        # Create metrics in batches
        metrics_created = 0
        for page in range(num_metrics):
            start_idx = page * ELEMENTS_PER_METRIC
            end_idx = min(start_idx + ELEMENTS_PER_METRIC, len(element_uris))
            batch_uris = element_uris[start_idx:end_idx]

            metric_name = self._build_metric_name(
                attribute_title, attribute_id, page, num_metrics
            )

            metric_counter[0] += 1
            if self._create_single_metric(
                metric_name, attribute_uri, batch_uris, metric_counter[0]
            ):
                metrics_created += 1

        return metrics_created

    def create_metrics_for_unmapped_elements(self) -> None:
        """
        Create Platform metrics for unmapped elements to enable validation-based lookup.

        For each attribute with unmapped elements, creates metrics with expressions
        that reference those elements. When Platform validates these metrics, it returns
        the element values, which can then be cached.

        Metrics are created with up to ELEMENTS_PER_METRIC elements per metric.
        """
        if not self.unmapped_uris:
            return

        # Group unmapped URIs by attribute
        unmapped_by_attribute = self._group_unmapped_by_attribute()
        num_attributes = len(unmapped_by_attribute)

        logger.info(
            "----Creating Platform metrics for unmapped attributes (%s)----",
            num_attributes,
        )

        # Create metrics for each attribute (using list to track global count)
        metric_counter = [0]
        total_metrics_created = 0
        for attribute_uri, element_uris in unmapped_by_attribute.items():
            metrics_created = self._create_metrics_for_attribute(
                attribute_uri, element_uris, metric_counter
            )
            total_metrics_created += metrics_created

        logger.info("Created %s metric(s) for validation", total_metrics_created)

    def _create_platform_metric(self, title: str, expression: str) -> str:
        """
        Create a Platform metric with the given title and MAQL expression.

        Args:
            title: Metric title
            expression: MAQL expression

        Returns:
            URI of the created metric
        """
        url = f"{self.platform_client.domain}/gdc/md/{self.platform_client.pid}/obj"
        payload = {
            "metric": {
                "meta": {
                    "title": title,
                    "summary": "",
                    "tags": "",
                    "deprecated": 0,
                    "unlisted": 1,
                },
                "content": {
                    "expression": expression,
                    "format": "#,##0.00",
                    "folders": [],
                },
            }
        }

        response = self.platform_client._post(url, payload)
        response_data = response.json()
        metric_uri = response_data.get("uri")

        if not metric_uri:
            raise Exception("Failed to create metric - no URI returned")

        return metric_uri

    def delete_created_metrics(self) -> None:
        """
        Delete all metrics that were created during the prefetch process.
        """
        if not self.created_metric_uris:
            return

        logger.info(
            "----Deleting created Platform metrics (%s)----",
            len(self.created_metric_uris),
        )

        for idx, metric_uri in enumerate(self.created_metric_uris, 1):
            logger.info("Deleting %s: %s ..", idx, metric_uri)
            try:
                url = f"{self.platform_client.domain}{metric_uri}"
                self.platform_client._delete(url)
                logger.info("Done")
            except Exception as e:
                logger.error("ERROR: %s", e)

        # Clear the list after deletion
        self.created_metric_uris.clear()
