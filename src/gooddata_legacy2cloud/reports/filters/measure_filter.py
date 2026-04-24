# (C) 2026 GoodData Corporation
"""
Measure filter class for Legacy to Cloud filter migration.

This module provides a class for handling measure value filters.
"""

from gooddata_legacy2cloud.models.cloud.identifier import (
    Identifier,
    IdentifierWrapper,
)
from gooddata_legacy2cloud.models.cloud.insight_filters import (
    Comparison,
    ComparisonWrapper,
    MeasureFilterModel,
    MeasureValueFilterWrapper,
)
from gooddata_legacy2cloud.reports.common import (
    find_metric_object_value,
    find_number_in_tree,
)
from gooddata_legacy2cloud.reports.filters.base_filter import Filter
from gooddata_legacy2cloud.reports.filters.date_helpers import (
    contains_date_range_recursively,
)
from gooddata_legacy2cloud.reports.filters.helpers import (
    get_metric_identifier_for_warnings,
)


class MeasureFilter(Filter):
    """
    Class for handling measure value filters (>, <, =, etc.).
    """

    OPERATOR_MAPPING = {
        ">": "GREATER_THAN",
        ">=": "GREATER_THAN_OR_EQUAL_TO",
        "<": "LESS_THAN",
        "<=": "LESS_THAN_OR_EQUAL_TO",
        "=": "EQUAL_TO",
        "<>": "NOT_EQUAL_TO",
    }

    def extract_measure_data(self, tree):
        """
        Extract measure URI and comparison value from a measure filter tree.

        Args:
            tree (dict): The filter tree object

        Returns:
            tuple: (measure_uri, operator, comparison_value, has_granularity, has_variable) for the filter
        """
        operator = tree.get("type")
        measure_uri = None
        comparison_value = None
        has_granularity = False
        has_variable = False

        # Check if there's a "by" node in the content, which indicates granularity
        # Also check for "where" with "prompt object", which indicates a variable filter
        for node in tree.get("content", []):
            if node.get("type") == "metric":
                # Found a metric wrapper, now check for "by" or "where" within it
                for metric_child in node.get("content", []):
                    if metric_child.get("type") == "by":
                        has_granularity = True
                    elif metric_child.get("type") == "where":
                        # Check if there's a prompt object in the where clause
                        for where_child in metric_child.get("content", []):
                            if where_child.get("type") == "prompt object":
                                has_variable = True
                    elif metric_child.get("type") == "expression":
                        # Look for the metric inside the expression
                        for expr_child in metric_child.get("content", []):
                            if expr_child.get("type") == "metric object":
                                measure_uri = expr_child.get("value")
            elif node.get("type") == "number":
                comparison_value = extract_numeric_value(node)

        # If we didn't find the measure URI in the structured search, try direct approach
        if not measure_uri:
            # Handle the case when the measure is on the left side of the operator
            for i, node in enumerate(tree.get("content", [])):
                if i == 0 and node.get("type") == "metric object":
                    measure_uri = node.get("value")
                elif (
                    i == 1
                    and measure_uri
                    and (node.get("type") == "number" or contains_number(node))
                ):
                    comparison_value = extract_numeric_value(node)

            # Handle the case when the measure is on the right side of the operator
            if measure_uri is None:
                for i, node in enumerate(tree.get("content", [])):
                    if i == 1 and node.get("type") == "metric object":
                        measure_uri = node.get("value")
                        # Flip the operator when the measure is on the right
                        operator = flip_operator(operator)
                    elif i == 0 and (
                        node.get("type") == "number" or contains_number(node)
                    ):
                        comparison_value = extract_numeric_value(node)

        return measure_uri, operator, comparison_value, has_granularity, has_variable

    def detect_granularity_attributes(self, tree):
        """
        Detect granularity attributes in the measure filter.

        Args:
            tree (dict): The filter tree object

        Returns:
            list[str]: List of attribute URIs found in the granularity, empty list if none found
        """
        granularity_attrs = []
        for node in tree.get("content", []):
            if node.get("type") == "metric":
                for metric_child in node.get("content", []):
                    if metric_child.get("type") == "by":
                        for by_child in metric_child.get("content", []):
                            if by_child.get("type") == "attribute object":
                                granularity_attrs.append(by_child.get("value"))
        return granularity_attrs

    def _map_legacy_attribute_uri_to_cloud_id(self, attribute_uri: str) -> str | None:
        """Map a Legacy attribute URI to a Cloud identifier."""
        try:
            obj = self.ctx.legacy_client.get_object(attribute_uri)
            legacy_identifier: str | None = None

            if "attribute" in obj and "meta" in obj["attribute"]:
                legacy_identifier = obj["attribute"]["meta"].get("identifier")
            elif (
                "attributeDisplayForm" in obj and "meta" in obj["attributeDisplayForm"]
            ):
                legacy_identifier = obj["attributeDisplayForm"]["meta"].get(
                    "identifier"
                )

            if not legacy_identifier:
                legacy_identifier = attribute_uri

            mapped = self.ctx.ldm_mappings.search_mapping_identifier(legacy_identifier)
            return mapped if mapped else legacy_identifier
        except Exception:
            # Fallback to the URI itself, consistent with report attribute mapping fallbacks
            return attribute_uri

    def extract_filter_components(self, filter_obj, metric_local_ids):
        """
        Extract all necessary components from a filter object.

        Args:
            filter_obj (dict): The Legacy filter object
            metric_local_ids (dict): Dictionary mapping metric URIs to local IDs

        Returns:
            tuple: (measure_uri, operator, comparison_value, has_granularity, has_variable, granularity_attrs)
                  or (None, None, None, False, False, []) if extraction fails
        """
        tree = filter_obj.get("tree", {})

        # Extract measure URI, operator, and comparison value
        measure_uri, operator, comparison_value, has_granularity, has_variable = (
            self.extract_measure_data(tree)
        )

        # Check for granularity attributes
        granularity_attrs = self.detect_granularity_attributes(tree)

        if not measure_uri or comparison_value is None:
            # If either is missing, try to look up values from other fields
            measure_uri = find_metric_object_value(tree)
            comparison_value = find_number_in_tree(tree)

        # If we still don't have both values, we can't create the filter
        if not measure_uri or comparison_value is None:
            self.log_warning(
                "FILTER REMOVED: Could not extract measure URI or comparison value from filter",
                to_stderr=True,
            )
            return None, None, None, False, False, []

        return (
            measure_uri,
            operator,
            comparison_value,
            has_granularity,
            has_variable,
            granularity_attrs,
        )

    def get_measure_cloud_id(self, measure_uri, metric_cloud_ids):
        """
        Get the Cloud ID for a measure.

        Args:
            measure_uri (str): The measure URI
            metric_cloud_ids (dict): Dictionary mapping metric URIs to Cloud IDs

        Returns:
            tuple: (measure_local_id, legacy_identifier) or (measure_uri, None) if an error occurs
        """
        try:
            # Get the metric object to extract the identifier
            metric_obj = self.ctx.legacy_client.get_object(measure_uri)
            legacy_identifier = metric_obj["metric"]["meta"]["identifier"]

            # Get the local ID for the metric
            if legacy_identifier in metric_cloud_ids:
                # Use the provided local ID from the context
                measure_cloud_id = metric_cloud_ids[legacy_identifier]
            else:
                # Look up the identifier in the metric mappings
                measure_cloud_id = self.ctx.metric_mappings.search_mapping_identifier(
                    legacy_identifier
                )

            return measure_cloud_id
        except Exception as e:
            # If we can't find the mapping, use the URI as the ID
            self.log_warning(
                f"Error mapping measure URI to local ID: {str(e)}", to_stderr=False
            )
            return measure_uri, None

    def get_metric_identifier_and_expression(
        self, measure_uri, operator, comparison_value
    ):
        """
        Get the metric identifier and filter expression for warning messages.

        Args:
            measure_uri (str): The measure URI
            operator (str): The filter operator
            comparison_value (float): The comparison value

        Returns:
            tuple: (metric_identifier, filter_expression) for use in warning messages
        """
        # Use shared helper to get metric identifier
        metric_identifier = get_metric_identifier_for_warnings(self.ctx, measure_uri)

        # Build the filter expression for the warning message
        filter_expression = ""
        try:
            if operator and comparison_value is not None:
                filter_expression = f" {operator} {comparison_value}"
        except Exception:
            pass

        return metric_identifier, filter_expression

    def create_measure_value_filter(
        self,
        measure_cloud_id: str,
        operator: str,
        comparison_value: float,
        dimensionality: list[str] | None = None,
    ) -> MeasureValueFilterWrapper:
        """
        Create a measure value filter in Cloud format using Pydantic models.

        Args:
            measure_cloud_id (str): The Cloud ID of the measure
            operator (str): The filter operator
            comparison_value (float): The comparison value
            dimensionality (list[str] | None): Optional list of attribute identifiers

        Returns:
            MeasureValueFilterWrapper: The Cloud measure value filter model
        """
        cloud_operator = self.OPERATOR_MAPPING.get(operator, "EQUAL_TO")

        filter_dimensionality: list[IdentifierWrapper] | None = None
        if dimensionality is not None:
            filter_dimensionality = [
                IdentifierWrapper(identifier=Identifier(id=lid, type="label"))
                for lid in dimensionality
            ]

        filter_model = MeasureValueFilterWrapper(
            measure_value_filter=MeasureFilterModel(
                measure=IdentifierWrapper(
                    identifier=Identifier(id=measure_cloud_id, type="metric")
                ),
                condition=ComparisonWrapper(
                    comparison=Comparison(
                        operator=cloud_operator, value=comparison_value
                    )
                ),
                dimensionality=filter_dimensionality,
            )
        )

        return filter_model

    def process(self, filter_obj, metric_local_ids=None, **kwargs):
        """
        Process a measure value filter from Legacy to Cloud format.

        Args:
            filter_obj (dict): The Legacy filter object
            metric_local_ids (dict): Dictionary mapping metric URIs to local IDs
            **kwargs: Additional arguments

        Returns:
            tuple: (cloud_filter, filter_config) with the converted filter
        """
        if metric_local_ids is None:
            metric_local_ids = {}

        # Extract filter components
        (
            measure_uri,
            operator,
            comparison_value,
            has_granularity,
            has_variable,
            granularity_attrs,
        ) = self.extract_filter_components(filter_obj, metric_local_ids)

        if measure_uri is None:
            return None, {}

        # Get metric identifier and expression for warning messages (used by multiple warnings)
        metric_identifier, filter_expression = (
            self.get_metric_identifier_and_expression(
                measure_uri, operator, comparison_value
            )
        )

        # Get the local ID for the measure
        measure_cloud_id = self.get_measure_cloud_id(measure_uri, metric_local_ids)

        # Check for unsupported features in measure value filters

        def _contains_variable_recursively(node):
            """Recursively check if a node contains variable indicators (prompt objects)."""
            if not isinstance(node, dict):
                return False

            # Check if this node itself is a prompt object
            if node.get("type") == "prompt object":
                return True

            # Recursively check all children
            for child in node.get("content", []):
                if _contains_variable_recursively(child):
                    return True

            return False

        def _contains_attribute_element_recursively(node):
            """Recursively check if a node contains attributeElement objects."""
            if not isinstance(node, dict):
                return False

            # Check if this node itself is an attributeElement object
            if node.get("type") == "attributeElement object":
                return True

            # Recursively check all children
            for child in node.get("content", []):
                if _contains_attribute_element_recursively(child):
                    return True

            return False

        tree = filter_obj.get("tree", {})

        # Check for variables (use both extract_measure_data result and recursive check)
        if has_variable or _contains_variable_recursively(tree):
            self.log_warning(
                f"FILTER MODIFIED: variables inside Metric value filters not supported: ({metric_identifier}{filter_expression})",
                to_stderr=True,
            )

        # Check for date ranges
        if contains_date_range_recursively(tree):
            self.log_warning(
                f"FILTER MODIFIED: date ranges inside Metric value filters not supported: ({metric_identifier}{filter_expression})",
                to_stderr=True,
            )

        # Check for attribute elements
        if _contains_attribute_element_recursively(tree):
            self.log_warning(
                f"FILTER MODIFIED: additional attribute filters inside Metric value filters not supported: ({metric_identifier}{filter_expression})",
                to_stderr=True,
            )

        dimensionality: list[str] | None = None
        if granularity_attrs:
            # Map all granularity attributes to Cloud identifiers
            mapped_ids = [
                self._map_legacy_attribute_uri_to_cloud_id(uri)
                for uri in granularity_attrs
            ]
            dimensionality = [mid for mid in mapped_ids if mid]
            if not dimensionality:
                dimensionality = None

        # Create and return the measure value filter
        cloud_filter = self.create_measure_value_filter(
            measure_cloud_id,
            operator,
            comparison_value,
            dimensionality=dimensionality,
        )
        return cloud_filter.model_dump(by_alias=True, exclude_none=True), {}


def contains_number(node):
    """
    Check if a node contains a number.

    Args:
        node (dict): The tree node to check

    Returns:
        bool: True if the node contains a number, False otherwise
    """
    if node.get("type") == "number":
        return True

    for child in node.get("content", []):
        if child.get("type") == "number":
            return True

    return False


def extract_numeric_value(node):
    """
    Extract a numeric value from a node.

    Args:
        node (dict): The tree node to extract from

    Returns:
        float: The extracted numeric value, or None if not found
    """
    if node.get("type") == "number":
        try:
            return float(node.get("value", 0))
        except ValueError, TypeError:
            return 0

    for child in node.get("content", []):
        if child.get("type") == "number":
            try:
                return float(child.get("value", 0))
            except ValueError, TypeError:
                return 0

    return None


def flip_operator(operator):
    """
    Flip a comparison operator (for when measure is on the right side).

    Args:
        operator (str): The operator to flip

    Returns:
        str: The flipped operator
    """
    operator_flips = {">": "<", ">=": "<=", "<": ">", "<=": ">=", "=": "=", "<>": "<>"}

    return operator_flips.get(operator, operator)
