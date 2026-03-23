# (C) 2026 GoodData Corporation
"""
Ranking filter class for Platform to Cloud filter migration.

This module provides a class for handling ranking filters (top/bottom N).
"""

import logging

from gooddata_platform2cloud.reports.common import (
    contains_node_type,
    find_number_in_tree,
)
from gooddata_platform2cloud.reports.filters.base_filter import Filter
from gooddata_platform2cloud.reports.filters.date_helpers import (
    contains_date_range_recursively,
)
from gooddata_platform2cloud.reports.filters.helpers import (
    attribute_has_displayed_form,
    check_exact_granularity_match,
    get_display_form_identifiers,
    get_displayed_form_identifier,
    get_metric_identifier_for_warnings,
    has_attributes_in_buckets,
)

logger = logging.getLogger("migration")


class RankingFilter(Filter):
    """
    Class for handling ranking filters (top/bottom N).
    """

    def extract_ranking_data(self, tree):
        """
        Extract ranking data from a ranking filter tree.

        Args:
            tree (dict): The filter tree object

        Returns:
            tuple: (direction, limit, attribute_uris, measure_uri, has_variable, has_date_range) for the filter
        """
        direction = "top" if tree.get("type") == "top" else "bottom"
        limit = None
        attribute_uris = []  # Modified to collect multiple attributes
        measure_uri = None
        has_variable = False
        has_date_range = False

        for node in tree.get("content", []):
            if node.get("type") == "count":
                limit = int(float(node.get("value", 10)))
                break
            elif node.get("type") == "number":
                limit = int(float(node.get("value", 10)))
                break

        # If we didn't find a direct number, look deeper
        if limit is None:
            limit = find_number_in_tree(tree)
            if limit is not None:
                limit = int(limit)

        # Default to 10 if no limit found
        if limit is None:
            limit = 10

        # Extract attribute and measure URIs and check for variables and date ranges
        for node in tree.get("content", []):
            if node.get("type") == "metric":
                # Process metric and look for variables
                for metric_child in node.get("content", []):
                    if metric_child.get("type") == "expression":
                        for expr_child in metric_child.get("content", []):
                            if expr_child.get("type") == "metric object":
                                measure_uri = expr_child.get("value")
                    elif metric_child.get("type") == "by":
                        # Collect all attributes in the BY clause
                        for by_child in metric_child.get("content", []):
                            if by_child.get("type") == "attribute object":
                                attribute_uris.append(by_child.get("value"))
                    elif metric_child.get("type") == "where":
                        # Check the entire WHERE clause recursively for date ranges and variables
                        if contains_date_range_recursively(metric_child):
                            has_date_range = True

                        # Check for variables (prompt objects) recursively
                        if contains_node_type(metric_child, "prompt object"):
                            has_variable = True
            elif node.get("type") == "by":
                # Collect all attributes in a top-level BY clause
                for child in node.get("content", []):
                    if child.get("type") == "attribute object":
                        attribute_uris.append(child.get("value"))
            elif node.get("type") == "of":
                for child in node.get("content", []):
                    if child.get("type") == "metric object":
                        measure_uri = child.get("value")

        return (
            direction,
            limit,
            attribute_uris,
            measure_uri,
            has_variable,
            has_date_range,
        )

    def get_metric_identifier_and_ranking_expression(
        self, measure_uri, direction, limit
    ):
        """
        Get the metric identifier and ranking filter expression for warning messages.

        Args:
            measure_uri (str): The measure URI
            direction (str): The ranking direction (top/bottom)
            limit (int): The ranking limit number

        Returns:
            tuple: (metric_identifier, filter_expression) for use in warning messages
        """
        # Use shared helper to get metric identifier
        metric_identifier = get_metric_identifier_for_warnings(self.ctx, measure_uri)

        # Build the filter expression for the warning message
        filter_expression = ""
        try:
            if direction and limit:
                filter_expression = f" {direction.upper()} {limit}"
        except Exception:
            pass

        return metric_identifier, filter_expression

    def process(
        self, filter_obj, metric_local_ids=None, displayed_attributes=None, **kwargs
    ):
        """
        Process a ranking filter from Platform to Cloud format.

        Args:
            filter_obj (dict): The Platform filter object
            metric_local_ids (dict): Dictionary mapping metric URIs to local IDs
            displayed_attributes (set): Set of displayed attribute URIs
            **kwargs: Additional arguments (may include 'buckets' for bucket validation)

        Returns:
            tuple: (cloud_filter, filter_config) with the converted filter
        """
        if metric_local_ids is None:
            metric_local_ids = {}

        if displayed_attributes is None:
            displayed_attributes = set()

        # Check if buckets are provided and contain attributes
        buckets = kwargs.get("buckets", [])
        if buckets and not has_attributes_in_buckets(buckets):
            # Extract ranking data to get metric info for the warning
            tree = filter_obj.get("tree", {})
            direction, limit, _, measure_uri, _, _ = self.extract_ranking_data(tree)

            # Get metric identifier and expression for warning message
            if measure_uri and direction and limit:
                metric_identifier, filter_expression = (
                    self.get_metric_identifier_and_ranking_expression(
                        measure_uri, direction, limit
                    )
                )
                warning_message = f"FILTER REMOVED: top/bottom filter only supported with at least one attribute in the buckets: ({metric_identifier}{filter_expression})"
            else:
                warning_message = "FILTER REMOVED: top/bottom filter only supported with at least one attribute in the buckets"

            self.log_warning(warning_message, to_stderr=True)
            return None, {}

        tree = filter_obj.get("tree", {})

        # Extract ranking data - now returns a list of attribute URIs
        direction, limit, attribute_uris, measure_uri, has_variable, has_date_range = (
            self.extract_ranking_data(tree)
        )

        # Get metric identifier and expression for warning messages (used by multiple warnings)
        if measure_uri and direction and limit:
            metric_identifier, filter_expression = (
                self.get_metric_identifier_and_ranking_expression(
                    measure_uri, direction, limit
                )
            )
        else:
            metric_identifier, filter_expression = "unknown_metric", ""

        # Log warnings for unsupported features
        if has_variable:
            self.log_warning(
                f"FILTER MODIFIED: variables inside top/bottom filters not supported: ({metric_identifier}{filter_expression})",
                to_stderr=True,
            )

        if has_date_range:
            self.log_warning(
                f"FILTER MODIFIED: date ranges inside top/bottom filters not supported: ({metric_identifier}{filter_expression})",
                to_stderr=True,
            )

        # Check for additional attribute filters inside top/bottom filters
        if contains_node_type(tree, "attributeElement object"):
            self.log_warning(
                f"FILTER MODIFIED: attribute filters inside top/bottom filters not supported: ({metric_identifier}{filter_expression})",
                to_stderr=True,
            )

        # Check that we have required components - measure is required, attributes are optional
        if not measure_uri:
            self.log_warning(
                f"FILTER REMOVED: Missing measure in {direction} filter", to_stderr=True
            )
            return None, {}

        try:
            # Extract measure identifier and get its local ID
            measure_obj = self.ctx.platform_client.get_object(measure_uri)
            platform_measure_identifier = measure_obj["metric"]["meta"]["identifier"]

            # Check if the measure is displayed in the visualization
            measure_is_displayed = False
            for uri, local_id in metric_local_ids.items():
                try:
                    # Get the Platform metric object for comparison
                    displayed_metric_obj = self.ctx.platform_client.get_object(uri)
                    displayed_platform_identifier = displayed_metric_obj["metric"][
                        "meta"
                    ]["identifier"]
                    if displayed_platform_identifier == platform_measure_identifier:
                        measure_is_displayed = True
                        break
                except Exception:
                    continue

            # If the measure isn't displayed, log a specific warning and return None
            if not measure_is_displayed:
                self.log_warning(
                    f"FILTER REMOVED: top/bottom filter can only be applied to a metric displayed in visualization: ({metric_identifier}{filter_expression})",
                    to_stderr=True,
                )
                return None, {}

            # Get the local ID for the measure
            if platform_measure_identifier in metric_local_ids:
                # Use the provided local ID from the context
                measure_local_id = metric_local_ids[platform_measure_identifier]
            else:
                # Look up the identifier in the metric mappings
                measure_local_id = self.ctx.metric_mappings.search_mapping_identifier(
                    platform_measure_identifier
                )

            # Handle ranking filter granularity according to Cloud limitations
            matching_df_id = None

            # Case 1: No attributes in Platform ranking filter - use empty granularity in Cloud
            if not attribute_uris:
                # No attributes in Platform, no attributes in Cloud - this is fine
                matching_df_id = None

            # Case 2: Exactly one attribute in Platform ranking filter
            elif len(attribute_uris) == 1:
                attribute_uri = attribute_uris[0]
                try:
                    # Validate the attribute - especially important for date attributes
                    attr_obj = self.ctx.platform_client.get_object(attribute_uri)

                    # Extract attribute identifier
                    attribute_identifier = None
                    if "attributeDisplayForm" in attr_obj:
                        attribute_identifier = attr_obj["attributeDisplayForm"]["meta"][
                            "identifier"
                        ]
                    elif "attribute" in attr_obj:
                        attribute_identifier = attr_obj["attribute"]["meta"][
                            "identifier"
                        ]

                    # Check if this attribute has any display form displayed in the visualization
                    if attribute_has_displayed_form(
                        self.ctx, attribute_uri, displayed_attributes
                    ):
                        # Get the identifier of the specific display form that is displayed
                        matching_df_id = get_displayed_form_identifier(
                            self.ctx, attribute_uri, displayed_attributes
                        )
                        if not matching_df_id:
                            # Fallback to primary display form if specific one not found
                            # This happens when displayed_attributes contains the attribute URI directly
                            # rather than a display form URI
                            primary_df_id, original_df_id, original_df_type = (
                                get_display_form_identifiers(self.ctx, attribute_uri)
                            )
                            matching_df_id = primary_df_id
                    else:
                        # The attribute is not displayed, use global ranking with warning
                        # Get attribute identifier for warning
                        attr_id = self.ctx.ldm_mappings.search_mapping_identifier(
                            attribute_identifier
                        )
                        display_attr_name = attr_id if attr_id else attribute_identifier

                        self.log_warning(
                            f"FILTER MODIFIED: Ranking filter attribute ({display_attr_name}) not displayed in visualization. Using global ranking: ({metric_identifier}{filter_expression})",
                            to_stderr=True,
                        )
                        matching_df_id = None

                except Exception:
                    # If there's an error processing the single attribute, use global ranking
                    self.log_warning(
                        f"FILTER MODIFIED: Error processing ranking filter attribute. Using global ranking: ({metric_identifier}{filter_expression})",
                        to_stderr=True,
                    )
                    matching_df_id = None

            # Case 3: Multiple attributes in Platform ranking filter - check for exact granularity match
            else:
                # Check if filter attributes exactly match visualization attributes
                is_exact_match, filter_identifiers, viz_identifiers = (
                    check_exact_granularity_match(
                        self.ctx, attribute_uris, displayed_attributes
                    )
                )

                if is_exact_match:
                    # Special case: exact granularity match - safe to migrate without granularity
                    filter_attrs_str = ", ".join(filter_identifiers)
                    viz_attrs_str = ", ".join(viz_identifiers)

                    # Log INFO to console (not to visualization)
                    logger.info(
                        "Ranking filter granularity (%s) exactly matching visualization granularity (%s) - safe to migrate without granularity: (%s%s)",
                        filter_attrs_str,
                        viz_attrs_str,
                        metric_identifier,
                        filter_expression,
                    )

                    # Use empty granularity without warning
                    matching_df_id = None
                else:
                    # Multiple attributes that don't match exactly - show warning

                    # Process each attribute to get Cloud identifiers for the warning message
                    attr_identifiers = []
                    for attribute_uri in attribute_uris:
                        try:
                            attr_obj = self.ctx.platform_client.get_object(
                                attribute_uri
                            )
                            attr_identifier = (
                                attr_obj.get("attribute", {})
                                .get("meta", {})
                                .get("identifier", "")
                            )
                            if attr_identifier:
                                attr_id = (
                                    self.ctx.ldm_mappings.search_mapping_identifier(
                                        attr_identifier
                                    )
                                )
                                if attr_id:
                                    attr_identifiers.append(attr_id)
                                else:
                                    attr_identifiers.append(attr_identifier)
                            else:
                                attr_identifiers.append("unknown_attribute")
                        except Exception:
                            attr_identifiers.append("unknown_attribute")

                    # Format attribute identifiers as comma-separated string
                    attrs_str = (
                        ", ".join(attr_identifiers)
                        if attr_identifiers
                        else "unknown_attributes"
                    )

                    self.log_warning(
                        f"FILTER MODIFIED: Multiple attributes in ranking filter not supported. Using global ranking: ({metric_identifier}{filter_expression}, was: {attrs_str})",
                        to_stderr=True,
                    )
                    # Set matching_df_id to None to create filter without attributes
                    matching_df_id = None

            # Create the ranking filter with the format expected in the output
            cloud_filter = {
                "rankingFilter": {
                    "measure": {"localIdentifier": measure_local_id},
                    "operator": direction.upper(),
                    "value": limit,
                }
            }

            # Add attributes only if we have them
            if matching_df_id:
                cloud_filter["rankingFilter"]["attributes"] = [
                    {"localIdentifier": matching_df_id}
                ]

            return cloud_filter, {}

        except Exception as e:
            self.log_warning(
                f"Error creating ranking filter: {str(e)}", to_stderr=False
            )
            return None, {}
