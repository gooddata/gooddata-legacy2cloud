# (C) 2026 GoodData Corporation
"""
Attribute filter class for Platform to Cloud filter migration.

This module provides classes for handling attribute filters (positive and negative).
"""

from gooddata_platform2cloud.reports.common import generate_local_id
from gooddata_platform2cloud.reports.filters.base_filter import Filter
from gooddata_platform2cloud.reports.filters.date_helpers import is_date_attribute
from gooddata_platform2cloud.reports.filters.helpers import (
    convert_attribute_elements,
    get_display_form_identifiers,
)


class AttributeFilter(Filter):
    """
    Base class for attribute filters (positive and negative).
    """

    def get_attribute_identifier(self, attr_uri):
        """
        Extract the attribute identifier for better error messages.

        Args:
            attr_uri: The URI of the attribute

        Returns:
            str: The attribute identifier
        """
        try:
            # Get the attribute object to extract the identifier
            attr_obj = self.ctx.platform_client.get_object(attr_uri)
            platform_identifier = None

            if "attribute" in attr_obj:
                platform_identifier = attr_obj["attribute"]["meta"]["identifier"]
            elif "attributeDisplayForm" in attr_obj:
                platform_identifier = attr_obj["attributeDisplayForm"]["meta"][
                    "identifier"
                ]

            if platform_identifier:
                # Convert Platform identifier to Cloud identifier
                attribute_identifier = self.ctx.ldm_mappings.search_mapping_identifier(
                    platform_identifier
                )
            else:
                attribute_identifier = attr_uri  # Fallback if no identifier found
        except Exception as e:
            # If there's an error extracting the identifier, use the URI as fallback
            attribute_identifier = attr_uri
            self.log_warning(
                f"Error extracting attribute identifier: {str(e)}", to_stderr=False
            )

        return attribute_identifier

    def check_date_null_filter(self, attr_uri, values, filter_obj):
        """
        Check if this is a date attribute with a NULL filter (element id=0).

        Args:
            attr_uri: The URI of the attribute
            values: List of element values
            filter_obj: The original filter object

        Returns:
            tuple: (is_date_null, attribute_identifier) indicating if it's a date NULL filter
        """
        is_date_null = False
        attribute_identifier = self.get_attribute_identifier(attr_uri)

        try:
            # Check if it's a date attribute
            if is_date_attribute(self.ctx, attr_uri):
                # Check if any of the element values is id=0, indicating NULL for date attributes
                for val in values:
                    if val.endswith("elements?id=0"):
                        is_date_null = True
                        # Determine if it's within a NOT filter (parent tree is NOT)
                        is_not_null = False
                        if filter_obj.get("tree", {}).get("type") == "not":
                            is_not_null = True

                        # Create more specific warning with attribute identifier
                        filter_type = "NOT NULL" if is_not_null else "IS NULL"
                        self.log_warning(
                            f"FILTER REMOVED: Date {attribute_identifier} {filter_type} filter not supported in Visualizations",
                            to_stderr=True,
                        )
                        break
        except Exception as e:
            self.log_warning(
                f"Error checking if attribute is date: {str(e)}", to_stderr=False
            )

        return is_date_null, attribute_identifier

    def get_original_attribute(self, attr_uri, primary_df_id):
        """
        Get the original attribute object and ID for a display form.

        Args:
            attr_uri: The URI of the attribute
            primary_df_id: The primary display form ID

        Returns:
            str: The original attribute ID
        """
        try:
            obj = self.ctx.platform_client.get_object(attr_uri)
            # Get the original attribute object if this is a display form
            if (
                "attributeDisplayForm" in obj
                and "content" in obj["attributeDisplayForm"]
            ):
                original_attribute_obj = self.ctx.platform_client.get_object(
                    obj["attributeDisplayForm"]["content"]["formOf"]
                )
                original_attribute_id = self.ctx.ldm_mappings.search_mapping_identifier(
                    original_attribute_obj["attribute"]["meta"]["identifier"]
                )
                # Use the original attribute ID for consistent filtering
                return original_attribute_id
        except Exception as e:
            self.log_warning(
                f"Error getting original attribute: {str(e)}", to_stderr=False
            )

        return primary_df_id

    def build_filter_config(
        self, filter_local_id, original_df_id, original_df_type="label"
    ):
        """
        Build the filter configuration for the attribute filter.

        Args:
            filter_local_id: The local ID of the filter
            original_df_id: The original display form ID
            original_df_type: The type of the display form

        Returns:
            dict: The filter configuration
        """
        return {
            filter_local_id: {
                "displayAsLabel": {
                    "identifier": {"id": original_df_id, "type": original_df_type}
                }
            }
        }


class PositiveAttributeFilter(AttributeFilter):
    """
    Class for handling positive attribute filters (IN).
    """

    def extract_attribute_data(self, tree):
        """
        Extract attribute URI and element values from a positive attribute filter tree.

        Args:
            tree (dict): The filter tree object

        Returns:
            tuple: (attr_uri, values) with the attribute URI and list of element values
        """
        attr_uri = None
        values = []

        # Extract attribute URI and element values
        for node in tree.get("content", []):
            if node.get("type") == "attribute object":
                attr_uri = node.get("value")
            elif node.get("type") == "list":
                for item in node.get("content", []):
                    if item.get("type") == "attributeElement object":
                        val = item.get("value")
                        if val is not None:
                            values.append(val)

        return attr_uri, values

    def build_filter(self, filter_local_id, primary_df_id, converted_values):
        """
        Build the positive attribute filter structure.

        Args:
            filter_local_id: The local ID of the filter
            primary_df_id: The primary display form ID
            converted_values: The converted element values

        Returns:
            dict: The positive attribute filter
        """
        return {
            "positiveAttributeFilter": {
                "localIdentifier": filter_local_id,
                "displayForm": {"identifier": {"id": primary_df_id, "type": "label"}},
                "in": {"values": converted_values},
            }
        }

    def process(self, filter_obj, **kwargs):
        """
        Process a positive attribute filter from Platform to Cloud format.

        Args:
            filter_obj (dict): The Platform filter object
            **kwargs: Additional arguments (unused)

        Returns:
            tuple: (cloud_filter, filter_config) with the converted filter
        """
        # Extract attribute URI and values from the filter tree
        attr_uri, values = self.extract_attribute_data(filter_obj.get("tree", {}))

        if not attr_uri:
            return None, {}

        # Check if this is a date attribute with a NULL filter
        is_date_null, attribute_identifier = self.check_date_null_filter(
            attr_uri, values, filter_obj
        )

        # Handle date NULL filters separately
        if is_date_null:
            # This should be implemented in DateFilter class
            # For now, return None to skip this filter
            return None, {}

        # Generate a unique local ID for the filter
        filter_seed = attribute_identifier + "_filter"
        filter_local_id = generate_local_id(filter_seed)

        # Get display form identifiers
        primary_df_id, original_df_id, original_df_type = get_display_form_identifiers(
            self.ctx, attr_uri
        )

        # Get original attribute ID
        updated_primary_df_id = self.get_original_attribute(attr_uri, primary_df_id)

        # Convert attribute elements to their values
        converted_values, missing_elements = convert_attribute_elements(
            self.ctx, values
        )

        if missing_elements:
            self.log_warning(
                f"FILTER MODIFIED: attr filter {original_df_id} IN is missing values: {', '.join(missing_elements)}",
                to_stderr=True,
            )

        # Build the positive attribute filter
        cloud_filter = self.build_filter(
            filter_local_id, updated_primary_df_id, converted_values
        )

        # Build the filter configuration
        filter_config = self.build_filter_config(
            filter_local_id, original_df_id, "label"
        )

        return cloud_filter, filter_config


class NegativeAttributeFilter(AttributeFilter):
    """
    Class for handling negative attribute filters (NOT IN).
    """

    def _extract_from_content(self, content):
        """
        Helper method to extract attribute URI and element values from content array.

        Args:
            content (list): The content array to extract from

        Returns:
            tuple: (attr_uri, values) with the attribute URI and list of element values
        """
        attr_uri = None
        values = []

        for node in content:
            if node.get("type") == "attribute object":
                attr_uri = node.get("value")
            elif node.get("type") == "list":
                for item in node.get("content", []):
                    if item.get("type") == "attributeElement object":
                        val = item.get("value")
                        if val is not None:
                            values.append(val)

        return attr_uri, values

    def extract_attribute_data(self, tree):
        """
        Extract attribute URI and element values from a negative attribute filter tree.

        Supports both formats:
        1. Unary NOT: NOT (A IN B) where tree.type = "not"
        2. Binary NOT IN: A NOT IN B where tree.type = "not in"

        Args:
            tree (dict): The filter tree object

        Returns:
            tuple: (attr_uri, values) with the attribute URI and list of element values
        """
        filter_type = tree.get("type")

        # Handle binary "not in" format: A NOT IN B
        if filter_type == "not in":
            # Content directly contains attribute and list
            return self._extract_from_content(tree.get("content", []))

        # Handle unary "not" format: NOT (A IN B)
        elif filter_type == "not":
            # Navigate to find the "in" node within parentheses
            for child in tree.get("content", []):
                if child.get("type") == "()":
                    for inner_child in child.get("content", []):
                        if inner_child.get("type") == "in":
                            # Extract from the "in" node content
                            return self._extract_from_content(
                                inner_child.get("content", [])
                            )

            # If no valid structure found, return empty
            return None, []

        # Unknown format
        return None, []

    def build_filter(self, filter_local_id, primary_df_id, converted_values):
        """
        Build the negative attribute filter structure.

        Args:
            filter_local_id: The local ID of the filter
            primary_df_id: The primary display form ID
            converted_values: The converted element values

        Returns:
            dict: The negative attribute filter
        """
        return {
            "negativeAttributeFilter": {
                "localIdentifier": filter_local_id,
                "displayForm": {"identifier": {"id": primary_df_id, "type": "label"}},
                "notIn": {"values": converted_values},
            }
        }

    def process(self, filter_obj, **kwargs):
        """
        Process a negative attribute filter from Platform to Cloud format.

        Args:
            filter_obj (dict): The Platform filter object
            **kwargs: Additional arguments (unused)

        Returns:
            tuple: (cloud_filter, filter_config) with the converted filter
        """
        # Extract attribute URI and values from the filter tree
        attr_uri, values = self.extract_attribute_data(filter_obj.get("tree", {}))

        if not attr_uri:
            return None, {}

        # Get the attribute identifier for a more stable ID
        attribute_identifier = self.get_attribute_identifier(attr_uri)
        filter_seed = attribute_identifier + "_filter"

        # Generate a unique local ID for the filter
        filter_local_id = generate_local_id(filter_seed)

        # Get display form identifiers
        primary_df_id, original_df_id, original_df_type = get_display_form_identifiers(
            self.ctx, attr_uri
        )

        # Get original attribute ID
        updated_primary_df_id = self.get_original_attribute(attr_uri, primary_df_id)

        # Convert attribute elements to their values
        converted_values, missing_elements = convert_attribute_elements(
            self.ctx, values
        )

        if missing_elements:
            self.log_warning(
                f"FILTER MODIFIED: attr filter {original_df_id} NOT IN is missing values: {', '.join(missing_elements)}",
                to_stderr=True,
            )

        # Build the negative attribute filter
        cloud_filter = self.build_filter(
            filter_local_id, updated_primary_df_id, converted_values
        )

        # Build the filter configuration
        filter_config = self.build_filter_config(
            filter_local_id, original_df_id, original_df_type
        )

        return cloud_filter, filter_config
