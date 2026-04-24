# (C) 2026 GoodData Corporation
"""
Date filter class for Legacy to Cloud filter migration.

This module provides a class for handling date filters (between dates, relative dates).
"""

from gooddata_legacy2cloud.models.cloud.identifier import (
    Identifier,
    IdentifierWrapper,
)
from gooddata_legacy2cloud.models.cloud.insight_filters import (
    NegativeAttributeFilter,
    NegativeAttributeFilterContent,
    PositiveAttributeFilter,
    PositiveAttributeFilterContent,
    RelativeDateFilter,
    RelativeDateFilterContent,
    Values,
)
from gooddata_legacy2cloud.reports.common import contains_node_type
from gooddata_legacy2cloud.reports.filters.base_filter import Filter
from gooddata_legacy2cloud.reports.filters.date_classification import (
    _is_cyclical_legacy_attribute,
    classify_date_filter_type,
)
from gooddata_legacy2cloud.reports.filters.date_cyclical_processor import (
    CyclicalDateProcessor,
)
from gooddata_legacy2cloud.reports.filters.date_helpers import (
    get_date_dataset_and_granularity,
)
from gooddata_legacy2cloud.reports.filters.date_ordinal_processor import (
    OrdinalDateProcessor,
)


class DateFilter(Filter):
    """
    Class for handling date filters.
    """

    def _parse_date_node(self, node):
        """
        Parse a date node (-, +, or time macro) and return the calculated value.

        Args:
            node (dict): The date node to parse

        Returns:
            int or None: The calculated date offset value, or None if parsing failed
        """
        node_type = node.get("type")

        if node_type == "-":
            # Handle THIS-n, PREVIOUS-n, etc.
            content = node.get("content", [])
            if len(content) >= 2:
                time_macro = (
                    content[0].get("value", "").upper()
                    if content[0].get("type") == "time macro"
                    else None
                )
                offset = (
                    content[1].get("value", "0")
                    if content[1].get("type") == "number"
                    else "0"
                )

                if time_macro == "THIS":
                    try:
                        return -int(offset)  # THIS-n becomes -n
                    except ValueError:
                        return 0
                elif time_macro == "PREVIOUS":
                    try:
                        return -(int(offset) + 1)  # PREVIOUS-n becomes -(n+1)
                    except ValueError:
                        return -1
                elif time_macro == "NEXT":
                    try:
                        offset_val = int(offset)
                        return (
                            1 - offset_val if offset_val > 0 else 1
                        )  # NEXT-n becomes (1-n)
                    except ValueError:
                        return 1
                else:
                    self.log_warning(
                        f"FILTER MODIFIED: Unsupported time macro in date filter: {time_macro}-{offset}. Using default value.",
                        to_stderr=True,
                    )
                    return -30

        elif node_type == "+":
            # Handle THIS+n, PREVIOUS+n, etc.
            content = node.get("content", [])
            if len(content) >= 2:
                time_macro = (
                    content[0].get("value", "").upper()
                    if content[0].get("type") == "time macro"
                    else None
                )
                offset = (
                    content[1].get("value", "0")
                    if content[1].get("type") == "number"
                    else "0"
                )

                if time_macro == "THIS":
                    try:
                        return int(offset)  # THIS+n becomes +n
                    except ValueError:
                        return 0
                elif time_macro == "PREVIOUS":
                    try:
                        return -1 + int(offset)  # PREVIOUS+n becomes (-1+n)
                    except ValueError:
                        return -1
                elif time_macro == "NEXT":
                    try:
                        return 1 + int(offset)  # NEXT+n becomes (1+n)
                    except ValueError:
                        return 1
                else:
                    self.log_warning(
                        f"FILTER MODIFIED: Unsupported time macro in date filter: {time_macro}+{offset}. Using default value.",
                        to_stderr=True,
                    )
                    return 0

        elif node_type == "time macro":
            # Handle standalone THIS, PREVIOUS, NEXT
            time_macro = node.get("value", "").upper()
            if time_macro == "THIS":
                return 0  # THIS means current period
            elif time_macro == "PREVIOUS":
                return -1  # PREVIOUS means previous period
            elif time_macro == "NEXT":
                return 1  # NEXT means next period
            else:
                self.log_warning(
                    f"FILTER MODIFIED: Unsupported time macro in date filter: {time_macro}. Using default value.",
                    to_stderr=True,
                )
                return 0

        # If we couldn't parse the node, return None
        return None

    def process_between_date(self, filter_obj):
        """
        Process a between date filter (relative date filter) from Legacy to Cloud format.

        Args:
            filter_obj (dict): The Legacy filter object

        Returns:
            tuple: (cloud_filter, filter_config) with the converted filter
        """
        tree = filter_obj.get("tree", {})

        # Extract attribute URI using helper method
        attr_uri = self._get_attribute_uri_from_tree(tree)

        from_val = None
        to_val = None

        # Extract from/to values
        # For BETWEEN filters, we need to process nodes in order
        content_nodes = tree.get("content", [])
        date_nodes = []  # Non-attribute nodes that represent date values

        # Collect date-related nodes (skip attribute objects as we already extracted them)
        for node in content_nodes:
            if node.get("type") != "attribute object":
                # Collect all non-attribute nodes for date processing
                date_nodes.append(node)

        # Second pass: process date nodes in order (first = from, second = to)
        for i, node in enumerate(date_nodes):
            calculated_value = self._parse_date_node(node)

            if calculated_value is not None:
                if i == 0:
                    from_val = calculated_value
                elif i == 1:
                    to_val = calculated_value

        # Handle special case where the filter might use date elements instead of relative dates
        # This is for filters that specify exact dates rather than relative offsets
        date_elements_present = contains_node_type(tree, "attributeElement object")

        if date_elements_present:
            self.log_warning(
                "FILTER MODIFIED: Absolute date filters using specific date elements are not yet supported in relativeDateFilter. Using default date range instead.",
                to_stderr=True,
            )
            # Default to "last 30 days" if explicit date elements are used but not supported
            from_val = -30
            to_val = 0

        # If we couldn't determine the range, use a default
        if from_val is None or to_val is None:
            self.log_warning(
                f"FILTER MODIFIED: Could not determine date range from filter. Using default (last 30 periods). Current values: from={from_val}, to={to_val}",
                to_stderr=True,
            )
            from_val = -30
            to_val = 0

        # Only create a relative date filter if we have all required components
        if not attr_uri:
            return None, {}

        # Extract attribute info using helper method
        attribute_identifier, dataset_id, granularity = (
            self._extract_date_attribute_info(attr_uri)
        )

        if not attribute_identifier or not dataset_id:
            self.log_warning(
                f"FILTER REMOVED: Could not determine dataset ID for date attribute: {attr_uri}, filter will not be applied",
                to_stderr=True,
            )
            return None, {}

        # Check if this is a cyclical date attribute with relative values - not supported
        if _is_cyclical_legacy_attribute(attribute_identifier):
            condition = f"BETWEEN {from_val} AND {to_val}"
            self.log_warning(
                f"FILTER REMOVED: Cyclical date filters can not use relative values: ({attribute_identifier} {condition})",
                to_stderr=True,
            )
            return None, {}

        # Create the relative date filter using helper method
        cloud_filter = self._create_relative_date_filter(
            dataset_id, granularity, from_val, to_val
        )
        return cloud_filter.model_dump(by_alias=True, exclude_none=True), {}

    def _extract_date_attribute_info(self, attr_uri):
        """
        Extract attribute identifier, dataset ID, and granularity from a date attribute URI.

        Args:
            attr_uri (str): The date attribute URI

        Returns:
            tuple: (attribute_identifier, dataset_id, granularity) or (None, None, None) if extraction fails
        """
        try:
            # Get the attribute object to determine dataset and granularity
            obj = self.ctx.legacy_client.get_object(attr_uri)

            # Extract attribute identifier for granularity detection and dataset lookup
            attribute_identifier = None
            if "attributeDisplayForm" in obj:
                attribute_identifier = obj["attributeDisplayForm"]["meta"]["identifier"]
            elif "attribute" in obj:
                attribute_identifier = obj["attribute"]["meta"]["identifier"]

            if not attribute_identifier:
                return None, None, None

            # Get dataset ID and granularity from the attribute identifier
            dataset_id, granularity = get_date_dataset_and_granularity(
                self.ctx, attribute_identifier
            )

            return attribute_identifier, dataset_id, granularity

        except Exception as e:
            self.log_warning(
                f"Failed to extract date attribute info: {str(e)}", to_stderr=True
            )
            return None, None, None

    def _create_relative_date_filter(
        self,
        dataset_id: str,
        granularity: str,
        from_val: int | None,
        to_val: int | None,
    ) -> RelativeDateFilter:
        """
        Create a relative date filter with the given parameters.

        Args:
            dataset_id (str): The dataset identifier
            granularity (str): The date granularity
            from_val (int): The from value for the date range
            to_val (int): The to value for the date range

        Returns:
            dict: The relative date filter structure
        """
        # Ensure from_val is always less than or equal to to_val
        if from_val is not None and to_val is not None and from_val > to_val:
            self.log_warning(
                f"FILTER MODIFIED: Invalid date range: from ({from_val}) is after to ({to_val}). Swapping values.",
                to_stderr=True,
            )
            from_val, to_val = to_val, from_val

        return RelativeDateFilter(
            relative_date_filter=RelativeDateFilterContent(
                data_set=IdentifierWrapper(
                    identifier=Identifier(id=dataset_id, type="dataset")
                ),
                granularity=granularity,
                from_=from_val,
                to=to_val,
            )
        )

    def _get_attribute_uri_from_tree(self, tree):
        """
        Extract attribute URI from a filter tree.

        Args:
            tree (dict): The filter tree

        Returns:
            str or None: The attribute URI if found, None otherwise
        """
        for node in tree.get("content", []):
            if node.get("type") == "attribute object":
                return node.get("value")
        return None

    def _get_time_macro_from_tree(self, tree):
        """
        Extract time macro value from a filter tree.

        Args:
            tree (dict): The filter tree

        Returns:
            str or None: The time macro value if found, None otherwise
        """
        for node in tree.get("content", []):
            if node.get("type") == "time macro":
                return node.get("value")
        return None

    def process_date_null(self, attr_uri: str, is_negative: bool = False):
        """
        Process a date NULL / NOT NULL filter from Legacy to Cloud format.

        Args:
            attr_uri (str): The URI of the date attribute
            is_negative (bool): Whether this is a NOT NULL filter

        Returns:
            tuple: (cloud_filter, filter_config) with the converted filter
        """
        try:
            attribute_identifier, dataset_id, granularity = (
                self._extract_date_attribute_info(attr_uri)
            )

            if not attribute_identifier or not dataset_id or not granularity:
                self.log_warning(
                    f"FILTER REMOVED: Could not determine dataset/granularity for date attribute: {attr_uri}",
                    to_stderr=True,
                )
                return None, {}

            date_label_id = self.ctx.ldm_mappings.search_mapping_identifier(
                attribute_identifier
            )

            if not date_label_id:
                self.log_warning(
                    f"FILTER REMOVED: Missing label mapping for date NULL filter: ({attribute_identifier})",
                    to_stderr=True,
                )
                return None, {}

            # Companion date dimension filter (no from/to).
            relative_date_filter = RelativeDateFilter(
                relative_date_filter=RelativeDateFilterContent(
                    data_set=IdentifierWrapper(
                        identifier=Identifier(id=dataset_id, type="dataset")
                    ),
                    granularity=granularity,
                )
            ).model_dump(by_alias=True, exclude_none=True)

            # NULL / NOT NULL expressed via empty-string value on the date label.
            display_form = IdentifierWrapper(
                identifier=Identifier(id=date_label_id, type="label")
            )

            if is_negative:
                attribute_filter = NegativeAttributeFilter(
                    negative_attribute_filter=NegativeAttributeFilterContent(
                        display_form=display_form,
                        not_in=Values(values=[""]),
                    )
                ).model_dump(by_alias=True, exclude_none=True)
            else:
                attribute_filter = PositiveAttributeFilter(
                    positive_attribute_filter=PositiveAttributeFilterContent(
                        display_form=display_form,
                        in_=Values(values=[""]),
                    )
                ).model_dump(by_alias=True, exclude_none=True)

            return [relative_date_filter, attribute_filter], {}

        except Exception as e:
            self.log_warning(
                f"Failed to create date NULL filter: {str(e)}", to_stderr=True
            )

        return None, {}

    def _detect_null_elements(
        self, element_uris, attribute_identifier, is_negative=False
    ):
        """
        Detect NULL/empty elements in a date filter and log appropriate warnings.

        Args:
            element_uris (list): List of element URIs to check
            attribute_identifier (str): The attribute identifier for warnings
            is_negative (bool): Whether this is a negative (NOT NULL) filter

        Returns:
            tuple: (non_null_uris, has_nulls, all_null) where:
                   - non_null_uris: list of URIs that are not NULL
                   - has_nulls: boolean indicating if any NULL values were found
                   - all_null: boolean indicating if all values were NULL
        """
        import re

        null_uris = []
        non_null_uris = []

        for uri in element_uris:
            # Check for NULL element ID (id=0)
            match = re.search(r"elements\?id=(\d+)", uri)
            if match and int(match.group(1)) == 0:
                null_uris.append(uri)
            else:
                non_null_uris.append(uri)

        has_nulls = len(null_uris) > 0
        all_null = len(non_null_uris) == 0

        if has_nulls and not all_null:
            # Determine the filter type based on is_negative
            filter_type = "NOT NULL" if is_negative else "IS NULL"
            self.log_warning(
                f"FILTER MODIFIED: {filter_type} Date filters not supported: ({attribute_identifier})",
                to_stderr=True,
            )

        return non_null_uris, has_nulls, all_null

    def process_equality_date(self, filter_obj):
        """
        Process a date equality filter (attribute = time macro) from Legacy to Cloud format.

        Args:
            filter_obj (dict): The Legacy filter object

        Returns:
            tuple: (cloud_filter, filter_config) with the converted filter
        """
        tree = filter_obj.get("tree", {})

        # Extract attribute URI using helper method
        attr_uri = self._get_attribute_uri_from_tree(tree)
        if not attr_uri:
            self.log_warning(
                "FILTER REMOVED: Could not extract attribute URI from equality date filter",
                to_stderr=True,
            )
            return None, {}

        # Extract time macro value using helper method
        time_macro_value = self._get_time_macro_from_tree(tree)

        if not time_macro_value:
            self.log_warning(
                "FILTER REMOVED: Could not extract time macro from equality date filter",
                to_stderr=True,
            )
            return None, {}

        # Parse the time macro to get the relative date offset
        parsed_value = self._parse_time_macro_node(
            {"type": "time macro", "value": time_macro_value}
        )

        if parsed_value is None:
            self.log_warning(
                f"FILTER MODIFIED: Could not parse time macro '{time_macro_value}' in equality date filter. Using default (current period).",
                to_stderr=True,
            )
            parsed_value = 0  # Default to current period

        # Extract attribute info using helper method
        attribute_identifier, dataset_id, granularity = (
            self._extract_date_attribute_info(attr_uri)
        )

        if not attribute_identifier or not dataset_id:
            self.log_warning(
                f"FILTER REMOVED: Could not determine dataset ID for equality date attribute: {attr_uri}",
                to_stderr=True,
            )
            return None, {}

        # Check if this is a cyclical date attribute with relative values - not supported
        if _is_cyclical_legacy_attribute(attribute_identifier):
            condition = f"= {time_macro_value}"
            self.log_warning(
                f"FILTER REMOVED: Cyclical date filters can not use relative values: ({attribute_identifier} {condition})",
                to_stderr=True,
            )
            return None, {}

        # For equality filters, from and to are the same (single period)
        from_val = parsed_value
        to_val = parsed_value

        # Create the relative date filter using helper method
        cloud_filter = self._create_relative_date_filter(
            dataset_id, granularity, from_val, to_val
        )
        return cloud_filter.model_dump(by_alias=True, exclude_none=True), {}

    def _parse_time_macro_node(self, node):
        """
        Parse a time macro node to get the relative date offset.
        This is a simplified version of _parse_date_node for standalone time macros.

        Args:
            node (dict): The time macro node to parse

        Returns:
            int or None: The calculated date offset value, or None if parsing failed
        """
        if node.get("type") == "time macro":
            time_macro = node.get("value", "").upper()
            if time_macro == "THIS":
                return 0  # THIS means current period
            elif time_macro == "PREVIOUS":
                return -1  # PREVIOUS means previous period
            elif time_macro == "NEXT":
                return 1  # NEXT means next period
            else:
                self.log_warning(
                    f"FILTER MODIFIED: Unsupported time macro in equality date filter: {time_macro}. Using default value.",
                    to_stderr=True,
                )
                return 0

        # If we couldn't parse the node, return None
        return None

    def extract_absolute_date_data(self, filter_obj, is_negative=False):
        """
        Extract attribute URI and element values from an absolute date filter.

        Args:
            filter_obj (dict): The Legacy filter object
            is_negative (bool): Whether this is a negative (NOT IN) filter

        Returns:
            tuple: (attr_uri, values) with the attribute URI and list of element values
        """
        tree = filter_obj.get("tree", {})
        filter_type = tree.get("type")
        attr_uri = None
        values = []

        if not is_negative:
            # Positive filter: type = "in"
            for node in tree.get("content", []):
                if node.get("type") == "attribute object":
                    attr_uri = node.get("value")
                elif node.get("type") == "list":
                    for item in node.get("content", []):
                        if item.get("type") == "attributeElement object":
                            val = item.get("value")
                            if val is not None:
                                values.append(val)
        else:
            # Negative filter: type = "not" or "not in"
            if filter_type == "not in":
                # Binary NOT IN format
                for node in tree.get("content", []):
                    if node.get("type") == "attribute object":
                        attr_uri = node.get("value")
                    elif node.get("type") == "list":
                        for item in node.get("content", []):
                            if item.get("type") == "attributeElement object":
                                val = item.get("value")
                                if val is not None:
                                    values.append(val)
            elif filter_type == "not":
                # Unary NOT format: NOT (A IN B)
                for child in tree.get("content", []):
                    if child.get("type") == "()":
                        for inner_child in child.get("content", []):
                            if inner_child.get("type") == "in":
                                for node in inner_child.get("content", []):
                                    if node.get("type") == "attribute object":
                                        attr_uri = node.get("value")
                                    elif node.get("type") == "list":
                                        for item in node.get("content", []):
                                            if (
                                                item.get("type")
                                                == "attributeElement object"
                                            ):
                                                val = item.get("value")
                                                if val is not None:
                                                    values.append(val)

        return attr_uri, values

    def process(self, filter_obj, **kwargs):
        """
        Process a date filter from Legacy to Cloud format.

        Args:
            filter_obj (dict): The Legacy filter object
            **kwargs: Additional arguments, including filter_type for absolute date filters

        Returns:
            tuple: (cloud_filter, filter_config) with the converted filter
        """
        # Check the filter type to determine processing approach
        filter_dispatch_type = kwargs.get("filter_type")

        if filter_dispatch_type == "equality":
            # Handle date equality filters (attribute = time macro)
            return self.process_equality_date(filter_obj)
        elif filter_dispatch_type in ["positive_absolute", "negative_absolute"]:
            # Extract the attribute and element data from the filter
            is_negative = filter_dispatch_type == "negative_absolute"
            attr_uri, values = self.extract_absolute_date_data(filter_obj, is_negative)

            if not attr_uri or not values:
                self.log_warning(
                    "FILTER REMOVED: Could not extract attribute or values from absolute date filter",
                    to_stderr=True,
                )
                return None, {}

            # Get attribute identifier first for NULL detection
            try:
                obj = self.ctx.legacy_client.get_object(attr_uri)
                attribute_identifier = None
                if "attributeDisplayForm" in obj:
                    attribute_identifier = obj["attributeDisplayForm"]["meta"][
                        "identifier"
                    ]
                elif "attribute" in obj:
                    attribute_identifier = obj["attribute"]["meta"]["identifier"]

                if not attribute_identifier:
                    self.log_warning(
                        "FILTER REMOVED: Could not determine attribute identifier for date filter",
                        to_stderr=True,
                    )
                    return None, {}
            except Exception as e:
                self.log_warning(
                    f"FILTER REMOVED: Error getting attribute identifier: {str(e)}",
                    to_stderr=True,
                )
                return None, {}

            # Check for NULL elements early and filter them out
            non_null_values, has_nulls, all_null = self._detect_null_elements(
                values, attribute_identifier, is_negative
            )

            # If all values are NULL, convert to Cloud NULL / NOT NULL filter pattern.
            if all_null:
                return self.process_date_null(attr_uri, is_negative=is_negative)

            # Use non-NULL values for further processing
            values_to_process = non_null_values

            # Classify the date filter as cyclical or ordinal
            date_type, attribute_identifier, granularity = classify_date_filter_type(
                self.ctx, attr_uri, values_to_process
            )

            # Check if the granularity is supported
            if date_type is None or attribute_identifier is None or granularity is None:
                # Unsupported granularity, warning already logged by classify_date_filter_type
                return None, {}

            if date_type == "cyclical":
                # Create both attribute filter and relativeDateFilter
                cyclical_processor = CyclicalDateProcessor(self.ctx)
                return cyclical_processor.process_cyclical_date_filter(
                    filter_obj,
                    attr_uri,
                    values_to_process,
                    is_negative,
                    attribute_identifier,
                    granularity,
                )
            elif date_type == "ordinal":
                # Create absoluteDateFilter
                ordinal_processor = OrdinalDateProcessor(self.ctx)
                return ordinal_processor.process_ordinal_date_filter(
                    filter_obj,
                    attr_uri,
                    values_to_process,
                    is_negative,
                    attribute_identifier,
                    granularity,
                )
            else:
                self.log_warning(
                    f"FILTER REMOVED: Unknown date filter classification: {date_type}",
                    to_stderr=True,
                )
                return None, {}

        # Handle traditional relative date filters (between)
        filter_type = filter_obj.get("tree", {}).get("type")

        if filter_type == "between":
            return self.process_between_date(filter_obj)
        else:
            self.log_warning(
                f"FILTER REMOVED: Unsupported date filter type: {filter_type}",
                to_stderr=True,
            )
            return None, {}
