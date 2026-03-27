# (C) 2026 GoodData Corporation
"""
Date filter post-processor for handling Cloud cyclical date filter constraints.

This module handles the Cloud limitation where only one date dimension can have
cyclical date filters, and they must be positioned after the primary date dimension filter.
"""

from gooddata_platform2cloud.metrics.cyclical_date_conversion import (
    convert_cyclical_date_elements,
)
from gooddata_platform2cloud.reports.data_classes import ContextWithWarnings
from gooddata_platform2cloud.reports.filters.date_classification import (
    _is_cyclical_platform_attribute,
)
from gooddata_platform2cloud.reports.filters.filter_mapper import (
    _get_attribute_uri_from_filter,
)


class DateFilterPostProcessor:
    """
    Post-processor for date filters that handles Cloud's cyclical filter constraints.

    Cloud limitations:
    - Only one date dimension can have cyclical filters
    - Cyclical filters must be positioned immediately after the primary dimension's relativeDateFilter
    - Selection priority: most cyclical filters, then earliest position in original Platform filters
    """

    def __init__(self, ctx: ContextWithWarnings):
        """
        Initialize the post-processor.

        Args:
            ctx: The context object with API and mappings for logging warnings
        """
        self.ctx = ctx

    def process(
        self,
        cloud_filters,
        platform_filters,
        attribute_filter_configs,
        cyclical_filter_info,
    ):
        """
        Post-process date filters to handle Cloud cyclical filter constraints.

        Args:
            cloud_filters (list): List of processed Cloud filters
            platform_filters (list): Original Platform filters (for tracking order)
            attribute_filter_configs (dict): Filter configurations
            cyclical_filter_info (dict): Info about which Platform filters produced cyclical results

        Returns:
            tuple: (updated_cloud_filters, updated_configs) with constraints applied
        """
        if not cloud_filters or not cyclical_filter_info:
            return cloud_filters, attribute_filter_configs

        # Analyze date dimensions and their filters
        dimension_analysis = self._analyze_date_dimensions(
            cloud_filters, platform_filters, cyclical_filter_info
        )

        if not dimension_analysis:
            # No cyclical date filters found, nothing to process
            return cloud_filters, attribute_filter_configs

        # Select primary date dimension
        primary_dimension = self._select_primary_dimension(dimension_analysis)

        if not primary_dimension:
            # No cyclical filters found, nothing to process
            return cloud_filters, attribute_filter_configs

        # Remove non-primary cyclical filters and issue warnings
        filtered_cloud_filters, updated_configs = (
            self._remove_non_primary_cyclical_filters(
                cloud_filters,
                primary_dimension,
                dimension_analysis,
                platform_filters,
                attribute_filter_configs,
            )
        )

        # Reorder primary dimension's cyclical filters to be adjacent
        final_cloud_filters = self._reorder_primary_cyclical_filters(
            filtered_cloud_filters, primary_dimension, dimension_analysis
        )

        return final_cloud_filters, updated_configs

    def _analyze_date_dimensions(
        self, cloud_filters, platform_filters, cyclical_filter_info
    ):
        """
        Group filters by date dimension and identify cyclical filters using metadata.

        Args:
            cloud_filters (list): List of processed Cloud filters
            platform_filters (list): Original Platform filters for order tracking
            cyclical_filter_info (dict): Info about which Platform filters produced cyclical results

        Returns:
            dict: Analysis of date dimensions with their filters and positions
        """
        dimensions = {}

        # Process cyclical filters first using the metadata
        for platform_index, info in cyclical_filter_info.items():
            start_idx = info["cloud_start_index"]
            filter_count = info["filter_count"]

            # Find the relative date filter in this cyclical group to get dataset ID
            dataset_id = None
            relative_filter_idx = None
            cyclical_filter_indices = []

            for i in range(start_idx, start_idx + filter_count):
                if i < len(cloud_filters):
                    cloud_filter = cloud_filters[i]

                    if "relativeDateFilter" in cloud_filter:
                        dataset_id = self._extract_dataset_id(cloud_filter)
                        relative_filter_idx = i
                    elif (
                        "positiveAttributeFilter" in cloud_filter
                        or "negativeAttributeFilter" in cloud_filter
                    ):
                        cyclical_filter_indices.append(i)

            if dataset_id:
                # Initialize dimension if not seen before
                if dataset_id not in dimensions:
                    dimensions[dataset_id] = {
                        "cyclical_filters": [],
                        "relative_filters": [],
                        "first_position": start_idx,
                        "platform_index": platform_index,
                    }

                # Add the relative date filter
                if relative_filter_idx is not None:
                    dimensions[dataset_id]["relative_filters"].append(
                        {
                            "filter": cloud_filters[relative_filter_idx],
                            "cloud_index": relative_filter_idx,
                            "platform_index": platform_index,
                        }
                    )

                # Add cyclical attribute filters
                for cyclical_idx in cyclical_filter_indices:
                    dimensions[dataset_id]["cyclical_filters"].append(
                        {
                            "filter": cloud_filters[cyclical_idx],
                            "cloud_index": cyclical_idx,
                            "platform_index": platform_index,
                        }
                    )

        # Process any standalone relative date filters (non-cyclical)
        for i, cloud_filter in enumerate(cloud_filters):
            if "relativeDateFilter" in cloud_filter:
                dataset_id = self._extract_dataset_id(cloud_filter)

                if dataset_id:
                    # Check if this filter is already processed as part of a cyclical group
                    already_processed = False
                    for info in cyclical_filter_info.values():
                        if (
                            i >= info["cloud_start_index"]
                            and i < info["cloud_start_index"] + info["filter_count"]
                        ):
                            already_processed = True
                            break

                    if not already_processed:
                        # This is a standalone relative date filter
                        if dataset_id not in dimensions:
                            dimensions[dataset_id] = {
                                "cyclical_filters": [],
                                "relative_filters": [],
                                "first_position": i,
                                "platform_index": i,  # Approximate
                            }

                        dimensions[dataset_id]["relative_filters"].append(
                            {
                                "filter": cloud_filter,
                                "cloud_index": i,
                                "platform_index": i,  # Approximate
                            }
                        )

        return dimensions

    def _remove_orphaned_companion_filters(self, filters, primary_dimension):
        """
        Remove orphaned companion relativeDateFilter entries.

        Companion filters are relativeDateFilter entries without from/to values that were
        created for cyclical date attributes. When cyclical filters are removed from
        non-primary dimensions, their companion filters become orphaned and should be removed.

        Args:
            filters (list): List of cloud filters
            primary_dimension (str): The primary date dimension ID (e.g., 'dt_ship')

        Returns:
            list: Filtered list with orphaned companions removed
        """
        cleaned_filters = []

        for pf in filters:
            # Skip orphaned companion filters (relativeDateFilter without from/to for non-primary dimensions)
            if (
                "relativeDateFilter" in pf
                and "from" not in pf["relativeDateFilter"]
                and "to" not in pf["relativeDateFilter"]
                and self._extract_dataset_id(pf) != primary_dimension
            ):
                # Skip this orphaned companion filter
                continue

            # Keep everything else
            cleaned_filters.append(pf)

        return cleaned_filters

    def _extract_dataset_id(self, cloud_filter):
        """
        Extract dataset ID from a Cloud filter.

        Args:
            cloud_filter (dict): A Cloud filter object

        Returns:
            str or None: The dataset ID (e.g., 'dt_order') if found
        """
        if "relativeDateFilter" in cloud_filter:
            return (
                cloud_filter["relativeDateFilter"]
                .get("dataSet", {})
                .get("identifier", {})
                .get("id")
            )

        return None

    def _create_friendly_filter_description(self, platform_filter, dim_id):
        """
        Create a user-friendly description of a Platform filter for warning messages.

        Args:
            platform_filter (dict): The original Platform filter
            dim_id (str): The dataset ID (e.g., 'dt_order')

        Returns:
            str: Friendly description like "dt_order - order.month.in.year IN May, Apr"
        """
        try:
            # Extract attribute URI from Platform filter
            attr_uri = _get_attribute_uri_from_filter(platform_filter)
            if not attr_uri:
                return f"{dim_id} - unknown attribute"

            # Get the Platform attribute identifier
            obj = self.ctx.platform_client.get_object(attr_uri)
            attribute_identifier = None
            if "attributeDisplayForm" in obj:
                attribute_identifier = obj["attributeDisplayForm"]["meta"]["identifier"]
            elif "attribute" in obj:
                attribute_identifier = obj["attribute"]["meta"]["identifier"]

            if not attribute_identifier:
                return f"{dim_id} - unknown attribute"

            # Extract element URIs from the filter
            element_uris = self._extract_element_uris_from_filter(platform_filter)

            # Try to convert elements to readable values for cyclical filters
            if element_uris and _is_cyclical_platform_attribute(attribute_identifier):
                converted_values, _, _ = convert_cyclical_date_elements(
                    self.ctx, element_uris, attribute_identifier
                )

                if converted_values:
                    # Map cyclical values to readable names
                    readable_values = self._convert_cyclical_values_to_names(
                        converted_values, attribute_identifier
                    )
                    if readable_values:
                        values_str = ", ".join(readable_values)
                        return f"{dim_id} - {attribute_identifier} IN {values_str}"

            # Fallback: just show the attribute identifier
            return f"{dim_id} - {attribute_identifier}"

        except Exception:
            # Fallback to basic info if anything fails
            return f"{dim_id} - cyclical filter"

    def _extract_element_uris_from_filter(self, platform_filter):
        """
        Extract element URIs from a Platform filter.

        Args:
            platform_filter (dict): The Platform filter object

        Returns:
            list: List of element URIs
        """
        element_uris = []
        tree = platform_filter.get("tree", {})

        def collect_elements(node):
            if isinstance(node, dict):
                if node.get("type") == "attributeElement object":
                    val = node.get("value")
                    if val:
                        element_uris.append(val)
                elif "content" in node:
                    for child in node.get("content", []):
                        collect_elements(child)

        collect_elements(tree)
        return element_uris

    def _convert_cyclical_values_to_names(self, converted_values, attribute_identifier):
        """
        Convert cyclical Cloud values to readable names.

        Note: By this point, converted_values contains standardized Cloud values
        (e.g., both day.in.week and day.in.euweek are in the same format: "00"=Sunday, "01"=Monday, etc.)

        Args:
            converted_values (list): List of Cloud values (e.g., ["01", "02", "03"])
            attribute_identifier (str): The attribute identifier

        Returns:
            list: List of readable names or None if conversion fails
        """
        from gooddata_platform2cloud.reports.filters.date_classification import (
            _get_platform_attribute_type,
        )

        attr_type = _get_platform_attribute_type(attribute_identifier)
        if not attr_type:
            return None

        try:
            # Shared mapping for standardized Cloud dayOfWeek format
            # (used by both day.in.week and day.in.euweek after conversion)
            day_of_week_mapping = {
                "00": "Sunday",
                "01": "Monday",
                "02": "Tuesday",
                "03": "Wednesday",
                "04": "Thursday",
                "05": "Friday",
                "06": "Saturday",
            }

            # Mappings for standardized Cloud values
            mappings = {
                "month.in.year": {
                    "01": "January",
                    "02": "February",
                    "03": "March",
                    "04": "April",
                    "05": "May",
                    "06": "June",
                    "07": "July",
                    "08": "August",
                    "09": "September",
                    "10": "October",
                    "11": "November",
                    "12": "December",
                },
                "quarter.in.year": {"01": "Q1", "02": "Q2", "03": "Q3", "04": "Q4"},
                # Both day.in.week and day.in.euweek use the same unified Cloud format
                "day.in.week": day_of_week_mapping,
                "day.in.euweek": day_of_week_mapping,
                # Other cyclical types (less commonly filtered, but supported)
                "month.in.quarter": {"01": "Month 1", "02": "Month 2", "03": "Month 3"},
                "week.in.year": None,  # Too many values (1-53), skip readable conversion
                "week.in.quarter": None,  # Too many values, skip readable conversion
                "day.in.year": None,  # Too many values (1-366), skip readable conversion
                "day.in.quarter": None,  # Too many values, skip readable conversion
                "day.in.month": None,  # Too many values (1-31), skip readable conversion
            }

            mapping = mappings.get(attr_type)
            if mapping:
                readable_names = []
                for val in converted_values:
                    name = mapping.get(val, val)  # Fallback to original value
                    readable_names.append(name)
                return readable_names

        except Exception:
            pass

        return None

    def _select_primary_dimension(self, dimension_analysis):
        """
        Select the primary date dimension based on cyclical filter count and position.

        Args:
            dimension_analysis (dict): Analysis of date dimensions

        Returns:
            str or None: The dataset ID of the primary dimension, or None if no cyclical filters
        """
        # Filter dimensions that have cyclical filters
        cyclical_dimensions = {
            dim_id: info
            for dim_id, info in dimension_analysis.items()
            if info["cyclical_filters"]
        }

        if not cyclical_dimensions:
            return None

        # Sort by: most cyclical filters (desc), then earliest platform_index (asc) for tie-breaking
        primary_dim = sorted(
            cyclical_dimensions.items(),
            key=lambda x: (-len(x[1]["cyclical_filters"]), x[1]["platform_index"]),
        )[0][0]

        return primary_dim

    def _remove_non_primary_cyclical_filters(
        self,
        cloud_filters,
        primary_dimension,
        dimension_analysis,
        platform_filters,
        attribute_filter_configs,
    ):
        """
        Remove cyclical filters from non-primary dimensions and issue warnings.

        Args:
            cloud_filters (list): List of Cloud filters
            primary_dimension (str): The selected primary dimension
            dimension_analysis (dict): Analysis of date dimensions
            platform_filters (list): Original Platform filters for warning messages
            attribute_filter_configs (dict): Filter configurations

        Returns:
            tuple: (filtered_cloud_filters, updated_configs)
        """
        filtered_filters = []
        updated_configs = attribute_filter_configs.copy()

        # Track which filter indices to remove
        indices_to_remove = set()

        # Identify cyclical filters from non-primary dimensions
        for dim_id, info in dimension_analysis.items():
            if dim_id == primary_dimension:
                continue  # Skip primary dimension

            # Remove only the cyclical attribute filters, keep relative date filters
            for filter_info in info["cyclical_filters"]:
                indices_to_remove.add(filter_info["cloud_index"])

                # Issue warning for each removed Platform filter
                platform_index = filter_info["platform_index"]
                if platform_index < len(platform_filters):
                    platform_filter = platform_filters[platform_index]
                    friendly_description = self._create_friendly_filter_description(
                        platform_filter, dim_id
                    )
                    self.ctx.log_warning(
                        f"FILTER REMOVED: Only one date dimension can have cyclical date filters: ({friendly_description})",
                        to_stderr=True,
                    )

        # Build filtered list, preserving order
        for i, cloud_filter in enumerate(cloud_filters):
            if i not in indices_to_remove:
                filtered_filters.append(cloud_filter)

        return filtered_filters, updated_configs

    def _reorder_primary_cyclical_filters(
        self, cloud_filters, primary_dimension, dimension_analysis
    ):
        """
        Reorder filters so that the primary dimension's relative date filter is the FIRST date filter,
        followed immediately by its cyclical filters.

        Args:
            cloud_filters (list): List of filtered Cloud filters
            primary_dimension (str): The primary dimension
            dimension_analysis (dict): Analysis of date dimensions

        Returns:
            list: Reordered list of Cloud filters
        """
        if primary_dimension not in dimension_analysis:
            return cloud_filters

        primary_info = dimension_analysis[primary_dimension]

        if not primary_info["cyclical_filters"] or not primary_info["relative_filters"]:
            return cloud_filters

        # Find the primary dimension's relative date filter
        primary_relative_filter = None

        for filter_info in primary_info["relative_filters"]:
            for i, pf in enumerate(cloud_filters):
                if pf is filter_info["filter"]:
                    primary_relative_filter = pf
                    break
            if primary_relative_filter is not None:
                break

        if primary_relative_filter is None:
            return cloud_filters

        # Collect primary dimension's cyclical filters with Platform order
        primary_cyclical_filters = []
        primary_cyclical_indices = set()

        for cf_info in primary_info["cyclical_filters"]:
            for i, pf in enumerate(cloud_filters):
                if pf is cf_info["filter"]:
                    primary_cyclical_filters.append((cf_info["platform_index"], pf))
                    primary_cyclical_indices.add(i)
                    break

        # Sort cyclical filters by original Platform order
        primary_cyclical_filters.sort(key=lambda x: x[0])

        # Reconstruct the filter list:
        # - Primary date filter becomes the FIRST date filter
        # - Primary cyclical filters go right after it
        # - Other date filters follow
        # - Non-date filters maintain their relative positions but shift as needed

        # Separate date and non-date filters
        date_filters = []
        non_date_filters = []

        for i, pf in enumerate(cloud_filters):
            if (
                "relativeDateFilter" in pf
                or "absoluteDateFilter" in pf
                or i in primary_cyclical_indices
            ):
                date_filters.append((i, pf))
            else:
                non_date_filters.append((i, pf))

        # Based on user's exact example, implement the specific pattern:
        # 1. Non-date filters before first date position stay
        # 2. First date positions get: [primary, primary_cyclicals, dates_before_primary]
        # 3. Remaining positions filled with: non-dates that got displaced, then dates_after_primary

        # Find primary position and separate date filters
        primary_pos = next(
            i for i, pf in enumerate(cloud_filters) if pf is primary_relative_filter
        )
        first_date_pos = min(
            i
            for i, pf in enumerate(cloud_filters)
            if (
                "relativeDateFilter" in pf
                or "absoluteDateFilter" in pf
                or i in primary_cyclical_indices
            )
        )

        # Collect filters by category
        dates_before_primary = []
        dates_after_primary = []
        non_date_filters = []

        for i, pf in enumerate(cloud_filters):
            if (
                "relativeDateFilter" in pf
                or "absoluteDateFilter" in pf
                or i in primary_cyclical_indices
            ):
                # Check if this is a companion filter for the primary dimension
                is_primary_companion = False
                if "relativeDateFilter" in pf:
                    dataset_id = self._extract_dataset_id(pf)
                    relative_filter = pf["relativeDateFilter"]
                    has_from = "from" in relative_filter
                    has_to = "to" in relative_filter
                    is_companion = not has_from and not has_to
                    is_primary_companion = (
                        is_companion and dataset_id == primary_dimension
                    )

                if (
                    i < primary_pos
                    and pf is not primary_relative_filter
                    and i not in primary_cyclical_indices
                    and not is_primary_companion
                ):
                    dates_before_primary.append(pf)
                elif (
                    i > primary_pos
                    and pf is not primary_relative_filter
                    and i not in primary_cyclical_indices
                    and not is_primary_companion
                ):
                    dates_after_primary.append((i, pf))  # Keep position info
            else:
                non_date_filters.append((i, pf))

        # Build result step by step
        result = []

        # Add non-date filters that were before first date position
        for orig_pos, nf in non_date_filters:
            if orig_pos < first_date_pos:
                result.append(nf)

        # Add reorganized date section: [primary, primary_cyclicals, dates_before_primary]
        result.append(primary_relative_filter)
        for _, cf in primary_cyclical_filters:
            result.append(cf)
        for df in dates_before_primary:
            result.append(df)

        # Add displaced non-date filters (those that were between/after dates)
        for orig_pos, nf in non_date_filters:
            if orig_pos >= first_date_pos:
                result.append(nf)

        # Add date filters that were after primary (in original positions where possible)
        for orig_pos, df in dates_after_primary:
            result.append(df)

        # Clean up orphaned companion relativeDateFilter entries
        # These are relativeDateFilter without from/to that are not the primary dimension
        result = self._remove_orphaned_companion_filters(result, primary_dimension)

        return result
