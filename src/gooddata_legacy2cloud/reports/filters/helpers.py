# (C) 2026 GoodData Corporation
"""
Helper functions for filter processing. These functions are used across
different filter types to avoid code duplication and make maintenance easier.
"""

from gooddata_legacy2cloud.metrics.attribute_element import AttributeElement
from gooddata_legacy2cloud.metrics.contants import MISSING_VALUE
from gooddata_legacy2cloud.metrics.display_form_utils import get_primary_display_form
from gooddata_legacy2cloud.reports.data_classes import ContextWithWarnings


def has_attributes_in_buckets(buckets):
    """
    Check if there are any attributes in the visualization buckets.

    Args:
        buckets (list): The buckets array from visualization content

    Returns:
        bool: True if at least one attribute is found in buckets, False otherwise
    """
    if not buckets or not isinstance(buckets, list):
        return False

    for bucket in buckets:
        items = bucket.get("items", [])
        for item in items:
            if "attribute" in item:
                return True
    return False


def get_display_form_identifiers(ctx: ContextWithWarnings, attr_uri):
    """
    Extract both the primary and original display form identifiers from an attribute URI.

    Args:
        ctx: The context object with API and mappings
        attr_uri: The URI of the attribute

    Returns:
        tuple: (primary_df_id, original_df_id, original_df_type) with display form information
    """
    try:
        obj = ctx.legacy_client.get_object(attr_uri)

        # First get the actual display form used in the filter
        original_df_id = None
        original_df_type = "label"
        if "attributeDisplayForm" in obj:
            adf = obj["attributeDisplayForm"]
            original_df_id = ctx.ldm_mappings.search_mapping_identifier(
                adf["meta"]["identifier"]
            )
            original_df_type = adf["meta"]["category"].lower()
            if original_df_type == "attributedisplayform":
                original_df_type = "label"
        elif (
            "attribute" in obj
            and "content" in obj["attribute"]
            and obj["attribute"]["content"].get("displayForms")
        ):
            adf = obj["attribute"]["content"]["displayForms"][0]
            original_df_id = ctx.ldm_mappings.search_mapping_identifier(
                adf["meta"]["identifier"]
            )
            original_df_type = adf["meta"]["category"].lower()
            if original_df_type == "attributedisplayform":
                original_df_type = "label"
        else:
            original_df_id = attr_uri

        # Now get the primary display form
        primary_df_id = None
        if (
            "attribute" in obj
            and "content" in obj["attribute"]
            and obj["attribute"]["content"].get("displayForms")
        ):
            display_forms = obj["attribute"]["content"]["displayForms"]
            if display_forms:
                # Use shared utility to get primary display form
                primary_display_form = get_primary_display_form(display_forms)

                if primary_display_form:
                    primary_df_id = ctx.ldm_mappings.search_mapping_identifier(
                        primary_display_form["meta"]["identifier"]
                    )

        # Fallback if primary not found
        if not primary_df_id:
            primary_df_id = original_df_id

        return primary_df_id, original_df_id, original_df_type

    except Exception:
        # In case of error, return the attribute URI as fallback
        return attr_uri, attr_uri, "label"


def convert_attribute_elements(ctx: ContextWithWarnings, element_uris):
    """
    Convert attribute element URIs to their actual element values.

    Args:
        ctx: The context object with API and mappings
        element_uris: List of element URIs to convert

    Returns:
        tuple: (converted_values, missing_elements) with converted values and list of URIs that couldn't be converted
    """
    converted_values = []
    missing_elements = []

    for uri in element_uris:
        val = AttributeElement(ctx, uri).get()
        if val == MISSING_VALUE:
            missing_elements.append(uri)
        else:
            converted_values.append(val)

    return converted_values, missing_elements


def find_visible_display_form(ctx, attribute_uri, displayed_attributes):
    """
    Find the first display form of an attribute that is visible in the visualization.

    Args:
        ctx: The context object with API and mappings
        attribute_uri: The URI of the attribute
        displayed_attributes: Set of displayed attribute URIs

    Returns:
        object: The display form object if found, None otherwise
    """
    try:
        attr_obj = ctx.legacy_client.get_object(attribute_uri)
        if (
            "attribute" in attr_obj
            and "content" in attr_obj["attribute"]
            and attr_obj["attribute"]["content"].get("displayForms")
        ):
            for df in attr_obj["attribute"]["content"]["displayForms"]:
                if "meta" in df and "uri" in df["meta"]:
                    df_uri = df["meta"]["uri"]
                    # Safely check if URI is in displayed_attributes
                    if df_uri and df_uri in displayed_attributes:
                        return df
        return None
    except Exception:
        return None


def get_attribute_uri_from_display_form(ctx: ContextWithWarnings, display_form_uri):
    """
    Get the parent attribute URI from an attributeDisplayForm URI.

    Args:
        ctx: The context object with API and mappings
        display_form_uri: The URI of the display form

    Returns:
        str: The parent attribute URI, or None if not found or not a display form
    """
    try:
        obj = ctx.legacy_client.get_object(display_form_uri)
        if (
            "attributeDisplayForm" in obj
            and "content" in obj["attributeDisplayForm"]
            and "formOf" in obj["attributeDisplayForm"]["content"]
        ):
            return obj["attributeDisplayForm"]["content"]["formOf"]
        return None
    except Exception:
        return None


def get_parent_attributes_from_display_forms(
    ctx: ContextWithWarnings, display_form_uris
):
    """
    Convert a collection of display form URIs to their parent attribute URIs.

    Args:
        ctx: The context object with API and mappings
        display_form_uris: Collection of display form URIs

    Returns:
        set: Set of parent attribute URIs
    """
    parent_attributes = set()
    for df_uri in display_form_uris:
        parent_attr_uri = get_attribute_uri_from_display_form(ctx, df_uri)
        if parent_attr_uri:
            parent_attributes.add(parent_attr_uri)
    return parent_attributes


def _find_displayed_form_for_attribute(
    ctx: ContextWithWarnings, attribute_uri, displayed_attribute_uris
):
    """
    Find the display form object that is displayed for a given attribute.

    This helper method consolidates the common logic for finding display forms
    used by both attribute_has_displayed_form and get_displayed_form_identifier.

    Args:
        ctx: The context object with API and mappings
        attribute_uri: The URI of the attribute to check
        displayed_attribute_uris: Set of displayed attribute/display form URIs

    Returns:
        tuple: (display_form_obj, identifier) where display_form_obj is the found
               display form object and identifier is its Legacy identifier, or (None, None) if not found
    """
    try:
        # First check if any of the displayed URIs are display forms of our attribute
        attr_obj = ctx.legacy_client.get_object(attribute_uri)
        if (
            "attribute" in attr_obj
            and "content" in attr_obj["attribute"]
            and "displayForms" in attr_obj["attribute"]["content"]
        ):
            for df in attr_obj["attribute"]["content"]["displayForms"]:
                if "meta" in df and "uri" in df["meta"]:
                    df_uri = df["meta"]["uri"]
                    if df_uri in displayed_attribute_uris:
                        # Found a displayed form, return it with its identifier
                        return df, df["meta"]["identifier"]

        # Check if any displayed URI is a display form that belongs to our attribute
        for displayed_uri in displayed_attribute_uris:
            parent_attr_uri = get_attribute_uri_from_display_form(ctx, displayed_uri)
            if parent_attr_uri == attribute_uri:
                # This displayed URI is a display form of our attribute
                try:
                    df_obj = ctx.legacy_client.get_object(displayed_uri)
                    if "attributeDisplayForm" in df_obj:
                        df_identifier = df_obj["attributeDisplayForm"]["meta"][
                            "identifier"
                        ]
                        return df_obj["attributeDisplayForm"], df_identifier
                except Exception:
                    continue

        return None, None
    except Exception:
        return None, None


def attribute_has_displayed_form(
    ctx: ContextWithWarnings, attribute_uri, displayed_attribute_uris
):
    """
    Check if an attribute has any of its display forms displayed in the visualization.

    Args:
        ctx: The context object with API and mappings
        attribute_uri: The URI of the attribute to check
        displayed_attribute_uris: Set of displayed attribute/display form URIs

    Returns:
        bool: True if the attribute has any display form displayed, False otherwise
    """
    display_form, _ = _find_displayed_form_for_attribute(
        ctx, attribute_uri, displayed_attribute_uris
    )
    return display_form is not None


def get_displayed_form_identifier(
    ctx: ContextWithWarnings, attribute_uri, displayed_attribute_uris
):
    """
    Get the mapped identifier of the displayed form for an attribute.

    Args:
        ctx: The context object with API and mappings
        attribute_uri: The URI of the attribute to check
        displayed_attribute_uris: Set of displayed attribute/display form URIs

    Returns:
        str: The mapped identifier of the displayed form, or None if not found
    """
    _, df_identifier = _find_displayed_form_for_attribute(
        ctx, attribute_uri, displayed_attribute_uris
    )
    if df_identifier:
        return ctx.ldm_mappings.search_mapping_identifier(df_identifier)
    return None


def check_exact_granularity_match(
    ctx: ContextWithWarnings, filter_attributes, displayed_attributes
):
    """
    Check if the filter attributes exactly match the visualization attributes.

    Args:
        ctx: The context object with API and mappings
        filter_attributes (list): List of attribute URIs from the filter
        displayed_attributes (set): Set of displayed attribute/display form URIs from visualization

    Returns:
        tuple: (is_exact_match, filter_attr_identifiers, viz_attr_identifiers)
    """
    try:
        # If no filter attributes, this is not a match case
        if not filter_attributes:
            return False, [], []

        # Convert displayed attributes (which may be display forms) to their parent attributes
        parent_attributes = get_parent_attributes_from_display_forms(
            ctx, displayed_attributes
        )

        # Get identifiers for filter attributes
        filter_attr_identifiers = set()
        for attribute_uri in filter_attributes:
            try:
                attr_obj = ctx.legacy_client.get_object(attribute_uri)
                attr_identifier = (
                    attr_obj.get("attribute", {}).get("meta", {}).get("identifier", "")
                )

                if attr_identifier:
                    # Map to Cloud identifier for comparison
                    mapped_id = ctx.ldm_mappings.search_mapping_identifier(
                        attr_identifier
                    )
                    final_id = mapped_id if mapped_id else attr_identifier
                    filter_attr_identifiers.add(final_id)
            except Exception:
                # If we can't process an attribute, it can't be an exact match
                return False, [], []

        # Get identifiers for visualization attributes
        viz_attr_identifiers = set()
        for attr_uri in parent_attributes:
            try:
                attr_obj = ctx.legacy_client.get_object(attr_uri)
                attr_identifier = (
                    attr_obj.get("attribute", {}).get("meta", {}).get("identifier", "")
                )

                if attr_identifier:
                    # Map to Cloud identifier for comparison
                    mapped_id = ctx.ldm_mappings.search_mapping_identifier(
                        attr_identifier
                    )
                    final_id = mapped_id if mapped_id else attr_identifier
                    viz_attr_identifiers.add(final_id)
            except Exception:
                # If we can't process an attribute, it can't be an exact match
                return False, [], []

        # Check if sets are exactly equal
        is_exact_match = (
            filter_attr_identifiers == viz_attr_identifiers
            and len(filter_attr_identifiers) > 0
        )

        return (
            is_exact_match,
            sorted(list(filter_attr_identifiers)),
            sorted(list(viz_attr_identifiers)),
        )

    except Exception:
        return False, [], []


def get_metric_identifier_for_warnings(ctx: ContextWithWarnings, measure_uri):
    """
    Get the metric identifier for use in warning messages.

    Args:
        ctx: The context object with API and mappings
        measure_uri (str): The measure URI

    Returns:
        str: The metric identifier (Cloud if available, otherwise Legacy identifier or URI)
    """
    try:
        # Get the Legacy identifier
        metric_obj = ctx.legacy_client.get_object(measure_uri)
        if (
            metric_obj
            and "metric" in metric_obj
            and "meta" in metric_obj["metric"]
            and "identifier" in metric_obj["metric"]["meta"]
        ):
            legacy_identifier = metric_obj["metric"]["meta"]["identifier"]

            # Convert to Cloud identifier
            cloud_identifier = ctx.metric_mappings.search_mapping_identifier(
                legacy_identifier
            )
            if cloud_identifier:
                return cloud_identifier
            else:
                # Fallback to Legacy identifier if mapping not found
                return legacy_identifier
        else:
            # If couldn't get Legacy ID, use just the URI
            return measure_uri
    except Exception:
        # Keep fallback identifier in case of errors
        return measure_uri
