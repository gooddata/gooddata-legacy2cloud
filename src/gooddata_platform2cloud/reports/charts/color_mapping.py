# (C) 2026 GoodData Corporation
"""
Color mapping for Platform to Cloud chart migration.

This module handles processing of color mapping configurations for chart reports.
"""

from gooddata_platform2cloud.metrics.attribute_element import AttributeElement
from gooddata_platform2cloud.metrics.contants import MISSING_VALUE
from gooddata_platform2cloud.reports.data_classes import ContextWithWarnings


def process_color_mapping(ctx: ContextWithWarnings, chart, properties):
    """
    Process color mapping configuration from Platform chart styles.

    Args:
        ctx (ReportContext): The context object containing mappings
        chart (dict): The Platform chart configuration
        properties (dict): The properties dictionary to update

    Returns:
        dict: Updated properties with temporary color mapping data
    """
    styles = chart.get("styles", {})
    global_styles = styles.get("global", {})

    if "colorMapping" in global_styles:
        color_mapping = global_styles.get("colorMapping", [])
        missing_elements = []  # collect Platform element URIs that return missing value
        color_element_map = {}  # map to store element values by URI

        # First pass: Collect element values and GUIDs
        for item in color_mapping:
            platform_guid = item.get("guid")
            platform_uri = item.get("uri")
            if not platform_guid or not platform_uri:
                continue

            # Format the GUID value
            if platform_guid.startswith("guid"):
                index_part = platform_guid[len("guid") :]
            else:
                index_part = platform_guid
            try:
                index_number = int(index_part)
                cloud_guid = f"guid{index_number}"
            except ValueError:
                cloud_guid = platform_guid

            # If the URI indicates an element, translate it using AttributeElement and store it
            if "/elements?id=" in platform_uri:
                converted_id = AttributeElement(ctx, platform_uri).get()
                if converted_id == MISSING_VALUE:
                    missing_elements.append(platform_uri)
                    continue  # Skip this mapping item
                color_element_map[platform_uri] = {
                    "id_type": "element",
                    "id": converted_id,
                    "guid": cloud_guid,
                }
            else:
                try:
                    platform_obj = ctx.platform_client.get_object(platform_uri)
                    if "metric" in platform_obj:
                        platform_metric_identifier = platform_obj["metric"]["meta"][
                            "identifier"
                        ]
                    else:
                        platform_metric_identifier = platform_uri
                    converted_id = ctx.metric_mappings.search_mapping_identifier(
                        platform_metric_identifier
                    )
                    color_element_map[platform_uri] = {
                        "id_type": "metric",
                        "id": converted_id,
                        "guid": cloud_guid,
                    }
                except Exception:
                    color_element_map[platform_uri] = {
                        "id_type": "unknown",
                        "id": platform_uri,
                        "guid": cloud_guid,
                    }

        if missing_elements:
            try:
                # Get the display form URI by stripping the '/elements' part.
                df_uri = missing_elements[0].split("/elements")[0]
                df_obj = ctx.platform_client.get_object(df_uri)
                if "attributeDisplayForm" in df_obj:
                    attribute_cloud_id = ctx.ldm_mappings.search_mapping_identifier(
                        df_obj["attributeDisplayForm"]["meta"]["identifier"]
                    )
                else:
                    attribute_cloud_id = df_uri
            except Exception:
                attribute_cloud_id = missing_elements[0]
            ctx.log_info(
                f"missing values in color mapping for attribute {attribute_cloud_id}: {', '.join(missing_elements)}",
                to_stderr=True,
            )

        # Store the original color mapping data to be processed after buckets are created
        if color_element_map:
            properties["__temp_color_mapping"] = color_element_map

    return properties


def apply_color_mapping(properties, buckets_final):
    """
    Apply color mapping to the final buckets.

    Args:
        properties (dict): The properties dictionary with temp color mapping
        buckets_final (list): The final buckets configuration

    Returns:
        dict: Updated properties with color mapping applied
    """
    if "__temp_color_mapping" not in properties:
        return properties

    color_element_map = properties.pop("__temp_color_mapping")
    cloud_color_mapping = []

    # First build a map of metric ids to measure localIdentifiers
    measure_id_map = {}
    attribute_id_map = {}
    for bucket in buckets_final:
        for item in bucket.get("items", []):
            if "measure" in item:
                measure_def = item["measure"]["definition"].get("measureDefinition", {})
                measure_id = measure_def.get("item", {}).get("identifier", {}).get("id")
                if measure_id:
                    measure_id_map[measure_id] = item["measure"]["localIdentifier"]
            elif "attribute" in item:
                attribute_id = item["attribute"]["displayForm"]["identifier"].get("id")
                if attribute_id:
                    attribute_id_map[attribute_id] = item["attribute"][
                        "localIdentifier"
                    ]

    # Now create the color mapping with the correct identifiers
    for uri, mapping_data in color_element_map.items():
        id_type = mapping_data["id_type"]
        converted_id = mapping_data["id"]
        cloud_guid = mapping_data["guid"]

        final_id = None
        if id_type == "metric" and converted_id in measure_id_map:
            final_id = measure_id_map[converted_id]
        elif id_type == "element":
            final_id = converted_id
        else:
            # Keep the original ID if not found
            final_id = converted_id

        cloud_color_mapping.append(
            {"color": {"type": "guid", "value": cloud_guid}, "id": final_id}
        )

    if cloud_color_mapping:
        properties["controls"]["colorMapping"] = cloud_color_mapping

    return properties
