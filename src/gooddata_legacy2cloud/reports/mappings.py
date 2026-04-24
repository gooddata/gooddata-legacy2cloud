# (C) 2026 GoodData Corporation
"""
This module contains the ReportMappings class, which extends IdMappings
to provide specific mapping functionality for reports.
"""

from gooddata_legacy2cloud.id_mappings import IdMappings
from gooddata_legacy2cloud.reports.data_classes import ContextWithWarnings


class ReportMappings(IdMappings):
    """
    Class for handling Legacy to Cloud mappings for reports.
    Extends the base IdMappings class with report-specific mapping methods.
    """

    def map_metric(
        self, ctx: ContextWithWarnings, metric_obj: dict, index: int
    ) -> dict:
        """
        Maps a Legacy metric to a Cloud measure object.

        Args:
            ctx: The context object with API and mappings
            metric_obj (dict): The Legacy metric object
            index (int): The index of the metric for generating fallback names

        Returns:
            dict: The mapped Cloud measure object
        """
        metric_uri = metric_obj.get("uri", "")
        if not metric_uri:
            metric_uri = f"MISSING_METRIC_{index}"
        # Retrieve the custom alias and format as provided in the Legacy report
        custom_alias = metric_obj.get("alias", "")
        custom_format = metric_obj.get("format", "")
        try:
            obj = ctx.legacy_client.get_object(metric_uri)
            # Expect the object to have the metric definition under the "metric" key
            legacy_identifier = obj["metric"]["meta"]["identifier"]
            local_id = ctx.metric_mappings.search_mapping_identifier(legacy_identifier)
            # Use the Legacy metric meta title as base title regardless of a custom alias
            base_title = obj["metric"]["meta"]["title"]
        except Exception:
            local_id = metric_uri
            base_title = f"Metric {index}"
        measure_dict = {
            "localIdentifier": local_id,
            "definition": {
                "measureDefinition": {
                    "item": {"identifier": {"id": local_id, "type": "metric"}},
                    "filters": [],
                }
            },
            "title": base_title,
            "alias": custom_alias,
            "format": custom_format,
        }
        return {"measure": measure_dict}

    def map_attribute(
        self, ctx: ContextWithWarnings, attr_obj: dict, index: int
    ) -> dict:
        """
        Maps a Legacy attribute to a Cloud attribute object.

        Args:
            ctx: The context object with API and mappings
            attr_obj (dict): The Legacy attribute object
            index (int): The index of the attribute for generating fallback names

        Returns:
            dict: The mapped Cloud attribute object
        """
        attr_uri = attr_obj.get("uri", "")
        if not attr_uri:
            attr_uri = f"MISSING_ATTRIBUTE_{index}"
        custom_alias = attr_obj.get("alias", "")
        try:
            obj = ctx.legacy_client.get_object(attr_uri)
            if "attribute" in obj:
                legacy_identifier = obj["attribute"]["meta"]["identifier"]
            elif "attributeDisplayForm" in obj:
                legacy_identifier = obj["attributeDisplayForm"]["meta"]["identifier"]
            else:
                legacy_identifier = attr_uri
            local_id = ctx.ldm_mappings.search_mapping_identifier(legacy_identifier)
        except Exception:
            local_id = attr_uri
        return {
            "attribute": {
                "localIdentifier": local_id,
                "displayForm": {"identifier": {"id": local_id, "type": "label"}},
                "alias": custom_alias,
            }
        }

    def map_total_entry(
        self, measure_local_id: str, attribute_local_id: str, total_type: str = "sum"
    ) -> dict:
        """
        Creates a totals entry for a measure and attribute.

        Args:
            measure_local_id (str): The local ID of the measure
            attribute_local_id (str): The local ID of the attribute
            total_type (str, optional): The type of total calculation. Defaults to "sum".

        Returns:
            dict: The totals entry object
        """
        return {
            "type": total_type,
            "measureIdentifier": measure_local_id,
            "attributeIdentifier": attribute_local_id,
        }

    def map_chart_metric(
        self, ctx: ContextWithWarnings, item: dict, index: int
    ) -> dict:
        """
        Maps a Legacy chart metric to a Cloud measure object.

        Args:
            ctx: The context object with API and mappings
            item (dict): The Legacy chart metric item
            index (int): The index of the metric for generating fallback names

        Returns:
            dict: The mapped Cloud measure object
        """
        metric_uri = item.get("uri", "")
        if not metric_uri:
            metric_uri = f"MISSING_CHART_METRIC_{index}"
        # Retrieve the custom alias and format from the chart item
        custom_alias = item.get("alias", "")
        custom_format = item.get("format", "")
        try:
            obj = ctx.legacy_client.get_object(metric_uri)
            legacy_identifier = obj["metric"]["meta"]["identifier"]
            local_id = ctx.metric_mappings.search_mapping_identifier(legacy_identifier)
            base_title = obj["metric"]["meta"]["title"]
        except Exception:
            local_id = metric_uri
            base_title = f"Metric {index}"
        return {
            "measure": {
                "localIdentifier": local_id,
                "definition": {
                    "measureDefinition": {
                        "item": {"identifier": {"id": local_id, "type": "metric"}},
                        "filters": [],
                    }
                },
                "title": base_title,
                "alias": custom_alias,
                "format": custom_format,
            }
        }

    def map_chart_attribute(
        self, ctx: ContextWithWarnings, item: dict, index: int
    ) -> dict:
        """
        Maps a Legacy chart attribute to a Cloud attribute object.

        Args:
            ctx: The context object with API and mappings
            item (dict): The Legacy chart attribute item
            index (int): The index of the attribute for generating fallback names

        Returns:
            dict: The mapped Cloud attribute object
        """
        attr_uri = item.get("uri", "")
        if not attr_uri:
            attr_uri = f"MISSING_CHART_ATTRIBUTE_{index}"
        custom_alias = item.get("alias", "")
        try:
            obj = ctx.legacy_client.get_object(attr_uri)
            if "attribute" in obj:
                legacy_identifier = obj["attribute"]["meta"]["identifier"]
            elif "attributeDisplayForm" in obj:
                legacy_identifier = obj["attributeDisplayForm"]["meta"]["identifier"]
            else:
                legacy_identifier = attr_uri
            local_id = ctx.ldm_mappings.search_mapping_identifier(legacy_identifier)
        except Exception:
            local_id = attr_uri
        return {
            "attribute": {
                "localIdentifier": local_id,
                "displayForm": {"identifier": {"id": local_id, "type": "label"}},
                "alias": custom_alias,
            }
        }


# TODO: Decide whether to keep the wrappers or whether to redesign the class.
# The class appears to only wrap what would be the function contents and is not
# used anywhere by itself.


# For backward compatibility, expose the class methods as module-level functions
# This allows existing code to continue working without changes
def map_metric(ctx: ContextWithWarnings, metric_obj: dict, index: int) -> dict:
    """Backward compatibility function that calls ReportMappings.map_metric"""
    mapper = ReportMappings()
    return mapper.map_metric(ctx, metric_obj, index)


def map_attribute(ctx: ContextWithWarnings, attr_obj: dict, index: int) -> dict:
    """Backward compatibility function that calls ReportMappings.map_attribute"""
    mapper = ReportMappings()
    return mapper.map_attribute(ctx, attr_obj, index)


def map_total_entry(
    measure_local_id: str, attribute_local_id: str, total_type: str = "sum"
) -> dict:
    """Backward compatibility function that calls ReportMappings.map_total_entry"""
    mapper = ReportMappings()
    return mapper.map_total_entry(measure_local_id, attribute_local_id, total_type)


def map_chart_metric(ctx: ContextWithWarnings, item: dict, index: int) -> dict:
    """Backward compatibility function that calls ReportMappings.map_chart_metric"""
    mapper = ReportMappings()
    return mapper.map_chart_metric(ctx, item, index)


def map_chart_attribute(ctx: ContextWithWarnings, item: dict, index: int) -> dict:
    """Backward compatibility function that calls ReportMappings.map_chart_attribute"""
    mapper = ReportMappings()
    return mapper.map_chart_attribute(ctx, item, index)
