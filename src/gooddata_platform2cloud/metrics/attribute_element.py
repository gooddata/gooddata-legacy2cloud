# (C) 2026 GoodData Corporation
"""
This module is responsible for searching for the attribute element values in Platform
and preparing Cloud ones.
"""

import logging
import re
from datetime import datetime

from gooddata_platform2cloud.dashboards.data_classes import DashboardContext
from gooddata_platform2cloud.insights.data_classes import InsightContext
from gooddata_platform2cloud.metrics.contants import (
    DAY_OF_WEEK_PARSING_MASKS,
    DAY_PARSING_MASKS,
    DELETED_VALUE,
    MISSING_VALUE,
    MONTH_PARSING_MASK,
    MONTH_YEAR_PARSING_MASKS,
    PARSING_MASKS,
    QUARTER_IDs,
    WEEK_IDs,
    YEAR_IDs,
)
from gooddata_platform2cloud.metrics.data_classes import MetricContext
from gooddata_platform2cloud.metrics.display_form_utils import get_primary_display_form
from gooddata_platform2cloud.metrics.utils import parse_day_shortcut_to_number
from gooddata_platform2cloud.reports.data_classes import ContextWithWarnings

logger = logging.getLogger("migration")


class AttributeElement:
    """
    Class searches for the attribute element values in Platform
    """

    def __init__(
        self,
        ctx: MetricContext | InsightContext | ContextWithWarnings | DashboardContext,
        source: str,
    ):
        self.ctx = ctx
        self.source = source
        self.value = ""
        self.type = ""
        self.link = ""
        self.object_link = ""
        self.object_id = ""
        self.element_id = ""
        self.object_data = None
        self.display_form_data = None
        self.warning_uri = source
        self._setup_identifiers()
        self._process_element()

    def _setup_identifiers(self):
        """
        Set up the identifiers.
        """
        pattern = r"(/gdc/md/[^/]+/obj/(\d+))/elements\?id=(\d+)"
        match = re.match(pattern, self.source)
        if match:
            self.object_link = match.group(1)
            self.link = self.object_link
            self.object_id = match.group(2)
            self.element_id = match.group(3)
        else:
            raise ValueError(f"Unknown link format: {self.source}")

    @staticmethod
    def _extract_element_id(uri: str) -> str:
        """
        Extract element id from URI (.../elements?id=<id>).
        """
        if not uri:
            return ""
        match = re.search(r"[?&]id=([^&]+)", uri)
        return match.group(1) if match else ""

    @staticmethod
    def _is_default_display_form(display_form: dict) -> bool:
        content = display_form.get("content", {})
        default = content.get("default")
        return default in [1, "1", True]

    def _order_display_forms(self, display_forms: list[dict]) -> list[dict]:
        """
        Order display forms: default first, then remaining in original order.
        """
        defaults = [df for df in display_forms if self._is_default_display_form(df)]
        non_defaults = [
            df for df in display_forms if not self._is_default_display_form(df)
        ]
        return defaults + non_defaults

    def _get_candidate_element_endpoints(
        self, obj: dict
    ) -> list[tuple[str, dict | None]]:
        """
        Resolve candidate element endpoints from object definition.
        Returns list of (elements_link, display_form_data) tuples.
        """
        candidates: list[tuple[str, dict | None]] = []

        if "attributeDisplayForm" in obj:
            display_form = obj["attributeDisplayForm"]
            elements_link = display_form.get("links", {}).get("elements")
            if elements_link:
                candidates.append((elements_link, display_form))
            return candidates

        if "attribute" in obj:
            display_forms = obj["attribute"].get("content", {}).get("displayForms", [])
            for display_form in self._order_display_forms(display_forms):
                elements_link = display_form.get("links", {}).get("elements")
                if elements_link:
                    candidates.append((elements_link, display_form))
        return candidates

    def _load_object_metadata(self, obj: dict) -> None:
        """
        Set object_data/type/display_form_data when available.
        """
        if "attribute" in obj:
            self.object_data = obj["attribute"]
            if "type" in obj["attribute"].get("content", {}):
                self.type = obj["attribute"]["content"]["type"]
            return

        if "attributeDisplayForm" in obj:
            self.display_form_data = obj["attributeDisplayForm"]
            form_of = obj["attributeDisplayForm"].get("content", {}).get("formOf")
            if form_of:
                attribute_obj = self.ctx.platform_client.get_object(form_of)
                if "attribute" in attribute_obj:
                    self.object_data = attribute_obj["attribute"]
                    if "type" in self.object_data.get("content", {}):
                        self.type = self.object_data["content"]["type"]

    def get_cloud_formatted_date(self, value, format_identifier):
        """
        Get the formatted date.
        """
        if value == "":
            return value
        elif format_identifier in PARSING_MASKS:
            date_object = datetime.strptime(value, PARSING_MASKS[format_identifier])

            if format_identifier in DAY_PARSING_MASKS:
                return f"{date_object.year}-{date_object.month:02}-{date_object.day:02}"
            elif format_identifier in MONTH_YEAR_PARSING_MASKS:
                return f"{date_object.year}-{date_object.month:02}"
            elif format_identifier in MONTH_PARSING_MASK:
                return f"{date_object.month:02}"
            elif format_identifier in DAY_OF_WEEK_PARSING_MASKS:
                return f"{date_object.weekday():02}"
            return f"{date_object.year}-{date_object.month:02}-{date_object.day:02}"

        elif format_identifier in QUARTER_IDs:
            quarter, year = value[1:].split("/")
            return f"{year}-{quarter}"

        elif format_identifier in WEEK_IDs:
            week, year = value[1:].split("/")
            return f"{year}-{week:02}"

        elif format_identifier in "day.in.euweek.short":
            return parse_day_shortcut_to_number(value)

        elif (
            format_identifier in "day.in.year.default" or format_identifier in YEAR_IDs
        ):
            return value

        else:
            logger.warning("Unknown date format: %s", format_identifier)
            return value

    def _format_cloud_date_value(self, value):
        """
        Format the date value.
        be ware there are two date identifiers within Platform object
        """

        if (
            self.display_form_data
            and "meta" in self.display_form_data
            and "identifier" in self.display_form_data["meta"]
        ):
            identifier = self.display_form_data["meta"]["identifier"]
            date_identifier = ".".join(identifier.split(".")[1:])

            return self.get_cloud_formatted_date(value, date_identifier)

        return value

    def _get_cloud_value(self, value):
        """
        Get the value of the attribute element.
        """
        if (
            self.object_data
            and "content" in self.object_data
            and "type" in self.object_data["content"]
            and "GDC.time" in self.object_data["content"]["type"]
        ):
            return self._format_cloud_date_value(value)

        if value == "":
            # Return None so the value of (empty value) filter is null in JSON
            return None
        return value

    def _get_primary_display_form(self, display_forms):
        """
        Get the primary display form.
        """
        return get_primary_display_form(display_forms)

    def _process_element(self):
        """
        Get the value of the attribute element.
        """
        attr_elems = self.ctx.platform_client.cache_attribute_elements
        if self.source in attr_elems:
            cached_value = attr_elems[self.source]
            if cached_value is not None:
                self.value = self._get_cloud_value(cached_value)
                return

        try:
            output = self.ctx.platform_client.get_object(self.object_link)
        except Exception:
            self.value = MISSING_VALUE
            return

        self._load_object_metadata(output)
        candidates = self._get_candidate_element_endpoints(output)
        is_attribute_source = "attribute" in output

        if not candidates:
            self.value = MISSING_VALUE
            return

        # For unresolved warnings, prefer resolved display-form endpoint when we
        # can confirm concrete display-form metadata; otherwise keep source URI.
        first_display_form = candidates[0][1] or {}
        if (
            is_attribute_source
            and "meta" in first_display_form
            and "uri" in first_display_form["meta"]
        ):
            self.warning_uri = f"{candidates[0][0]}?id={self.element_id}"

        had_empty_response = False
        had_true_missing = False

        for elements_link, display_form_data in candidates:
            endpoint = f"{elements_link}?id={self.element_id}"

            try:
                elements_output = self.ctx.platform_client.get_object(endpoint)
            except Exception:
                had_true_missing = True
                continue

            attribute_elements = elements_output.get("attributeElements")
            if not attribute_elements or "elements" not in attribute_elements:
                had_true_missing = True
                continue

            elements = attribute_elements["elements"]
            if not elements:
                had_empty_response = True
                continue

            for element in elements:
                element_uri = element.get("uri", "")
                if (
                    self._extract_element_id(element_uri) == self.element_id
                    or element_uri == self.source
                ):
                    if display_form_data:
                        self.display_form_data = display_form_data
                    self.value = self._get_cloud_value(element.get("title", ""))
                    return

            # Non-empty elements response, but target id not found.
            had_true_missing = True

        # Treat as deleted/stale for attribute-sourced URIs when all resolved
        # display-form candidates return explicit empty element lists.
        if had_empty_response and not had_true_missing and is_attribute_source:
            self.value = DELETED_VALUE
            return

        self.value = MISSING_VALUE

    def getObject(self):
        return self.object_data

    def getDisplayForm(self):
        return self.display_form_data

    def isMissingValue(self):
        return self.value == MISSING_VALUE

    def isDeletedValue(self):
        return self.value == DELETED_VALUE

    def get(self):
        """
        Get the value of the attribute element.
        """
        return self.value

    def get_type(self):
        return self.type

    def get_warning_uri(self):
        return self.warning_uri
