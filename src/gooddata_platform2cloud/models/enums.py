# (C) 2026 GoodData Corporation
from enum import Enum


class ExportFormat(str, Enum):
    CSV = "CSV"
    XLSX = "XLSX"
    PDF = "PDF"


class DateFilterType(str, Enum):
    RELATIVE = "relative"
    ABSOLUTE = "absolute"


class DateFilterTypeName(str, Enum):
    RELATIVE = "relative_date_filter"
    ABSOLUTE = "absolute_date_filter"


class AttributeFilterTypeName(str, Enum):
    NEGATIVE = "negative_attribute_filter"
    POSITIVE = "positive_attribute_filter"


class SkippingOrUpdating(str, Enum):
    """Represents whether an object present upstream should be skipped or updated."""

    SKIPPING = "Skipping"
    UPDATING = "Updating"


class Action(str, Enum):
    """Represents the action to be performed on an object."""

    CREATE = "create"
    UPDATE = "update"

    def past(self) -> str:
        """Returns the action name in past tense."""
        return f"{self.value[:-1]}ed"

    def continuous(self) -> str:
        """Returns the continuous form of the action name."""
        return f"{self.value[:-1]}ing"


class Operation(str, Enum):
    CREATE_WITH_RETRY = "create_with_retry"
    CREATE_OR_UPDATE_WITH_RETRY = "create_or_update_with_retry"
    CREATE_WITH_ERROR_FALLBACK = "create_with_error_fallback"
    CREATE_OR_UPDATE_WITH_ERROR_FALLBACK = "create_or_update_with_error_fallback"
