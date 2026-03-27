# (C) 2026 GoodData Corporation
from typing import TypeAlias

from gooddata_platform2cloud.models.cloud.automations import (
    AbsoluteDateFilter,
    NegativeAttributeFilter,
    PositiveAttributeFilter,
    RelativeDateFilter,
)

# String aliases (for better readability of maps)
UserEmail: TypeAlias = str
CloudDashboardId: TypeAlias = str
CloudInsightId: TypeAlias = str
CloudWidgetId: TypeAlias = str

# Filter model type (class) aliases
DateFilterModelType: TypeAlias = type[RelativeDateFilter | AbsoluteDateFilter]
AttributeFilterModelType: TypeAlias = type[
    NegativeAttributeFilter | PositiveAttributeFilter
]

# Filter model instance alias
FilterInstance: TypeAlias = (
    RelativeDateFilter
    | AbsoluteDateFilter
    | NegativeAttributeFilter
    | PositiveAttributeFilter
)
