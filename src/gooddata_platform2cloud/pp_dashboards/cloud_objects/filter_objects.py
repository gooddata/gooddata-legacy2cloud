# (C) 2026 GoodData Corporation
import uuid
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


# --- Core identifier objects ---
class Identifier(BaseModel):
    id: str
    type: str


class DisplayForm(BaseModel):
    identifier: Identifier


class DataSet(BaseModel):
    identifier: Identifier


class DefaultDateFilter(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    type: str
    granularity: str
    to: int
    from_: int = Field(alias="from", default=None)
    title: Optional[str] = ""
    localIdentifier: Optional[str] = Field(default_factory=lambda: uuid.uuid4().hex)


class DateFilter(DefaultDateFilter):
    dataSet: Optional[DataSet] = Field(default_factory=dict)


class AttributeElements(BaseModel):
    uris: Optional[List[str]] = Field(default_factory=list)


class SelectionMode(str, Enum):
    SINGLE = "single"
    MULTI = "multi"


class AttributeFilter(BaseModel):
    attributeElements: AttributeElements
    displayForm: DisplayForm
    negativeSelection: bool
    selectionMode: SelectionMode = (
        SelectionMode.SINGLE
    )  # Only "single" or "multi" allowed
    localIdentifier: Optional[str] = Field(default_factory=lambda: uuid.uuid4().hex)
    title: Optional[str] = ""


# Union wrapper for filters
class AttributeFilterWrapper(BaseModel):
    attributeFilter: AttributeFilter | None = None


class DateFilterWrapper(BaseModel):
    dateFilter: DateFilter | DefaultDateFilter | None = None


# --- Content ---
class Content(BaseModel):
    filters: List[DateFilterWrapper | AttributeFilterWrapper]
    version: Optional[str] = "2"


# --- Attributes ---
class Attributes(BaseModel):
    content: Content
    title: str
    description: Optional[str] = ""


# --- Root object ---
class FilterContext(BaseModel):
    id: str
    attributes: Attributes
    type: str = "filterContext"

    def add_filter(self, filter):
        self.attributes.content.filters.append(filter)
