# (C) 2026 GoodData Corporation
from typing import Literal

from pydantic import Field

from gooddata_platform2cloud.models.base import Base
from gooddata_platform2cloud.models.cloud.identifier import IdentifierWrapper
from gooddata_platform2cloud.models.cloud.uris import Uris


class DateFilter(Base):
    type_: str = Field(alias="type")
    granularity: str
    to: int | str | None = Field(default=None)
    from_: int | str | None = Field(alias="from", default=None)
    local_identifier: str


class AttributeFilter(Base):
    local_identifier: str
    attribute_elements: Uris
    display_form: IdentifierWrapper
    negative_selection: bool
    selection_mode: str | None = Field(default=None)


class Filter(Base):
    attribute_filter: AttributeFilter | None = None
    date_filter: DateFilter | None = None


class Content(Base):
    filters: list[Filter]
    version: int = 2


class Attributes(Base):
    title: str
    description: str = ""
    content: Content


class Links(Base):
    self_: str = Field(alias="self")


class Origin(Base):
    origin_type: str
    origin_id: str


class Meta(Base):
    origin: Origin


class FilterContextModel(Base):
    id_: str = Field(alias="id")
    type_: Literal["filterContext"] = Field(alias="type")
    attributes: Attributes
    links: Links | None = None
    meta: Meta | None = None


class FilterContextWrapper(Base):
    data: FilterContextModel
