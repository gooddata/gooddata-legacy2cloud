# (C) 2026 GoodData Corporation
from typing import Literal

from pydantic import Field

from gooddata_legacy2cloud.models.base import Base
from gooddata_legacy2cloud.models.cloud.meta import Meta


class Attributes(Base):
    """Attributes."""

    description: str
    granularity: str | None = None
    sort_column: str | None = None
    sort_dircetion: str | None = None
    source_column: str
    source_column_data_type: str
    tags: list[str]
    title: str


class AttributeModel(Base):
    """Attribute model."""

    attributes: Attributes
    id_: str = Field(alias="id")
    meta: Meta
    # relationships
    type_: Literal["attribute"] = Field(alias="type")


class AttributeWrapper(Base):
    """Attribute wrapper."""

    data: AttributeModel
    # included
    # links
