# (C) 2026 GoodData Corporation
from pydantic import Field

from gooddata_legacy2cloud.models.base import Base


class Identifier(Base):
    id_: str = Field(alias="id")
    type_: str = Field(alias="type")


class IdentifierWrapper(Base):
    identifier: Identifier
