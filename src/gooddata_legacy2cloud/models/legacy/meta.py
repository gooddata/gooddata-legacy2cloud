# (C) 2026 GoodData Corporation
from pydantic import Field

from gooddata_legacy2cloud.models.base import Base


class Meta(Base):
    tags: str
    deprecated: str
    category: str
    is_production: int
    created: str
    contributor: str
    updated: str
    shared_with_someone: int | None = None
    summary: str
    author: str
    uri: str
    identifier: str
    title: str
    unlisted: int | None = None
    flags: list[str] = Field(default_factory=list)
