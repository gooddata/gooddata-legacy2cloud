# (C) 2026 GoodData Corporation
from gooddata_platform2cloud.models.base import Base


class Entry(Base):
    author: str
    category: str
    created: str
    deprecated: str
    identifier: str
    link: str
    locked: int
    summary: str
    title: str
    unlisted: int
    updated: str


class Entries(Base):
    entries: list[Entry]


class QueryResult(Base):
    entries: list[Entry]


class QueryResultWrapper(Base):
    """Wrapper for Platform query result.

    Necessary because get_metrics and get_attributes return different structures.
    """

    query: QueryResult
