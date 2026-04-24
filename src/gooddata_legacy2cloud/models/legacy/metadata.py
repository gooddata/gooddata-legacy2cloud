# (C) 2026 GoodData Corporation
from gooddata_legacy2cloud.models.base import Base


class MetadataLinks(Base):
    identifier: str


class AboutMetadata(Base):
    links: list[MetadataLinks]


class Metadata(Base):
    """Domain metadata.

    Result of domain/md/
    """

    about: AboutMetadata
