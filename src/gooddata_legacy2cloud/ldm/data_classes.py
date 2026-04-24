# (C) 2026 GoodData Corporation
"""
This module contains data classes for
FactBuilderConfig and AttributeBuilderConfig.
"""

from dataclasses import dataclass

from gooddata_legacy2cloud.ldm.ads_mapping import ADSMapping
from gooddata_legacy2cloud.ldm.tag_provider import TagProvider
from gooddata_legacy2cloud.output_writer import OutputWriter


@dataclass(frozen=True)
class FactBuilderConfig:
    """
    The FactBuilderConfig class contains the configuration
    required to build a Cloud fact.
    """

    legacy_fact: list
    legacy_dataset_id: str
    logger: OutputWriter
    ADSMapping: ADSMapping
    TagProvider: TagProvider
    ignore_folders: bool


@dataclass(frozen=True)
class AttributeBuilderConfig:
    """
    The AttributeBuilderConfig class contains the configuration
    required to build Cloud attributes.
    """

    legacy_dataset_id: str
    logger: OutputWriter
    ADSMapping: ADSMapping
    TagProvider: TagProvider
    ignore_folders: bool
