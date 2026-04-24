# (C) 2026 GoodData Corporation
import logging
from typing import Annotated, Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from gooddata_legacy2cloud.pp_dashboards.utils import Meta

logger = logging.getLogger("migration")


class AttributeElement(BaseModel):
    title: str
    uri: str


class AttributeElements(BaseModel):
    elements: list[AttributeElement]

    @model_validator(mode="before")
    @classmethod
    def unwrap(cls, v):
        # unwrap only if the wrapper key is present; otherwise keep as-is
        if isinstance(v, dict) and "attributeElements" in v:
            return v["attributeElements"]
        return v


class Elements(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    elements: str | AttributeElements
    ctx: Any | None = Field(default=None, exclude=True, repr=False)

    def initialize_from_legacy(self):
        """
        Fetch and initialize elements from Legacy API.
        This must be called explicitly after setting context.
        """
        if self.ctx and isinstance(self.elements, str):
            try:
                self.elements = AttributeElements.model_validate(
                    self.ctx.legacy_client.get_object(self.elements)
                )
            except Exception as e:
                logger.error("Error initializing Elements: %s", e)


class DisplayForm(BaseModel):
    class Content(BaseModel):
        default: int | None = 0

    content: Content
    links: Elements
    meta: Meta


class Attribute(BaseModel):
    class Content(BaseModel):
        displayForms: list[DisplayForm] = Field(default_factory=list)
        type: str | None = ""

    content: Content
    meta: Meta

    @model_validator(mode="before")
    @classmethod
    def unwrap(cls, v):
        # unwrap only if the wrapper key is present; otherwise keep as-is
        if isinstance(v, dict) and "attribute" in v:
            return v["attribute"]
        return v


class AttributeDisplayForm(BaseModel):
    model_config = ConfigDict(extra="ignore", arbitrary_types_allowed=True)

    class Content(BaseModel):
        formOf: str | Attribute

    content: Content
    links: Elements
    meta: Meta
    ctx: Any | None = Field(default=None, exclude=True, repr=False)

    @model_validator(mode="before")
    @classmethod
    def unwrap(cls, v):
        # If it's wrapped, unwrap; otherwise leave it as-is so Union can handle it
        if isinstance(v, dict) and "attributeDisplayForm" in v:
            return v["attributeDisplayForm"]
        return v

    def initialize_from_legacy(self):
        """
        Fetch and initialize formOf object from Legacy API.
        This must be called explicitly after setting context.
        """
        if not self.ctx or not isinstance(self.content.formOf, str):
            return

        # Initialize links first
        if hasattr(self.links, "ctx"):
            self.links.ctx = self.ctx
            if hasattr(self.links, "initialize_from_legacy"):
                self.links.initialize_from_legacy()

        try:
            # Fetch and validate the formOf Attribute
            self.content.formOf = Attribute.model_validate(
                self.ctx.legacy_client.get_object(obj_link=self.content.formOf)
            )

            # Initialize Elements in all DisplayForms
            if hasattr(self.content.formOf, "content") and hasattr(
                self.content.formOf.content, "displayForms"
            ):
                for display_form in self.content.formOf.content.displayForms:
                    if hasattr(display_form, "links") and hasattr(
                        display_form.links, "ctx"
                    ):
                        display_form.links.ctx = self.ctx
                        if hasattr(display_form.links, "initialize_from_legacy"):
                            display_form.links.initialize_from_legacy()
        except Exception as e:
            logger.error("Error initializing AttributeDisplayForm: %s", e)


class FilterItemContent(BaseModel):
    model_config = ConfigDict(extra="ignore", arbitrary_types_allowed=True)

    class TimeDefault(BaseModel):
        model_config = ConfigDict(extra="ignore", populate_by_name=True)
        type_: Literal["floating", "absolute", "timeRange"] = Field(alias="type")
        to: int | str | None = None
        from_: int | str | None = Field(alias="from", default=None)

    class ListDefault(BaseModel):
        model_config = ConfigDict(extra="ignore")
        type_: Literal["list"] = Field(alias="type")
        elements: list[str] = Field(default_factory=list)

    id: str
    default: (
        Annotated[TimeDefault | ListDefault, Field(discriminator="type_")]
        | list[str]
        | None
    ) = Field(default_factory=lambda: [])
    obj: str | AttributeDisplayForm
    type: str
    multiple: int | None = 0
    label: str | None = ""
    ctx: Any | None = Field(default=None, exclude=True, repr=False)

    _cloud_filter_id: str | None = None
    unsupported_reason: str | None = Field(default=None, exclude=True)

    @model_validator(mode="before")
    @classmethod
    def unwrap(cls, v):
        # unwrap only if the wrapper key is present; otherwise keep as-is
        if isinstance(v, dict) and "filterItemContent" in v:
            return v["filterItemContent"]
        return v

    @property
    def dataset_id(self):
        if self.ctx and not isinstance(self.obj, str):
            dataset_id = self.obj.meta.identifier.split(".")[0]
            return self.ctx.ldm_mappings.id_mapping.get(dataset_id, "")
        return ""

    def initialize_from_legacy(self):
        """
        Fetch and initialize filter object data from Legacy API.
        This must be called explicitly after setting context.
        """
        if not self.ctx or not isinstance(self.obj, str):
            return

        try:
            # Fetch the filter object from Legacy. For PP dashboards this is typically an
            # attributeDisplayForm, but Legacy can also reference prompts (filtered variables)
            # which are not supported in Cloud dashboards.
            raw_obj: dict[str, Any] = cast(
                dict[str, Any], self.ctx.legacy_client.get_object(obj_link=self.obj)
            )

            if "prompt" in raw_obj:
                self.unsupported_reason = (
                    "Legacy prompt/filtered variable (not supported in Cloud dashboards): "
                    f"filterItemContentId={self.id}, obj={self.obj}"
                )
                return

            self.obj = AttributeDisplayForm.model_validate(raw_obj)

            # Propagate ctx and initialize nested object
            if hasattr(self.obj, "ctx"):
                self.obj.ctx = self.ctx
                if hasattr(self.obj, "initialize_from_legacy"):
                    self.obj.initialize_from_legacy()

            # Set default elements (convert Legacy element URIs to Cloud values)
            if self.type == "list" and isinstance(
                self.default, FilterItemContent.ListDefault
            ):
                element_uris = self.default.elements
                converted_values: list[str] = []
                for uri in element_uris:
                    try:
                        from gooddata_legacy2cloud.metrics.attribute_element import (
                            AttributeElement,
                        )

                        value = AttributeElement(self.ctx, uri).get()
                        if value not in ["--MISSING VALUE--", ""]:
                            converted_values.append(value)
                    except Exception as e:
                        logger.warning(
                            "Could not convert filter element %s: %s", uri, e
                        )

                self.default = converted_values
            elif self.type == "time" and self.default:
                # time filters - no additional processing needed
                pass

            # Map filter to Cloud by ATTRIBUTE identifier (same approach as KPI dashboard migration).
            # This avoids relying on Legacy display-form identifiers which may not exist in mappings.
            if not isinstance(self.obj.content.formOf, str):
                attribute_identifier = self.obj.content.formOf.meta.identifier
                self._cloud_filter_id = self.ctx.ldm_mappings.search_mapping_identifier(
                    attribute_identifier
                )
        except ValidationError as e:
            self.unsupported_reason = f"Could not validate AttributeDisplayForm for filterItemContentId={self.id}: {e}"
            logger.error("Error validating FilterItemContent.obj: %s - %s", self.id, e)
        except Exception as e:
            self.unsupported_reason = (
                f"Error initializing filterItemContentId={self.id}: {e}"
            )
            logger.error(
                "Error initializing FilterItemContent.obj: %s - %s", self.id, e
            )
