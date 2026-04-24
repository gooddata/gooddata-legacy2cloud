# (C) 2026 GoodData Corporation
import uuid
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

from gooddata_legacy2cloud.helpers import PP_INSIGHT_PREFIX
from gooddata_legacy2cloud.pp_dashboards.utils import get_migration_id


class Identifier(BaseModel):
    id: str
    type: Literal["metric"] = "metric"


class Item(BaseModel):
    identifier: Identifier


class MeasureDefinition(BaseModel):
    item: Item
    filters: list[dict] = Field(default_factory=list)


class Definition(BaseModel):
    measureDefinition: MeasureDefinition


class Measure(BaseModel):
    definition: Definition
    title: Optional[str] = ""
    localIdentifier: str = uuid.uuid4().hex


class MeasureItem(BaseModel):
    measure: Measure


class BucketItem(BaseModel):
    items: list[MeasureItem]
    localIdentifier: str = "measures"


class Content(BaseModel):
    buckets: list[BucketItem]
    filters: list[dict] = Field(default_factory=list)
    sorts: list[dict] = Field(default_factory=list)
    properties: Optional[dict] = Field(default_factory=dict)
    visualizationUrl: str = "local:headline"
    version: str = "2"


class Attributes(BaseModel):
    title: str
    content: Content
    description: str = ""
    tags: list[dict] = Field(default_factory=list)


class VisualizationObject(BaseModel):
    attributes: Attributes
    type: str = "visualizationObject"


class VisualisationMaker(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    ctx: Optional[Any] = Field(default=None, exclude=True, repr=False)

    def create_visualization(
        self,
        cloud_measure_identifier: str,
        legacy_measure_identifier: str = "",
        title: str = "",
        id_prefix: str = PP_INSIGHT_PREFIX,
    ) -> str:
        """Create a visualization object for a headline metric.

        Args:
            cloud_measure_identifier: Cloud metric ID
            legacy_measure_identifier: Legacy metric identifier
            title: Display title for the visualization
            id_prefix: Prefix for the visualization ID (default: ppkpinsight)

        Returns:
            The created visualization ID
        """
        # Create Cloud visualization object definition
        measure_obj = Measure(
            definition=Definition(
                measureDefinition=MeasureDefinition(
                    item=Item(identifier=Identifier(id=cloud_measure_identifier))
                )
            ),
            title=title,
        )
        visualization_object = VisualizationObject(
            attributes=Attributes(
                title=get_migration_id(
                    prefix=id_prefix,
                    legacy_title=title,
                    legacy_identifier=legacy_measure_identifier,
                ),
                content=Content(
                    buckets=[BucketItem(items=[MeasureItem(measure=measure_obj)])]
                ),
            )
        )
        if not self.ctx:
            raise Exception("Context not set for VisualisationMaker")

        r = self.ctx.cloud_client._post(
            f"api/v1/entities/workspaces/{self.ctx.cloud_client.ws}/visualizationObjects",
            data={"data": visualization_object.model_dump()},
        )
        if result := r.json().get("data", {}).get("id", None):
            return result
        else:
            raise Exception(r.json().get("detail"))
