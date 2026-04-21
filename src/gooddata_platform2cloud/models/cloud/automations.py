# (C) 2026 GoodData Corporation
"""This module contains the Cloud Automation model based on the GoodData Cloud
API specification

For details and full schema refer to https://www.gooddata.com/docs/cloud/api-and-sdk/api/api_reference_all/#operation/createEntity@Automations
"""

from typing import TYPE_CHECKING, Any, Literal

from pydantic import Field, field_validator

from gooddata_platform2cloud.models.base import Base
from gooddata_platform2cloud.models.cloud.filter_context import Filter
from gooddata_platform2cloud.models.cloud.identifier import IdentifierWrapper
from gooddata_platform2cloud.models.cloud.insight_filters.values import Values
from gooddata_platform2cloud.models.enums import ExportFormat


class VisualExportMetadata(Base):
    title: str | None = None
    filters: list[Filter] | None = None


class Schedule(Base):
    cron: str
    timezone: str


class VisualExportRequestPayloadContent(Base):
    dashboard_id: str
    file_name: str
    metadata: VisualExportMetadata | None = None


class TabularExportMetadata(Base):
    widget: str
    title: str


class RelativeDateFilter(Base):
    data_set: IdentifierWrapper
    granularity: str
    from_: int | None = Field(alias="from", default=None)
    to: int | None = Field(default=None)
    local_identifier: str

    if TYPE_CHECKING:

        def __init__(
            self,
            *,
            local_identifier: str,
            data_set: IdentifierWrapper,
            granularity: str,
            from_: int | None,
            to: int | None,
        ) -> None: ...

    @classmethod
    def from_kwargs(
        cls,
        local_identifier: str,
        data_set: IdentifierWrapper,
        granularity: str,
        from_: int | str | None,
        to: int | str | None,
    ) -> "RelativeDateFilter":
        return cls(
            local_identifier=local_identifier,
            data_set=data_set,
            granularity=granularity,
            from_=int(from_) if from_ else None,  # in case we get something like "-1"
            to=int(to) if to else None,
        )


class AbsoluteDateFilter(Base):
    data_set: IdentifierWrapper
    granularity: str
    from_: str | None = Field(alias="from", default=None)
    to: str | None = Field(default=None)
    local_identifier: str

    if TYPE_CHECKING:

        def __init__(
            self,
            *,
            local_identifier: str,
            data_set: IdentifierWrapper,
            granularity: str,
            from_: str | None,
            to: str | None,
        ) -> None: ...

    @classmethod
    def from_kwargs(
        cls,
        local_identifier: str,
        data_set: IdentifierWrapper,
        granularity: str,
        from_: str | int | None,
        to: str | int | None,
    ) -> "AbsoluteDateFilter":
        return cls(
            local_identifier=local_identifier,
            data_set=data_set,
            granularity=granularity,
            from_=str(from_) if from_ else None,
            to=str(to) if to else None,
        )


class NegativeAttributeFilter(Base):
    local_identifier: str
    display_form: IdentifierWrapper
    not_in: Values

    @classmethod
    def from_kwargs(
        cls,
        local_identifier: str,
        display_form: IdentifierWrapper,
        values: Values,
    ) -> "NegativeAttributeFilter":
        return cls(
            local_identifier=local_identifier,
            display_form=display_form,
            not_in=values,
        )


class PositiveAttributeFilter(Base):
    local_identifier: str
    display_form: IdentifierWrapper
    in_: Values = Field(alias="in")

    if TYPE_CHECKING:

        def __init__(
            self,
            *,
            local_identifier: str,
            display_form: IdentifierWrapper,
            in_: Values,
        ) -> None: ...

    @classmethod
    def from_kwargs(
        cls,
        local_identifier: str,
        display_form: IdentifierWrapper,
        values: Values,
    ) -> "PositiveAttributeFilter":
        return cls(
            local_identifier=local_identifier,
            display_form=display_form,
            in_=values,
        )


class VisualizationObjectCustomFilter(Base):
    """Custom filter - one of IFilter types defined in UI SDK

    Source reference (permalink):
    https://github.com/gooddata/gooddata-ui-sdk/blob/d32c42574c20efc278c438cef67ea4db2a96f6b8/libs/sdk-model/src/execution/filter/index.ts#L342
    """

    absolute_date_filter: AbsoluteDateFilter | None = None
    relative_date_filter: RelativeDateFilter | None = None
    positive_attribute_filter: PositiveAttributeFilter | None = None
    negative_attribute_filter: NegativeAttributeFilter | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "VisualizationObjectCustomFilter":
        return cls(**data)


class TabularExportSettings(Base):
    merge_headers: bool | None = None
    show_filters: bool | None = None


class TabularExportRequestPayloadContent(Base):
    related_dashboard_id: str
    file_name: str
    format: str
    visualization_object: str
    metadata: TabularExportMetadata | None = None
    visualization_object_custom_filters: (
        list[VisualizationObjectCustomFilter] | None
    ) = None
    settings: TabularExportSettings | None = None

    @field_validator("format")
    def validate_format(cls, v: str) -> str:
        if v not in list(ExportFormat):
            raise ValueError("Invalid format")
        return v


class VisualExportRequestPayload(Base):
    request_payload: VisualExportRequestPayloadContent


class TabularExportRequestPayload(Base):
    request_payload: TabularExportRequestPayloadContent

    @classmethod
    def from_kwargs(
        cls,
        cloud_dashboard_id: str,
        file_name: str,
        export_format: str,
        visualization_object_id: str,
        widget_id: str,
        dashboard_title: str | None,
        export_settings: TabularExportSettings | None,
        visualization_object_custom_filters: list[VisualizationObjectCustomFilter],
    ) -> "TabularExportRequestPayload":
        if dashboard_title is not None:
            title = dashboard_title
        else:
            title = ""

        return cls(
            request_payload=TabularExportRequestPayloadContent(
                related_dashboard_id=cloud_dashboard_id,
                file_name=file_name,
                format=export_format.upper(),
                visualization_object=visualization_object_id,
                metadata=TabularExportMetadata(
                    widget=widget_id,
                    title=title,
                ),
                settings=export_settings,
                visualization_object_custom_filters=visualization_object_custom_filters,
            )
        )


class EmailAddress(Base):
    email: str


class EmailDetails(Base):
    message: str
    subject: str


class VisibleFilter(Base):
    is_all_time_date_filter: bool = False
    local_identifier: str
    title: str


class AttributesMetadata(Base):
    visible_filters: list[VisibleFilter] | None = None
    widget: str | None = None


class Attributes(Base):
    description: str = ""
    details: EmailDetails
    external_recipients: list[EmailAddress] | None = None
    schedule: Schedule
    # Metadata should be an empty object for automation using default filters
    metadata: AttributesMetadata | dict = Field(default_factory=lambda: {})
    visual_exports: list[VisualExportRequestPayload] | None = None
    tabular_exports: list[TabularExportRequestPayload] | None = None
    title: str


class AnalyticalDashboard(Base):
    id: str
    type: Literal["analyticalDashboard"] = "analyticalDashboard"


class AnalyticalDashboardsWrapper(Base):
    data: AnalyticalDashboard


class ExportDefinition(Base):
    id: str
    type: Literal["exportDefinition"] = "exportDefinition"


class ExportDefinitionsWrapper(Base):
    data: list[ExportDefinition]


class NotificationChannel(Base):
    id: str
    type: Literal["notificationChannel"] = "notificationChannel"


class NotificationChannelWrapper(Base):
    data: NotificationChannel


class Recipient(Base):
    """Cloud recipient.

    Attributes:
        id: GoodData Cloud user id
        type: Type of recipient, always "user"
    """

    id: str
    type: Literal["user"] = "user"


class RecipientsWrapper(Base):
    """Wrapper for recipients"""

    data: list[Recipient]


class Relationships(Base):
    """Automation relationships"""

    analytical_dashboard: AnalyticalDashboardsWrapper | None = None
    export_definition: ExportDefinitionsWrapper | None = None
    notification_channel: NotificationChannelWrapper | None = None
    recipients: RecipientsWrapper = Field(
        default_factory=lambda: RecipientsWrapper(data=[])
    )


class CloudAutomation(Base):
    """Cloud Automation model based on the GoodData Cloud Entity API specification.

    For details and full schema refer to [API reference](https://www.gooddata.com/docs/cloud/api-and-sdk/api/api_reference_all/#operation/createEntity@Automations)
    """

    attributes: Attributes
    id: str
    relationships: Relationships
    type: Literal["automation"] = "automation"

    @classmethod
    def from_kwargs(
        cls,
        external_recipients: list[EmailAddress],
        visible_filters: list[VisibleFilter],
        cron_expression: str,
        visual_exports: list[VisualExportRequestPayload],
        tabular_exports: list[TabularExportRequestPayload],
        notification_channel_id: str,
        cloud_dashboard_id: str,
        internal_recipients: RecipientsWrapper,
        cloud_email_id: str,
        title: str,
        message: str,
        subject: str,
        timezone: str,
    ) -> "CloudAutomation":
        automation_object = cls(
            attributes=Attributes(
                external_recipients=external_recipients
                if external_recipients
                else None,
                metadata=AttributesMetadata(visible_filters=visible_filters),
                schedule=Schedule(
                    cron=cron_expression,
                    timezone=timezone,
                ),
                visual_exports=visual_exports if visual_exports else None,
                tabular_exports=tabular_exports if tabular_exports else None,
                title=title,
                details=EmailDetails(
                    message=message,
                    subject=subject,
                ),
            ),
            id=cloud_email_id,
            relationships=Relationships(
                analytical_dashboard=AnalyticalDashboardsWrapper(
                    data=AnalyticalDashboard(id=cloud_dashboard_id)
                ),
                notification_channel=NotificationChannelWrapper(
                    data=NotificationChannel(id=notification_channel_id)
                ),
                recipients=internal_recipients,
            ),
        )

        return automation_object
