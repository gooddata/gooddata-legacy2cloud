# (C) 2026 GoodData Corporation
from gooddata_platform2cloud.models.base import Base
from gooddata_platform2cloud.models.platform.meta import Meta


class KpiDashboardAttachment(Base):
    format: str
    uri: str
    filter_context: str | None = None


class ExportOptions(Base):
    include_filter_context: str
    merge_headers: str


class VisualizationWidgetAttachment(Base):
    formats: list[str]
    uri: str
    export_options: ExportOptions | None = None
    filter_context: str | None = None
    dashboard_uri: str


class When(Base):
    recurrency: str
    timeZone: str
    startDate: str


class Attachment(Base):
    kpi_dashboard_attachment: KpiDashboardAttachment | None = None
    visualization_widget_attachment: VisualizationWidgetAttachment | None = None


class Content(Base):
    attachments: list[Attachment]
    body: str
    when: When
    last_successfull: str | None = None
    subject: str
    to: list[str]
    bcc: list[str] | None = None


class ScheduledMail(Base):
    """Model based on Platform Scheduled Email API reference.

    Consult docs at [API reference](https://help.gooddata.com/doc/enterprise/en/expand-your-gooddata-platform/api-reference/#operation/createMetadataObject).
    Follow the link and select the ScheduledEmail pill.
    """

    content: Content
    meta: Meta
