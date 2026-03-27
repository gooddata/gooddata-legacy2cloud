# (C) 2026 GoodData Corporation
import logging

from gooddata_platform2cloud.dashboards.data_classes import DashboardContext
from gooddata_platform2cloud.dashboards.filter_context import FilterContext
from gooddata_platform2cloud.helpers import validate_non_null_string
from gooddata_platform2cloud.models.aliases import (
    AttributeFilterModelType,
    CloudInsightId,
    DateFilterModelType,
    FilterInstance,
)
from gooddata_platform2cloud.models.cloud.automations import (
    AbsoluteDateFilter,
    NegativeAttributeFilter,
    PositiveAttributeFilter,
    RelativeDateFilter,
    TabularExportRequestPayload,
    TabularExportSettings,
    VisualExportMetadata,
    VisualExportRequestPayload,
    VisualExportRequestPayloadContent,
    VisualizationObjectCustomFilter,
)
from gooddata_platform2cloud.models.cloud.dashboard import CloudDashboard, Widget
from gooddata_platform2cloud.models.cloud.filter_context import (
    Filter,
    FilterContextModel,
)
from gooddata_platform2cloud.models.enums import (
    AttributeFilterTypeName,
    DateFilterType,
    DateFilterTypeName,
    ExportFormat,
)
from gooddata_platform2cloud.models.platform.scheduled_exports import (
    Attachment,
    KpiDashboardAttachment,
    ScheduledMail,
    VisualizationWidgetAttachment,
)
from gooddata_platform2cloud.models.platform.visualization_objects import (
    VisualizationObjectWrapper,
)
from gooddata_platform2cloud.models.platform.visualization_widget import (
    VisualizationWidgetWrapper,
)
from gooddata_platform2cloud.scheduled_exports.helpers.constants import (
    ALL_TIME_DATE_FILTER,
)
from gooddata_platform2cloud.scheduled_exports.helpers.mappers import CloudMaps, Exports
from gooddata_platform2cloud.scheduled_exports.scheduled_export_context import (
    ScheduledExportsContext,
)

logger = logging.getLogger("migration")


class AttachmentProcessor:
    """Process Platform scheduled email attachments and convert them to Cloud exports."""

    def __init__(
        self,
        context: ScheduledExportsContext,
        cloud_maps: CloudMaps,
    ):
        self.context = context
        self.cloud_maps: CloudMaps = cloud_maps

        # A set to gather local identifiers of applied filters
        self.applied_filters: set[str] = set()

    def _get_filter_context_translator(self, filter_context_uri) -> FilterContext:
        """Returns a FilterContext object for the given filter context URI.

        Reuses the FilterContext tool from dashboard migration.
        """
        filter_context_translator_context = DashboardContext(
            platform_client=self.context.backends.platform_client,
            cloud_client=self.context.backends.cloud_client,
            ldm_mappings=self.context.mappings.ldm_mappings,
            metric_mappings=self.context.mappings.metric_mappings,
            insight_mappings=self.context.mappings.insight_mappings,
            mapping_logger=self.context.logging.mapping_logger,
            client_prefix=self.context.command_line_arguments.client_prefix,
        )

        return FilterContext(
            ctx=filter_context_translator_context,
            filter_context_uri=filter_context_uri,
        )

    def _get_platform_visualization_object(
        self, attachment_uri: str
    ) -> VisualizationObjectWrapper:
        """Get the Platform visualization object from the attachment URI."""
        raw_platform_widget_object = self.context.backends.platform_client.get_object(
            attachment_uri
        )
        platform_widget_object = VisualizationWidgetWrapper(
            **raw_platform_widget_object
        )
        raw_platform_visualization_object = (
            self.context.backends.platform_client.get_object(
                platform_widget_object.visualization_widget.content.visualization
            )
        )
        platform_visualization_object = VisualizationObjectWrapper(
            **raw_platform_visualization_object
        )
        return platform_visualization_object

    def _get_cloud_insight_id(self, platform_visualization_id: str) -> CloudInsightId:
        """Get the Cloud insight ID from the Platform visualization ID."""
        raw_cloud_visualization_id = (
            self.context.mappings.insight_mappings.get_value_by_key(
                platform_visualization_id
            )
        )

        cloud_insight_id = validate_non_null_string(
            raw_cloud_visualization_id, "Visualization ID"
        )

        return cloud_insight_id

    def _process_visualization_widget_attachment(
        self,
        cloud_dashboard: CloudDashboard,
        attachment: VisualizationWidgetAttachment,
    ) -> list[TabularExportRequestPayload]:
        """Turns the attachment into a tabular export object."""
        export_settings: TabularExportSettings | None = None
        attachment_formats = attachment.formats
        exports: list[TabularExportRequestPayload] = []

        platform_visualization_object = self._get_platform_visualization_object(
            attachment.uri
        )

        cloud_insight_id = self._get_cloud_insight_id(
            platform_visualization_object.visualization_object.meta.identifier
        )

        if attachment.export_options:
            export_settings = TabularExportSettings(
                merge_headers=bool(attachment.export_options.merge_headers),
                show_filters=bool(attachment.export_options.include_filter_context),
            )

        # Each attachment can have multiple formats (e.g. CSV and XLSX)
        for raw_format in attachment_formats:
            # Validate that the attachment format is supported
            if ExportFormat(raw_format.upper()) not in (
                ExportFormat.CSV,
                ExportFormat.XLSX,
            ):
                raise ValueError(
                    f"Unsupported format: {raw_format} for visualization widget attachment."
                )

            # We need to point the export to a specific Cloud widget
            cloud_widget_id = self.cloud_maps.get_widget_id(
                cloud_dashboard.id, cloud_insight_id
            )

            # Get the Cloud widget object
            cloud_widget = self.cloud_maps.get_widget_by_id(cloud_widget_id)

            # If Platform attachment has a filter context, we need to translate it to
            # Cloud and populate visualization_object_custom_filters
            visualization_object_custom_filters: list[
                VisualizationObjectCustomFilter
            ] = []
            if attachment.filter_context:
                filter_context_translator = self._get_filter_context_translator(
                    attachment.filter_context
                )

                filter_context_object: FilterContextModel = (
                    filter_context_translator.get_object()
                )
                visualization_object_custom_filters = (
                    self._get_visualization_object_custom_filters(
                        filter_context_object.attributes.content.filters, cloud_widget
                    )
                )
            else:
                visualization_object_custom_filters = []

            # Create the tabular export object
            request_payload = TabularExportRequestPayload.from_kwargs(
                cloud_dashboard_id=cloud_dashboard.id,
                file_name=platform_visualization_object.visualization_object.meta.title,
                export_format=raw_format.upper(),
                visualization_object_id=cloud_insight_id,
                widget_id=cloud_widget_id,
                dashboard_title=cloud_dashboard.attributes.title,
                export_settings=export_settings,
                visualization_object_custom_filters=visualization_object_custom_filters,
            )
            exports.append(request_payload)

        return exports

    def _process_kpi_dashboard_attachment(
        self,
        cloud_dashboard: CloudDashboard,
        platform_email: ScheduledMail,
        attachment: KpiDashboardAttachment,
    ) -> VisualExportRequestPayload:
        """Turns the attachment into a visual export object."""
        visual_export_filters: list[Filter] = []

        # If Platform attachment has a filter context, we need to translate it to Cloud
        if attachment.filter_context:
            filter_context_translator = self._get_filter_context_translator(
                attachment.filter_context
            )

            filter_context_object: FilterContextModel = (
                filter_context_translator.get_object()
            )

            # Filter metadata is applied to PDF exports only. Tabular exports have visualization_object_custom_filters
            for filter_ in filter_context_object.attributes.content.filters:
                if (
                    filter_.attribute_filter
                    and not filter_.attribute_filter.attribute_elements.uris
                ):
                    continue
                else:
                    # NOTE: This would be a natural place to get rid of the absolute
                    # date filter artifact which is created by Platform backend when
                    # an export has custom attribute filters applied. However, we
                    # need to keep them for later to determine if the default date
                    # filter should be applied when creating VisibleFilters.
                    visual_export_filters.append(filter_)

                    if filter_.attribute_filter:
                        self.applied_filters.add(
                            filter_.attribute_filter.display_form.identifier.id_
                        )
                    if filter_.date_filter:
                        if filter_.date_filter.from_ and filter_.date_filter.to:
                            self.applied_filters.add(
                                filter_.date_filter.local_identifier
                            )
                        else:
                            self.applied_filters.add(ALL_TIME_DATE_FILTER)
        # Validate the format
        attachment_format = ExportFormat(attachment.format.upper())
        if attachment_format != ExportFormat.PDF:
            raise ValueError(
                f"Unsupported format: {attachment.format} for KPI dashboard attachment."
            )

        export = VisualExportRequestPayload(
            request_payload=VisualExportRequestPayloadContent(
                dashboard_id=cloud_dashboard.id,
                file_name=platform_email.content.subject,
                metadata=VisualExportMetadata(
                    filters=visual_export_filters if visual_export_filters else None,
                    title=cloud_dashboard.attributes.title,
                ),
            )
        )

        return export

    def _get_visualization_object_custom_filters(
        self,
        filters: list[Filter],
        cloud_widget: Widget,
    ) -> list[VisualizationObjectCustomFilter]:
        """Create custom filter object for tabular export.

        For tabular exports, the custom filters metadata is stored in visualizationObjectCustomFilters
        object when posting the automation metadata to the backend, as opposed to
        filters applied to visual (PDF) exports.
        """

        # Iterate through the filters and create the custom filter objects
        visualization_object_custom_filters: list[VisualizationObjectCustomFilter] = []

        for filter_ in filters:
            AttributeFilterModel: AttributeFilterModelType
            DateFilterModel: DateFilterModelType
            filter_type_name: str
            filter_dict: dict[str, FilterInstance] = {}

            if filter_.attribute_filter:
                # Do not apply the filter if it does not store values
                if not filter_.attribute_filter.attribute_elements.uris:
                    continue

                # Attribute filters can be positive or negative
                if filter_.attribute_filter.negative_selection:
                    AttributeFilterModel = NegativeAttributeFilter
                    filter_type_name = AttributeFilterTypeName.NEGATIVE
                else:
                    AttributeFilterModel = PositiveAttributeFilter
                    filter_type_name = AttributeFilterTypeName.POSITIVE

                filter_dict[filter_type_name] = AttributeFilterModel.from_kwargs(
                    local_identifier=filter_.attribute_filter.local_identifier,
                    display_form=filter_.attribute_filter.display_form,
                    uris=filter_.attribute_filter.attribute_elements,
                )

                self.applied_filters.add(
                    filter_.attribute_filter.display_form.identifier.id_
                )

            elif filter_.date_filter:
                if filter_.date_filter.type_ == DateFilterType.RELATIVE:
                    DateFilterModel = RelativeDateFilter
                    filter_type_name = DateFilterTypeName.RELATIVE

                elif filter_.date_filter.type_ == DateFilterType.ABSOLUTE:
                    DateFilterModel = AbsoluteDateFilter
                    filter_type_name = DateFilterTypeName.ABSOLUTE

                else:
                    raise ValueError(
                        f"Date filter type {filter_.date_filter.type_} not recognized."
                    )

                if not cloud_widget.date_data_set:
                    continue

                filter_dict[filter_type_name] = DateFilterModel.from_kwargs(
                    data_set=cloud_widget.date_data_set,
                    granularity=filter_.date_filter.granularity,
                    from_=filter_.date_filter.from_,
                    to=filter_.date_filter.to,
                    local_identifier=filter_.date_filter.local_identifier,
                )

                if filter_.date_filter.from_ and filter_.date_filter.to:
                    self.applied_filters.add(filter_.date_filter.local_identifier)
                else:
                    self.applied_filters.add(ALL_TIME_DATE_FILTER)

            custom_filter = VisualizationObjectCustomFilter.from_dict(filter_dict)

            # append to visualization_object_custom_filters
            visualization_object_custom_filters.append(custom_filter)

        if visualization_object_custom_filters:
            return visualization_object_custom_filters
        else:
            return []

    def process_attachments(
        self, platform_email: ScheduledMail, cloud_dashboard_id: str
    ) -> Exports:
        """Process the attachments from the email"""
        exports = Exports()

        # Reset the applied filters
        self.applied_filters.clear()

        # Get the Cloud dashboard object
        cloud_dashboard = self.cloud_maps.get_dashboard_by_id(cloud_dashboard_id)

        # Get the attachments from the email
        attachments: list[Attachment] = platform_email.content.attachments

        for attachment in attachments:
            if attachment.visualization_widget_attachment:
                tabular_attachments = self._process_visualization_widget_attachment(
                    cloud_dashboard,
                    attachment.visualization_widget_attachment,
                )
                exports.tabular_exports.extend(tabular_attachments)
            elif attachment.kpi_dashboard_attachment:
                visual_attachment = self._process_kpi_dashboard_attachment(
                    cloud_dashboard, platform_email, attachment.kpi_dashboard_attachment
                )
                exports.visual_exports.append(visual_attachment)

        return exports
