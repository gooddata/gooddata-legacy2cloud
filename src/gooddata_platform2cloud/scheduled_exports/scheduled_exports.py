# (C) 2026 GoodData Corporation
"""This module is used for migrating scheduled emails. It includes functionality for
loading environment variables, setting up command line arguments, and running
the main migration process.
"""

import csv
import json
import logging
from typing import Any

from gooddata_platform2cloud.helpers import get_cloud_id, validate_non_null_string
from gooddata_platform2cloud.models.aliases import (
    AttributeFilterModelType,
    CloudDashboardId,
    DateFilterModelType,
)
from gooddata_platform2cloud.models.cloud.attribute import AttributeWrapper
from gooddata_platform2cloud.models.cloud.automations import (
    AbsoluteDateFilter,
    CloudAutomation,
    EmailAddress,
    NegativeAttributeFilter,
    PositiveAttributeFilter,
    Recipient,
    RecipientsWrapper,
    RelativeDateFilter,
    VisibleFilter,
    VisualizationObjectCustomFilter,
)
from gooddata_platform2cloud.models.cloud.filter_context import Filter
from gooddata_platform2cloud.models.enums import (
    Action,
    AttributeFilterTypeName,
    DateFilterType,
    DateFilterTypeName,
)
from gooddata_platform2cloud.models.exceptions import ExpectedSkip
from gooddata_platform2cloud.models.platform.accounts_settings import (
    AccountSettingsWrapper,
)
from gooddata_platform2cloud.models.platform.analytical_dashboards import (
    AnalyticalDashboardWrapper,
)
from gooddata_platform2cloud.models.platform.scheduled_exports import (
    Attachment,
    ScheduledMail,
)
from gooddata_platform2cloud.scheduled_exports.attachment_processor import (
    AttachmentProcessor,
)
from gooddata_platform2cloud.scheduled_exports.helpers.constants import (
    ALL_TIME_DATE_FILTER,
)
from gooddata_platform2cloud.scheduled_exports.helpers.mappers import (
    CloudMaps,
    Exports,
    Mappers,
)
from gooddata_platform2cloud.scheduled_exports.recur_to_cron.translator import (
    RecurToCronTranslator,
)
from gooddata_platform2cloud.scheduled_exports.scheduled_export_context import (
    ScheduledExportsContext,
)

logger = logging.getLogger("migration")


class ScheduledExportMigrator:
    """Migrate scheduled exports from Platform to Cloud

    This class is responsible for migrating scheduled emails from Platform to Cloud.
    It is initiated with a context object containing the necessary secrets and
    configurations.
    """

    def __init__(self, context: ScheduledExportsContext):
        """Initialize the ScheduledExportMigrator

        Args:
            context: ScheduledExportsContext object containing the necessary secrets and configurations

        Raises:
            ValueError: If notification channel id is not set
        """
        self.context: ScheduledExportsContext = context

    @staticmethod
    def get_external_recipients(platform_email: ScheduledMail) -> list[EmailAddress]:
        """Get external recipients from BCC field of platform email."""
        if not platform_email.content.bcc:
            return []

        external_recipients: list[EmailAddress] = []
        for email in platform_email.content.bcc:
            external_recipients.append(EmailAddress(email=email))

        return external_recipients

    def _get_cloud_dashboard_id(
        self,
        platform_email: ScheduledMail,
        platform_dashboards: list[AnalyticalDashboardWrapper],
    ) -> CloudDashboardId:
        """Get the Cloud dashboard id for a given Platform scheduled email.

        Finds Cloud dashboard id by first matching the Platform scheduled export with
        Platform dashboard based on the first email attachment, and then looking up
        the Cloud dashboard id in the mapping file.

        Raises:
            ValueError: If the attachment type is invalid
        """

        platform_dashboard_id: str
        platform_dashboard_uri: str

        # Extract first attachment - all attachments are on the same dashboard.
        first_attachment = platform_email.content.attachments[0]

        # Different visualization types store dashboard URI in different fields.
        if first_attachment.kpi_dashboard_attachment:
            platform_dashboard_uri = first_attachment.kpi_dashboard_attachment.uri
        elif first_attachment.visualization_widget_attachment:
            platform_dashboard_uri = (
                first_attachment.visualization_widget_attachment.dashboard_uri
            )
        else:
            raise ValueError("Invalid attachment type")

        # Find Platform dashboard ID by matching the URI.
        for dashboard in platform_dashboards:
            if dashboard.analytical_dashboard.meta.uri == platform_dashboard_uri:
                platform_dashboard_id = dashboard.analytical_dashboard.meta.identifier
                break

        # Look up Cloud dashboard ID in the mapping file.
        cloud_dashboard_id = self.context.mappings.dashboard_mappings.get_value_by_key(
            platform_dashboard_id
        )

        # Validate the ID is not null before returning.
        return validate_non_null_string(cloud_dashboard_id, "Dashboard ID")

    def _get_recipients(
        self,
        platform_email: ScheduledMail,
        cloud_maps: CloudMaps,
    ) -> RecipientsWrapper:
        """Get recipients from platform email.

        Creates Recipient objects by looking up user email adresses in the list
        of Cloud users.
        """
        recipients: list[Recipient] = []
        for email_address in platform_email.content.to:
            try:
                cloud_user = cloud_maps.get_user_by_email(email_address)
                recipients.append(Recipient(id=cloud_user.id))
            except KeyError:
                logger.warning(
                    f"User {email_address} not found in cloud."
                    f"User will be skipped in email {platform_email.meta.identifier}."
                )
                self.context.logging.output_logger.write_transformation(
                    f"{platform_email.meta.identifier} - {platform_email.meta.title}",
                    f"Skipping recipient {email_address}",
                    f"User {email_address} not found in Cloud.",
                )

        return RecipientsWrapper(data=recipients)

    def _get_visible_filters(
        self,
        cloud_dashboard_id: str,
        has_platform_filter_context: bool,
        applied_filters: set[str],
        cloud_maps: CloudMaps,
    ) -> list[VisibleFilter]:
        """Get visible filters from cloud dashboard.

        Returns:
            list[VisibleFilter] A list of objects representing the pills with
            filters and selected values in the email scheduling dialog in the UI.
            It contains a reference to the Cloud attribute ID and the local
            identifier of the context filter.
        """
        use_all_time_date_filter: bool
        visible_filters: list[VisibleFilter] = []
        cloud_has_date_filter: bool = False

        # Find Cloud filter context for the dashboard
        dashboard_filter_context = cloud_maps.get_dashboard_filter_context(
            cloud_dashboard_id
        )

        # Get the filters from the filter context
        cloud_filters: list[Filter] = (
            dashboard_filter_context.attributes.content.filters
        )

        # Iterate through the filters and create the visible filters objects
        if has_platform_filter_context:
            # If the email has Platform filter context, it means filters other than
            # dashboard default are applied.
            for filter_ in cloud_filters:
                if (
                    filter_.attribute_filter
                    and filter_.attribute_filter.display_form.identifier.id_
                    in applied_filters
                ):
                    visible_filters.append(self._get_visible_filter(filter_))
        else:
            # If there is no Platform filter context, we need to apply dashboard default
            # filters that have values selected.
            for filter_ in cloud_filters:
                if filter_.attribute_filter:
                    if filter_.attribute_filter.attribute_elements.values:
                        visible_filters.append(self._get_visible_filter(filter_))
                if filter_.date_filter:
                    cloud_has_date_filter = True

        if applied_filters:
            use_all_time_date_filter = ALL_TIME_DATE_FILTER in applied_filters
        else:
            use_all_time_date_filter = not cloud_has_date_filter

        # Add the default date range to the beggining of the list (so it displays
        # first in the UI).
        visible_filters.insert(
            0,
            VisibleFilter(
                local_identifier="0_dateFilter",
                title="Date range",
                is_all_time_date_filter=use_all_time_date_filter,
            ),
        )

        return visible_filters

    def _get_visible_filter(self, filter_: Filter) -> VisibleFilter:
        """Constructs a VisibleFilter object from a Filter."""
        if filter_.attribute_filter:
            # Get attribute id from the filter context
            attribute_id: str = filter_.attribute_filter.display_form.identifier.id_

            # Use attribute id to fetch it and find its name
            attribute_wrapper: AttributeWrapper = (
                self.context.backends.cloud_client.get_attribute_object(attribute_id)
            )
            attribute_name: str = attribute_wrapper.data.attributes.title

            # Create a visible filter object
            return VisibleFilter(
                local_identifier=filter_.attribute_filter.local_identifier,
                title=attribute_name,
            )
        else:
            raise NotImplementedError(
                f"Filter {filter_.model_dump_json(by_alias=True)} is not supported."
            )

    @staticmethod
    def _filter_export_filters(exports: Exports) -> Exports:
        """Filter invalid date filters from the export metadata.

        Go through exports and remove any date filters where from and to are not
        set. This aims to remove the absolute date filter artifact created when
        attribute filters are applied on top of the all time date filter.
        """
        for visual_export in exports.visual_exports:
            if (
                visual_export.request_payload.metadata
                and visual_export.request_payload.metadata.filters
            ):
                for filter_ in visual_export.request_payload.metadata.filters:
                    if filter_.date_filter:
                        if not filter_.date_filter.from_ and not filter_.date_filter.to:
                            visual_export.request_payload.metadata.filters.remove(
                                filter_
                            )
        for tabular_export in exports.tabular_exports:
            if tabular_export.request_payload.visualization_object_custom_filters:
                for (
                    custom_filter
                ) in tabular_export.request_payload.visualization_object_custom_filters:
                    if custom_filter.absolute_date_filter:
                        if (
                            not custom_filter.absolute_date_filter.from_
                            and not custom_filter.absolute_date_filter.to
                        ):
                            tabular_export.request_payload.visualization_object_custom_filters.remove(
                                custom_filter
                            )
        return exports

    @staticmethod
    def _does_any_attachment_have_filter_context(attachments: list[Attachment]) -> bool:
        """Iterate through attachments and determine if any have a custom filter context."""
        for attachment in attachments:
            if attachment.kpi_dashboard_attachment:
                if attachment.kpi_dashboard_attachment.filter_context:
                    return True
            elif attachment.visualization_widget_attachment:
                if attachment.visualization_widget_attachment.filter_context:
                    return True
        return False

    def _add_dashboard_default_filters(
        self,
        cloud_dashboard_id: str,
        cloud_maps: CloudMaps,
        applied_filters: set[str],
        exports: Exports,
    ) -> Exports:
        """Add dashboard default filters to the exports.

        Add any default filter values from the Cloud filter context which are
        not applied yet (Platform filter context does not contain dashboard default
        filters, but Cloud automations require that even these are explicitly
        included).
        """
        DateFilterModel: DateFilterModelType
        AttributeFilterModel: AttributeFilterModelType
        filter_type_name: str
        # Find Cloud filter context for the dashboard
        dashboard_filter_context = cloud_maps.get_dashboard_filter_context(
            cloud_dashboard_id
        )

        # Get the filters from the filter context
        cloud_filters: list[Filter] = (
            dashboard_filter_context.attributes.content.filters
        )
        # Iterate through the filters and add them to the exports if they are not already applied
        for filter_ in cloud_filters:
            if filter_.attribute_filter:
                # If the filter has no selected values, skip it
                if not filter_.attribute_filter.attribute_elements.values:
                    continue

                if (
                    filter_.attribute_filter.display_form.identifier.id_
                    in applied_filters
                ):
                    continue

                if exports.visual_exports:
                    for visual_export in exports.visual_exports:
                        if visual_export.request_payload.metadata:
                            if not visual_export.request_payload.metadata.filters:
                                visual_export.request_payload.metadata.filters = []
                            visual_export.request_payload.metadata.filters.append(
                                filter_
                            )

                if exports.tabular_exports:
                    for tabular_export in exports.tabular_exports:
                        if not tabular_export.request_payload.visualization_object_custom_filters:
                            tabular_export.request_payload.visualization_object_custom_filters = []
                        if filter_.attribute_filter.negative_selection:
                            AttributeFilterModel = NegativeAttributeFilter
                            filter_type_name = AttributeFilterTypeName.NEGATIVE
                        else:
                            AttributeFilterModel = PositiveAttributeFilter
                            filter_type_name = AttributeFilterTypeName.POSITIVE

                        tabular_export.request_payload.visualization_object_custom_filters.append(
                            VisualizationObjectCustomFilter.from_dict(
                                {
                                    filter_type_name: AttributeFilterModel.from_kwargs(
                                        local_identifier=filter_.attribute_filter.local_identifier,
                                        display_form=filter_.attribute_filter.display_form,
                                        values=filter_.attribute_filter.attribute_elements,
                                    )
                                }
                            )
                        )

            if filter_.date_filter:
                # If the filter is already applied, skip it
                if filter_.date_filter.local_identifier in applied_filters:
                    continue

                # Check that the date filter is valid (has from and to attributes)
                if not (filter_.date_filter.from_ and filter_.date_filter.to):
                    continue

                # Visual exports (PDFs) can accept the filter as is
                if exports.visual_exports:
                    for visual_export in exports.visual_exports:
                        if visual_export.request_payload.metadata:
                            if not visual_export.request_payload.metadata.filters:
                                visual_export.request_payload.metadata.filters = []
                            visual_export.request_payload.metadata.filters.append(
                                filter_
                            )

                if exports.tabular_exports:
                    for tabular_export in exports.tabular_exports:
                        date_dataset = cloud_maps.get_widget_date_dataset(
                            cloud_dashboard_id,
                            tabular_export.request_payload.visualization_object,
                        )

                        if not tabular_export.request_payload.visualization_object_custom_filters:
                            tabular_export.request_payload.visualization_object_custom_filters = []

                        if filter_.date_filter.type_ == DateFilterType.ABSOLUTE:
                            DateFilterModel = AbsoluteDateFilter
                            filter_type_name = DateFilterTypeName.ABSOLUTE

                        elif filter_.date_filter.type_ == DateFilterType.RELATIVE:
                            DateFilterModel = RelativeDateFilter
                            filter_type_name = DateFilterTypeName.RELATIVE

                        else:
                            raise ValueError(
                                f"Date filter type {filter_.date_filter.type_} not recognized."
                            )

                        tabular_export.request_payload.visualization_object_custom_filters.append(
                            VisualizationObjectCustomFilter.from_dict(
                                {
                                    filter_type_name: DateFilterModel.from_kwargs(
                                        local_identifier=filter_.date_filter.local_identifier,
                                        data_set=date_dataset,
                                        granularity=filter_.date_filter.granularity,
                                        from_=filter_.date_filter.from_,
                                        to=filter_.date_filter.to,
                                    )
                                }
                            )
                        )

        return exports

    def _transform_platform_scheduled_email(
        self,
        platform_email: ScheduledMail,
        platform_dashboards: list[AnalyticalDashboardWrapper],
        cloud_maps: CloudMaps,
    ) -> CloudAutomation:
        """Transform platform scheduled email to cloud scheduled email.

        Gather all the necessary data and call transformations to create a Cloud
        automation metadata object.
        """

        # Get Cloud dashboard ID
        cloud_dashboard_id: str = self._get_cloud_dashboard_id(
            platform_email, platform_dashboards
        )

        # Get external recipients (The bussines logic assumption is that users in
        # Platform BCC are meant to be external.) These are just a list of emails.
        external_recipients: list[EmailAddress] = self.get_external_recipients(
            platform_email
        )

        # Get internal recipients - list of Recipent objects linked to Cloud user IDs
        internal_recipients: RecipientsWrapper = self._get_recipients(
            platform_email, cloud_maps
        )

        exports: Exports = self.attachment_processor.process_attachments(
            platform_email, cloud_dashboard_id
        )

        # Convert Platform recurrence to Cloud cron expression
        cron_expression: str = self.recur_translator.convert_date_manip_to_cron(
            platform_email.content.when.recurrency
        )

        # Create an ID for the automation based on the convention
        cloud_email_id: str = get_cloud_id(
            platform_email.meta.title, platform_email.meta.identifier
        )

        # Add mapping to the mapping file
        self.context.logging.mapping_logger.write_identifier_relation(
            platform_email.meta.identifier, cloud_email_id
        )

        # Find out whether any of the attachments has a stored filter context.
        has_platform_filter_context: bool = (
            self._does_any_attachment_have_filter_context(
                platform_email.content.attachments
            )
        )

        # Prepare visible filters for the AttributesMetadata object.
        # If the email has no filter context, add the default date filter.
        visible_filters = self._get_visible_filters(
            cloud_dashboard_id,
            has_platform_filter_context,
            self.attachment_processor.applied_filters,
            cloud_maps,
        )

        # Filter out invalid date filters from the export metadata.
        exports = self._filter_export_filters(exports)

        # Add any default filter values from the Cloud filter context
        # which are not applied yet (Platform filter context does not contain dashboard
        # default filters, but Cloud automations require that even these are
        # explicitly included).
        exports = self._add_dashboard_default_filters(
            cloud_dashboard_id,
            cloud_maps,
            self.attachment_processor.applied_filters,
            exports,
        )

        title = platform_email.meta.title
        timezone = platform_email.content.when.timeZone
        message = platform_email.content.body
        subject = platform_email.content.subject

        # Create the Cloud automation object
        cloud_email = CloudAutomation.from_kwargs(
            external_recipients=external_recipients,
            visible_filters=visible_filters,
            cron_expression=cron_expression,
            visual_exports=exports.visual_exports,
            tabular_exports=exports.tabular_exports,
            title=title,
            message=message,
            subject=subject,
            timezone=timezone,
            cloud_dashboard_id=cloud_dashboard_id,
            internal_recipients=internal_recipients,
            cloud_email_id=cloud_email_id,
            notification_channel_id=self.context.notification_channel_id,
        )

        return cloud_email

    @staticmethod
    def _filter_platform_scheduled_emails(
        input_file: str | None, platform_scheduled_emails: list[ScheduledMail]
    ) -> list[ScheduledMail]:
        """Filter platform scheduled emails by input file if provided.

        If no input file is provided, all Platform scheduled emails are returned back.
        Otherwise, only scheduled emails specified in the input file are returned
        for further processing.
        """
        # If no input file is provided, return all validated exports
        if not input_file:
            return platform_scheduled_emails

        # If an input file is provided, filter exports by it
        with open(input_file, "r") as f:
            input_reader = csv.reader(f)
            input_exports: list[str] = [row[0] for row in input_reader]

        filtered_exports = []
        for export in platform_scheduled_emails:
            if export.meta.identifier in input_exports:
                filtered_exports.append(export)

        return filtered_exports

    def _create_or_update_automation(
        self, cloud_email_id: str, data: dict, action: Action
    ):
        """Create or update automation data in Cloud.

        Args:
            cloud_email_id: Cloud email ID
            data: dictionary containing the serialized automation data to post
        """

        if action == Action.CREATE:
            request_method = self.context.backends.cloud_client.post_automation
        elif action == Action.UPDATE:
            request_method = self.context.backends.cloud_client.put_automation

        request_data = {"data": data}

        response = request_method(request_data)

        if response.ok:
            logger.info(f"Successfully posted email {cloud_email_id}")
        else:
            raise Exception(f"Failed to post email {cloud_email_id}: {response.text}")

    def _set_up_helpers(self) -> None:
        """Creates instances of helper objects."""
        # Translates recurrrence expressions to cron
        self.recur_translator: RecurToCronTranslator = RecurToCronTranslator()

        # Caches cloud objects
        self.cloud_maps = Mappers.create_maps(self.context.backends.cloud_client)

        # Processes Platform attachments
        self.attachment_processor: AttachmentProcessor = AttachmentProcessor(
            self.context,
            self.cloud_maps,
        )

    def _get_platform_email_author(self, platform_email: ScheduledMail) -> str:
        """Get the email address of the Platform email author."""

        raw_platform_author = self.context.backends.platform_client.get_object(
            platform_email.meta.author
        )
        platform_author = AccountSettingsWrapper(**raw_platform_author)
        return platform_author.account_setting.email

    def _set_automation_author(
        self,
        automation_id: str,
        author_id: str,
    ) -> None:
        """Update createdBy.id for a single automation in Cloud automations layout."""
        automations_layout = self.context.backends.cloud_client.get_automations_layout()

        updated: bool = False
        found: bool = False
        for automation in automations_layout:
            if automation.get("id") != automation_id:
                continue

            found = True
            current_author_id = automation.get("createdBy", {}).get("id")
            if current_author_id == author_id:
                break

            automation["createdBy"]["id"] = author_id
            updated = True
            logger.info(f"Updated automation {automation_id} author to {author_id}")
            break

        if not found:
            logger.warning(
                f"Automation {automation_id} not found in automations layout; "
                "skipping author update."
            )
            return

        if not updated:
            return

        self.context.backends.cloud_client.put_automations_layout(automations_layout)

    def migrate(self) -> None:
        """Migrate scheduled emails from Platform to Cloud.

        Automatically skips emails which have no attachments or recipients.
        """

        # Log migration metadata
        self.context.logging.output_logger.write_migration_metadata(
            self.context.backends.platform_client.domain,
            self.context.backends.platform_client.pid,
            self.context.backends.cloud_client.domain,
            self.context.backends.cloud_client.ws,
            self.context.command_line_arguments.client_prefix,
        )

        # Set up helper objects
        self._set_up_helpers()

        # Cleanup target env if requested
        if self.context.command_line_arguments.cleanup_target_env:
            self.context.backends.cloud_client.sdk.catalog_workspace.put_declarative_automations(
                workspace_id=self.context.backends.cloud_client.ws,
                automations=[],
            )

        # Get scheduled emails from Platform (get raw data first in case they need to
        # be dumped, then validate the data later))
        exports: list[Any] = (
            self.context.backends.platform_client.get_objects_by_category(
                "scheduledMail"
            )
        )

        # Dump Platform scheduled exports if requested
        if self.context.command_line_arguments.dump_platform:
            with open(self.context.command_line_arguments.platform_dump_file, "w") as f:
                json.dump(exports, f, indent=4)

            logger.info(
                f"Platform scheduled exports dumped to '{self.context.command_line_arguments.platform_dump_file}'"
            )

        # Validate platform scheduled emails
        platform_scheduled_emails = (
            self.context.backends.platform_client.validate_platform_scheduled_exports(
                exports
            )
        )

        # Filter platform scheduled emails by input file if provided
        filtered_platform_scheduled_emails = self._filter_platform_scheduled_emails(
            self.context.input_file, platform_scheduled_emails
        )

        # Get Platform dashboards - needed for metadata lookups
        platform_dashboards = (
            self.context.backends.platform_client.get_dashboard_objects()
        )

        # Iterate through filtered platform emails, apply transformations and post
        serialized_data: list[dict] = []
        for platform_email in filtered_platform_scheduled_emails:
            data: dict | None = None
            exception_message: str | None = None
            current_automation_id: str | None = None
            current_author_id: str | None = None
            try:
                current_automation_id = get_cloud_id(
                    platform_email.meta.title, platform_email.meta.identifier
                )

                # Check that the automation author user exists in Cloud
                platform_author_email = self._get_platform_email_author(platform_email)
                try:
                    author_user_cloud = self.cloud_maps.get_user_by_email(
                        platform_author_email
                    )
                    current_author_id = author_user_cloud.id
                except KeyError:
                    raise ExpectedSkip(
                        f"Automation author {platform_author_email} not found in Cloud."
                    )

                # Skip if there are no recipients
                if not platform_email.content.to and not platform_email.content.bcc:
                    raise ExpectedSkip("Email has no recipients.")

                # Skip if there are no attachments
                if not platform_email.content.attachments:
                    raise ExpectedSkip("Email has no attachments.")

                # If overwrite_existing is not requested, we can skip the automations
                # which are already in Cloud (the post would fail anyway)
                if not self.context.command_line_arguments.overwrite_existing:
                    # Skip if the automation already exists in Cloud
                    if current_automation_id in self.cloud_maps.existing_automation_ids:
                        raise ExpectedSkip("Automation already exists in Cloud.")

                # Transform platform scheduled email to cloud automation. Skip on failure.
                cloud_email: CloudAutomation = self._transform_platform_scheduled_email(
                    platform_email, platform_dashboards, self.cloud_maps
                )

                # Serialize Cloud automation to JSON
                data = cloud_email.model_dump(by_alias=True, exclude_none=True)
                serialized_data.append(data)

                # Post cloud scheduled emails (unless skip deploy is requested)
                if not self.context.command_line_arguments.skip_deploy:
                    action = Action.CREATE
                    if cloud_email.id in self.cloud_maps.existing_automation_ids:
                        if self.context.command_line_arguments.overwrite_existing:
                            # If overwrite_existing is requested and the automation
                            # already exists in Cloud, we do UPDATE instead of CREATE
                            action = Action.UPDATE

                    self._create_or_update_automation(cloud_email.id, data, action)

            except ExpectedSkip as e:
                message = f"Skipping email {platform_email.meta.identifier} ({platform_email.meta.title}): {e}"
                logger.warning(message)
                exception_message = f"WARNING: {message}"
            except Exception as e:
                message = f"Skipping email {platform_email.meta.identifier} ({platform_email.meta.title}) because of an unexpected error: {e.__class__.__name__}: {e}"
                logger.error(message)
                exception_message = f"ERROR: {message}"
            finally:
                # If deploy is not prevented, update automations layout with
                # correct author ID. Run this from finally to ensure that author
                # of skipped automations (such as those that already exist)
                # is checked as well.
                if not self.context.command_line_arguments.skip_deploy:
                    if (
                        current_automation_id is not None
                        and current_author_id is not None
                    ):
                        self._set_automation_author(
                            current_automation_id,
                            current_author_id,
                        )

                # Log the transformation - either write the serialized Cloud
                # object or the exception message
                self.context.logging.output_logger.write_transformation(
                    platform_email.meta.title,
                    platform_email.model_dump_json(by_alias=True),
                    data or exception_message,
                )

        # Dump serialized Cloud automations if requested
        if self.context.command_line_arguments.dump_cloud:
            with open(self.context.command_line_arguments.cloud_dump_file, "w") as f:
                json.dump(serialized_data, f, indent=4)

            logger.info(
                f"Cloud scheduled exports dumped to '{self.context.command_line_arguments.cloud_dump_file}'"
            )
