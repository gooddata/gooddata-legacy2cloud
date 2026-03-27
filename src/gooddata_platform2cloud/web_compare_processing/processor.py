# (C) 2026 GoodData Corporation
"""
Processor for migration log files.
"""

import os
from typing import Dict, List, Optional, Set, Tuple

from gooddata_platform2cloud.config.env_vars import EnvVars
from gooddata_platform2cloud.web_compare_processing.detector import LogFileDetector
from gooddata_platform2cloud.web_compare_processing.file_utils import JsonDataLoader
from gooddata_platform2cloud.web_compare_processing.generator import WebCompareUtils
from gooddata_platform2cloud.web_compare_processing.inheritance import (
    InheritanceManager,
)
from gooddata_platform2cloud.web_compare_processing.metadata_extractor import (
    LogFileMetadataExtractor,
)
from gooddata_platform2cloud.web_compare_processing.parsing import (
    create_comparison_result,
)
from gooddata_platform2cloud.web_compare_processing.path_management import PathManager


class LogProcessor:
    """Processor for migration log files."""

    def __init__(self, env_vars: EnvVars, output_dir: str, use_inherited: bool = False):
        """
        Initialize the log processor.

        Args:
            env_vars: Environment variables
            output_dir: Output directory for generated files
            use_inherited: Whether to include unprefixed objects in prefixed outputs
        """
        self.use_inherited = use_inherited

        # Initialize helper classes
        self.metadata_extractor = LogFileMetadataExtractor(env_vars)
        self.path_manager = PathManager(output_dir)
        self.inheritance_manager = InheritanceManager()

    def process_log_file(
        self,
        log_file_path: str,
        object_type: Optional[str] = None,
        client_prefix: Optional[str] = None,
        all_prefixes: Dict[str, List[str]] | None = None,
        file_number: int = 0,
    ) -> Tuple[int, str]:
        """
        Process a single log file and generate web comparison.

        Args:
            log_file_path: Path to the log file
            object_type: Optional object type (auto-detected from filename if not provided)
            client_prefix: Optional client prefix (auto-detected from filename if not provided)
            all_prefixes: Dictionary to track prefixes by object type
            file_number: Sequential number of the file being processed

        Returns:
            Tuple of (status_code, output_path) where status_code is 0 on success, non-zero on error
            and output_path is the path to the generated HTML file (or empty string on error)
        """
        try:
            # Auto-detect and normalize type and prefix
            object_type, client_prefix = self._detect_type_and_prefix(
                log_file_path, object_type, client_prefix
            )

            # Parse the log file
            log_entries, log_metadata = self.metadata_extractor.extract_metadata(
                log_file_path
            )
            if not log_entries:
                log_entries = []

            # Get connection parameters
            connection_params = self.metadata_extractor.get_connection_params(
                log_metadata, client_prefix
            )

            # Load additional data
            failed_publishing_data, skipped_ids = self._load_additional_data(
                log_file_path, object_type, client_prefix
            )

            # Create comparison result using the function from the parsing module
            result = create_comparison_result(
                log_entries=log_entries,
                object_type=object_type,
                platform_domain=connection_params["platform_domain"],
                platform_workspace=connection_params["platform_ws"],
                cloud_domain=connection_params["cloud_domain"],
                cloud_workspace=connection_params["cloud_ws"],
                failed_publishing_data=failed_publishing_data,
                skipped_ids=skipped_ids,
                missing_migration_info=connection_params["missing_migration_info"],
            )

            # Handle inheritance
            if not client_prefix:
                # Store unprefixed objects for inheritance
                self.inheritance_manager.store_unprefixed_result(object_type, result)
            elif (
                self.use_inherited
                and client_prefix
                and self.inheritance_manager.has_unprefixed_result(object_type)
            ):
                # Add inherited objects if enabled and this is a prefixed log
                unprefixed_result = self.inheritance_manager.get_unprefixed_result(
                    object_type
                )
                if unprefixed_result:
                    # Get connection parameters with fallbacks for inheritance
                    inheritance_connection_params = (
                        self.metadata_extractor.get_connection_params(
                            log_metadata,
                            client_prefix,
                            use_fallbacks_for_inheritance=True,
                        )
                    )

                    # Add inherited objects
                    self.inheritance_manager.add_inherited_objects(
                        result, unprefixed_result, inheritance_connection_params
                    )

                    # Report is handled in the main log message at the end
            elif self.use_inherited and client_prefix:
                print(
                    f"Warning: Couldn't find unprefixed {object_type} objects for inheriting to {client_prefix}"
                )

            # Prepare paths and data for HTML generation
            output_path, resources_rel_path, sidebar_data = (
                self.path_manager.prepare_for_html_generation(
                    object_type, client_prefix, all_prefixes
                )
            )

            # Generate the HTML comparison
            WebCompareUtils.generate_comparison_html(
                result, output_path, resources_rel_path, sidebar_data, log_metadata
            )

            # Create indicator
            self.path_manager.create_indicator(object_type, client_prefix)

            # Format relative output path
            relative_output_path = os.path.relpath(
                output_path, self.path_manager.output_dir
            )

            # Calculate object counts for logging
            own_objects = len(result.items)
            inherited_objects = result.summary.inherited_count

            # Log information in a concise format with a single print statement
            object_count_text = (
                f"{own_objects - inherited_objects} objects + {inherited_objects} inherited"
                if client_prefix and inherited_objects > 0
                else f"{own_objects} objects"
            )
            print(
                f"Processing {file_number}: {relative_output_path} ({object_count_text})"
            )

            return 0, output_path

        except FileNotFoundError as e:
            print(f"Error: {e}")
            return 1, ""
        except Exception as e:
            print(f"Error processing {log_file_path}: {e}")
            return 1, ""

    def _detect_type_and_prefix(
        self,
        log_file_path: str,
        object_type: Optional[str],
        client_prefix: Optional[str],
    ) -> Tuple[str, str]:
        """
        Detect and normalize object type and client prefix.

        Args:
            log_file_path: Path to the log file
            object_type: Optional object type
            client_prefix: Optional client prefix

        Returns:
            Tuple of (normalized object type, client prefix)
        """
        # Auto-detect object type if not provided
        if not object_type:
            detected_type = LogFileDetector.detect_object_type(log_file_path)
            if not detected_type:
                raise ValueError(
                    f"Could not detect object type from filename: {log_file_path}"
                )
            object_type = detected_type

        # Auto-detect prefix if not provided
        if client_prefix is None:
            client_prefix = LogFileDetector.detect_prefix(log_file_path)

        # Normalize object type to singular form for processing
        object_type_singular = object_type.lower()
        if object_type_singular.endswith("s"):
            object_type_singular = object_type_singular[:-1]  # Remove trailing 's'

        return object_type_singular, client_prefix

    def _load_additional_data(
        self, log_file_path: str, object_type: str, client_prefix: str
    ) -> Tuple[List[Dict], Set[str]]:
        """
        Load additional data for log processing.

        Args:
            log_file_path: Path to the log file
            object_type: Object type
            client_prefix: Client prefix

        Returns:
            Tuple of (failed publishing data, skipped IDs)
        """
        # Get paths for additional data
        failed_publishing_path, skipped_objects_path = (
            self.path_manager.get_additional_data_paths(
                log_file_path, object_type, client_prefix
            )
        )

        # Load the data
        failed_publishing_data = JsonDataLoader.load_failed_publishing_data(
            failed_publishing_path
        )
        skipped_ids = JsonDataLoader.load_skipped_objects(skipped_objects_path)

        return failed_publishing_data, skipped_ids
