# (C) 2026 GoodData Corporation
"""
Log parser for migration log files.
"""

import os
from typing import Any, Dict, List, Optional, Tuple

from gooddata_legacy2cloud.web_compare_processing.parsing.definition_parser import (
    parse_definition,
)
from gooddata_legacy2cloud.web_compare_processing.parsing.id_extractor import (
    extract_ids_from_definitions,
)
from gooddata_legacy2cloud.web_compare_processing.parsing.metadata import (
    LogMetadata,
    extract_metadata_from_headers,
    extract_metadata_from_plain_text,
)
from gooddata_legacy2cloud.web_compare_processing.parsing.status_analyzer import (
    determine_success,
)


class LogParser:
    """Parser for migration log files."""

    @staticmethod
    def parse_migration_log(
        log_file_path: str,
    ) -> Tuple[List[Dict], Optional[LogMetadata]]:
        """
        Parse a migration log file to extract entries and metadata.
        Works with both files containing #MIGRATION_INFO# headers and those without.

        Args:
            log_file_path: Path to the log file

        Returns:
            Tuple containing a list of log entries and optional metadata
        """
        entries = []
        metadata = None

        try:
            with open(log_file_path, "r", encoding="utf-8") as f:
                lines = f.readlines()

            # Check if file is empty
            if not lines:
                return entries, metadata

            # Extract metadata if present
            metadata_dict, line_offset = extract_metadata_from_headers(lines)

            # Create metadata object if we found any metadata
            if metadata_dict:
                metadata = LogMetadata(
                    legacy_hostname=metadata_dict.get("legacy-hostname"),
                    legacy_ws=metadata_dict.get("legacy-ws"),
                    cloud_hostname=metadata_dict.get("cloud-hostname"),
                    cloud_ws=metadata_dict.get("cloud-ws"),
                    client_prefix=metadata_dict.get("client_prefix"),
                    timestamp=metadata_dict.get("timestamp"),
                )

                # Remove metadata lines from processing
                lines = lines[line_offset:]

            # Process remaining lines
            entries = LogParser._parse_log_entries(lines)

            return entries, metadata

        except Exception as e:
            print(f"Error parsing log file {os.path.basename(log_file_path)}: {e}")
            import traceback

            traceback.print_exc()
            return entries, metadata

    @staticmethod
    def _parse_log_entries(lines: List[str]) -> List[Dict]:
        """
        Parse log file lines into entries.
        Handles both structured JSON logs and simple text dump logs.

        Args:
            lines: Lines from the log file (after metadata)

        Returns:
            List of parsed log entries
        """
        entries = []
        line_index = 0
        entry_count = 0

        while line_index < len(lines):
            try:
                # Skip empty lines
                line_index = LogParser._skip_empty_lines(lines, line_index)
                if line_index >= len(lines):
                    break

                # Process a single entry
                entry_count += 1
                entry_data, line_index = LogParser._parse_single_entry(
                    lines, line_index, entry_count
                )

                if entry_data:
                    entries.append(entry_data)

            except Exception as e:
                print(f"Warning: Error parsing entry at line {line_index}: {e}")
                # Skip to the next entry (assume 3 lines per entry)
                line_index += 3
                continue

        return entries

    @staticmethod
    def _skip_empty_lines(lines: List[str], line_index: int) -> int:
        """
        Skip empty lines in the log file.

        Args:
            lines: Lines from the log file
            line_index: Current line index

        Returns:
            Updated line index after skipping empty lines
        """
        while line_index < len(lines) and not lines[line_index].strip():
            line_index += 1

        return line_index

    @staticmethod
    def _parse_single_entry(
        lines: List[str], line_index: int, entry_count: int
    ) -> Tuple[Optional[Dict], int]:
        """
        Parse a single entry from the log file.

        Args:
            lines: Lines from the log file
            line_index: Current line index
            entry_count: Current entry count

        Returns:
            Tuple of (entry data dictionary, updated line index)
        """
        # Read the components of an entry
        title_info = LogParser._parse_title(lines, line_index)
        if not title_info:
            return None, line_index

        legacy_title, title_line = title_info
        line_index += 1

        # Parse legacy definition
        legacy_info = LogParser._parse_definition_component(lines, line_index)
        if not legacy_info:
            return None, line_index

        legacy_def, legacy_def_line = legacy_info
        line_index += 1

        # DIRECT CHECK for ERROR lines
        raw_error = None
        if line_index < len(lines):
            raw_line = lines[line_index].strip()
            if raw_line.startswith("ERROR:"):
                raw_error = raw_line

        # Parse cloud definition
        cloud_info = LogParser._parse_definition_component(lines, line_index)
        if not cloud_info:
            return None, line_index

        cloud_def, cloud_def_line = cloud_info
        line_index += 1

        # Create entry data
        entry_data = LogParser._create_entry_data(
            legacy_title,
            legacy_def,
            cloud_def,
            title_line,
            legacy_def_line,
            cloud_def_line,
        )

        # DIRECT APPROACH: Set raw_error directly in entry data
        if raw_error:
            # Save original cloud_definition
            entry_data["original_cloud_def"] = entry_data.get("cloud_definition")
            # Override cloud_definition with the raw error
            entry_data["cloud_definition"] = raw_error
            # Also set raw_error field
            entry_data["raw_error"] = raw_error
            # Ensure success is False
            entry_data["success"] = False
            # Clear any cloud_id that might have been extracted
            if "cloud_id" in entry_data:
                entry_data["cloud_id"] = None

        return entry_data, line_index

    @staticmethod
    def _parse_title(lines: List[str], line_index: int) -> Optional[Tuple[str, int]]:
        """
        Parse the title line of an entry.

        Args:
            lines: Lines from the log file
            line_index: Current line index

        Returns:
            Tuple of (title, line index) or None if not possible
        """
        if line_index < len(lines):
            legacy_title = lines[line_index].strip()
            return legacy_title, line_index

        return None

    @staticmethod
    def _parse_definition_component(
        lines: List[str], line_index: int
    ) -> Optional[Tuple[Any, int]]:
        """
        Parse a definition component (legacy or cloud definition).

        Args:
            lines: Lines from the log file
            line_index: Current line index

        Returns:
            Tuple of (parsed definition, line index) or None if not possible
        """
        if line_index >= len(lines):
            return None

        definition, def_line = parse_definition(lines[line_index], line_index)

        # Extract metadata from plain text if needed
        if (
            isinstance(definition, str)
            and not definition.startswith("{")
            and not definition.startswith("[")
        ):
            metadata = extract_metadata_from_plain_text(definition)
            if metadata:
                definition = metadata

        return definition, def_line

    @staticmethod
    def _create_entry_data(
        legacy_title: str,
        legacy_def: Any,
        cloud_def: Any,
        title_line: int,
        legacy_def_line: int,
        cloud_def_line: int,
    ) -> Dict[str, Any]:
        """
        Create an entry data dictionary from parsed components.

        Args:
            legacy_title: Legacy title
            legacy_def: Legacy definition
            cloud_def: Cloud definition
            title_line: Line index of the title
            legacy_def_line: Line index of the legacy definition
            cloud_def_line: Line index of the cloud definition

        Returns:
            Entry data dictionary
        """
        # Store line information for reference
        line_info = {
            "title_line": title_line,
            "legacy_def_line": legacy_def_line,
            "cloud_def_line": cloud_def_line,
        }

        # Extract potential IDs from plain text
        legacy_id, legacy_obj_id, cloud_id = extract_ids_from_definitions(
            legacy_def, cloud_def
        )

        # Determine success status
        success = determine_success(legacy_def, cloud_def)

        # Create the entry data
        entry_data = {
            "legacy_title": legacy_title,
            "legacy_definition": legacy_def,
            "cloud_definition": cloud_def,
            "success": success,
            "line_info": line_info,
        }

        # Explicitly store error message if cloud_def is an error string
        if isinstance(cloud_def, str) and cloud_def.startswith("ERROR:"):
            entry_data["error_message"] = cloud_def
            # Also make sure it's marked as not successful
            entry_data["success"] = False

        # Add IDs if extracted
        if legacy_id:
            entry_data["legacy_id"] = legacy_id
        if legacy_obj_id:
            entry_data["legacy_obj_id"] = legacy_obj_id
        if cloud_id:
            entry_data["cloud_id"] = cloud_id

        return entry_data
