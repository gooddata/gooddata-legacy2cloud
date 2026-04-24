# (C) 2026 GoodData Corporation
"""
Metadata extraction utilities for migration log files.
"""

from typing import Any, Dict, List, Optional, Tuple, TypedDict

from gooddata_legacy2cloud.config.env_vars import EnvVars
from gooddata_legacy2cloud.web_compare_processing.parsing.log_parser import LogParser
from gooddata_legacy2cloud.web_compare_processing.parsing.metadata import LogMetadata


class ConnectionParamsDict(TypedDict):
    legacy_domain: str
    legacy_ws: str
    cloud_domain: str
    cloud_ws: str
    missing_migration_info: bool


class LogFileMetadataExtractor:
    """Extracts and manages metadata from log files."""

    def __init__(self, env_vars: EnvVars):
        """
        Initialize the metadata extractor.

        Args:
            env_vars: Environment variables for fallback values
        """
        self.env_vars = env_vars

    def extract_metadata(
        self, log_file_path: str
    ) -> Tuple[List[Dict[str, Any]], Optional[LogMetadata]]:
        """
        Extract metadata and log entries from a log file.

        Args:
            log_file_path: Path to the log file

        Returns:
            Tuple of (log entries, log metadata)
        """
        return LogParser.parse_migration_log(log_file_path)

    def get_connection_params(
        self,
        log_metadata: Optional[LogMetadata],
        client_prefix: Optional[str] = None,
        use_fallbacks_for_inheritance: bool = False,
    ) -> ConnectionParamsDict:
        """
        Get connection parameters, with different behavior for prefixed vs unprefixed outputs.

        For unprefixed outputs:
        - First try to use info from #MIGRATION_INFO# headers
        - If those don't exist, fall back to .env file or CLI parameters

        For prefixed outputs:
        - If migration info exists, use it
        - If migration info doesn't exist, set missing_migration_info flag to True
        - If use_fallbacks_for_inheritance is True, use .env fallbacks even for prefixed clients

        Args:
            log_metadata: Optional log metadata
            client_prefix: Client prefix (None or empty for unprefixed outputs)
            use_fallbacks_for_inheritance: Whether to use fallbacks for prefixed clients for inheritance

        Returns:
            Dictionary of connection parameters
        """
        has_migration_info = (
            log_metadata is not None
            and log_metadata.legacy_ws
            and log_metadata.cloud_ws
        )
        is_prefixed = client_prefix is not None and client_prefix != ""

        # Always get fallback values (for unprefixed and inheritance)
        legacy_domain = (
            log_metadata.legacy_hostname
            if log_metadata and log_metadata.legacy_hostname
            else self.env_vars.legacy_domain
        )
        legacy_ws = (
            log_metadata.legacy_ws
            if log_metadata and log_metadata.legacy_ws
            else self.env_vars.legacy_ws
        )
        cloud_domain = (
            log_metadata.cloud_hostname
            if log_metadata and log_metadata.cloud_hostname
            else self.env_vars.cloud_domain
        )
        cloud_ws = (
            log_metadata.cloud_ws
            if log_metadata and log_metadata.cloud_ws
            else self.env_vars.cloud_ws
        )

        # Flag to indicate missing migration info
        missing_migration_info = False

        if is_prefixed and not has_migration_info and not use_fallbacks_for_inheritance:
            # For prefixed outputs without migration info (and not for inheritance),
            # set the missing_migration_info flag but keep fallback values
            missing_migration_info = True

        # If any values are None, replace with empty string for compatibility
        return {
            "legacy_domain": legacy_domain or "",
            "legacy_ws": legacy_ws or "",
            "cloud_domain": cloud_domain or "",
            "cloud_ws": cloud_ws or "",
            "missing_migration_info": missing_migration_info,
        }
