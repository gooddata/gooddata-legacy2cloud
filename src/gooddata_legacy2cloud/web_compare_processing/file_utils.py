# (C) 2026 GoodData Corporation
"""
Utilities for file path building and JSON data loading.
"""

import json
import os
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar

T = TypeVar("T", List[Dict], Set[str])


class FilePathBuilder:
    """Builder for file paths related to log processing."""

    @staticmethod
    def _get_base_path(
        log_file_path: str,
        file_type: str,
        object_type: str,
        client_prefix: Optional[str] = None,
    ) -> str:
        """
        Construct the base path for output files.

        Args:
            log_file_path: Path to the log file
            file_type: Type of file ('failed' or 'skipped')
            object_type: Type of objects (singular form)
            client_prefix: Optional client prefix

        Returns:
            Base path for the output file
        """
        # Normalize object type to plural form
        object_type_plural = (
            object_type + "s" if not object_type.endswith("s") else object_type
        )

        # Get directory from log file path
        log_dir = os.path.dirname(log_file_path)
        if not log_dir:
            log_dir = "."

        # Construct filename
        filename = f"cloud_{file_type}_{object_type_plural}.json"
        if client_prefix:
            filename = f"{client_prefix}{filename}"

        # Return full path
        return os.path.join(log_dir, filename)

    @staticmethod
    def get_failed_publishing_path(
        log_file_path: str, object_type: str, client_prefix: Optional[str] = None
    ) -> str:
        """
        Construct the path to the failed publishing JSON file.

        Args:
            log_file_path: Path to the log file
            object_type: Type of objects (singular form)
            client_prefix: Optional client prefix

        Returns:
            Path to the failed publishing JSON file
        """
        return FilePathBuilder._get_base_path(
            log_file_path, "failed", object_type, client_prefix
        )

    @staticmethod
    def get_skipped_objects_path(
        log_file_path: str, object_type: str, client_prefix: Optional[str] = None
    ) -> str:
        """
        Construct the path to the skipped objects JSON file.

        Args:
            log_file_path: Path to the log file
            object_type: Type of objects (singular form)
            client_prefix: Optional client prefix

        Returns:
            Path to the skipped objects JSON file
        """
        return FilePathBuilder._get_base_path(
            log_file_path, "skipped", object_type, client_prefix
        )


class JsonDataLoader:
    """Loader for JSON data related to log processing."""

    @staticmethod
    def _load_json_file(
        file_path: str, transform_func: Callable[[Any], T], file_type: str
    ) -> T:
        """
        Generic method to load and transform JSON data from a file.

        Args:
            file_path: Path to the JSON file
            transform_func: Function to transform the loaded data
            file_type: Type of file for error messages

        Returns:
            Transformed data or empty default as defined by transform_func
        """
        if not os.path.exists(file_path):
            return transform_func([])

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            if not isinstance(data, list):
                return transform_func([])

            return transform_func(data)
        except json.JSONDecodeError as e:
            print(f"Error parsing {file_type} file: {e}")
            return transform_func([])
        except Exception as e:
            print(f"Error loading {file_type} file: {e}")
            return transform_func([])

    @staticmethod
    def load_failed_publishing_data(failed_publishing_path: str) -> List[Dict]:
        """
        Load data from a failed publishing JSON file.

        Args:
            failed_publishing_path: Path to the failed publishing JSON file

        Returns:
            List of failed publishing entries, or empty list if file doesn't exist
        """
        return JsonDataLoader._load_json_file(
            failed_publishing_path, lambda data: data, "failed publishing"
        )

    @staticmethod
    def load_skipped_objects(skipped_objects_path: str) -> Set[str]:
        """
        Load skipped object IDs from a JSON file.

        Args:
            skipped_objects_path: Path to the skipped objects JSON file

        Returns:
            Set of skipped object IDs
        """
        return JsonDataLoader._load_json_file(
            skipped_objects_path, lambda data: set(data), "skipped objects"
        )
