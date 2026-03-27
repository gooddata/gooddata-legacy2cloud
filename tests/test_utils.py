# (C) 2026 GoodData Corporation
import json

import pytest

IGNORED_FIELDS = {
    # UUID-derived, non-deterministic identifiers
    "localIdentifier",
    # Uses UUIDs as dict keys (non-deterministic) in some converted insights
    "attributeFilterConfigs",
}


def load_json(file_path: str) -> dict | list[dict]:
    """Load JSON data from file at given path."""
    with open(file_path, "r") as file:
        return json.load(file)


def _strip_ignored_fields(data):
    """Recursively remove ignored fields before sorting/comparison."""
    if isinstance(data, dict):
        return {
            k: _strip_ignored_fields(v)
            for k, v in data.items()
            if k not in IGNORED_FIELDS
        }
    if isinstance(data, list):
        return [_strip_ignored_fields(v) for v in data]
    return data


def list_are_equal(
    key: str,
    actual_data: list,
    expected_data: list,
) -> None:
    """Iterates over the items in the actual data and compares them to the expected data.

    Will fail the test if the actual data does not match the expected data.
    Lists are sorted before comparison to ensure order does not affect equality.
    """
    if len(actual_data) != len(expected_data):
        pytest.fail(
            f"Key: {key} - Actual list length: {len(actual_data)} - Expected list length: {len(expected_data)}"
        )

    def sort_key(item):
        if isinstance(item, dict):
            # Sort by stringified dict for deterministic order.
            return json.dumps(_strip_ignored_fields(item), sort_keys=True)
        return str(item)

    sorted_actual = sorted(actual_data, key=sort_key)
    sorted_expected = sorted(expected_data, key=sort_key)

    for i in range(len(sorted_actual)):
        if isinstance(sorted_actual[i], dict):
            dicts_are_equal(sorted_actual[i], sorted_expected[i])
        elif isinstance(sorted_actual[i], list):
            list_are_equal(key, sorted_actual[i], sorted_expected[i])
        elif sorted_actual[i] != sorted_expected[i]:
            pytest.fail(
                f"Value mismatch: - Actual: \u007b'{key}': '{sorted_actual[i]}'\u007d - Expected: \u007b'{key}': '{sorted_expected[i]}'\u007d"
            )


def dicts_are_equal(
    actual_data: dict,
    expected_data: dict,
) -> None:
    """Iterates over the items in the actual data and compares them to the expected data.

    Will fail the test if the actual data does not match the expected data.

    Note: fields listed in IGNORED_FIELDS are intentionally not compared because they can be
    generated from UUIDs (non-deterministic) in some migrations.
    """
    for key, value in actual_data.items():
        if key in IGNORED_FIELDS:
            continue
        if isinstance(value, dict):
            dicts_are_equal(value, expected_data[key])
        elif isinstance(value, list):
            list_are_equal(key, value, expected_data[key])
        elif value != expected_data[key]:
            pytest.fail(
                f"Value mismatch: - Actual: \u007b'{key}': '{value}'\u007d - Expected: \u007b'{key}': '{expected_data[key]}'\u007d"
            )
